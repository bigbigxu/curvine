// Copyright 2025 OPPO.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Per-instance transfer status, collected by the console.
//!
//! Transfer has no registry or heartbeat: `transfer.endpoints` is the whole
//! instance list (see `TransferServer::build_forwarder`). Every consumer here
//! therefore starts from that list and asks each address for its own status,
//! which is the only way to answer "which instance carries this job", "which
//! build is it running" and "is its metadata snapshot stale" — those are
//! per-process facts a shared store or a Prometheus scrape cannot attribute.
//!
//! Two consumers share this code on purpose:
//!
//! * `GET /api/v1/transfers/instances` (the Runtime page), through the TTL cache
//!   in [`TransferServicesCache`];
//! * observability target discovery, which needs each instance's *metrics* URL
//!   and can only learn the web port from the instance itself.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use curvine_client_core::file::FsContext;
use curvine_config::ClusterConf;
use curvine_job_client::{configured_transfer_endpoints, TransferClient};
use curvine_proto::TransferServiceInfoResponse;
use curvine_runtime::common::LocalTime;
use futures::future::join_all;
use serde::Serialize;
use tokio::time::timeout;

/// Status query budget per instance. Short on purpose: a page that hangs on one
/// dead instance is worse than a row that says it did not answer.
pub const INSTANCE_STATUS_TIMEOUT: Duration = Duration::from_secs(3);

/// Upper bound on the fan-out. `transfer.endpoints` is operator-written config,
/// so a typo (or a pasted range) must not become unbounded RPCs.
pub const MAX_INSTANCES: usize = 64;

/// How long one fan-out answer is reused.
///
/// The page polls, and several widgets read it in the same refresh; 2s keeps a
/// manual refresh responsive while collapsing those into one round of RPCs.
pub const CACHE_TTL: Duration = Duration::from_secs(2);

/// One instance's answer, or the reason there is none.
#[derive(Clone, Debug, Serialize)]
pub struct TransferInstanceStatus {
    pub addr: String,
    pub reachable: bool,
    /// Why the instance did not answer; `None` when it did.
    pub error: Option<String>,
    pub info: Option<TransferServiceInfoResponse>,
}

impl TransferInstanceStatus {
    fn unreachable(addr: String, error: String) -> Self {
        Self {
            addr,
            reachable: false,
            error: Some(error),
            info: None,
        }
    }

    /// The instance's own rpc address, or the configured one when it is silent.
    pub fn reported_addr(&self) -> &str {
        self.info
            .as_ref()
            .and_then(|info| info.addr.as_deref())
            .filter(|addr| !addr.trim().is_empty())
            .unwrap_or(&self.addr)
    }
}

/// The configured instances, deduplicated and bounded, in configuration order.
///
/// `transfer.endpoints` is authoritative; an empty list means a single-instance
/// deployment that never configured one, which `TransferConf::init` already
/// normalizes to its own `hostname:rpc_port`.
pub fn instance_addrs(conf: &ClusterConf) -> Vec<String> {
    let mut addrs: Vec<String> = Vec::new();
    for addr in configured_transfer_endpoints(conf) {
        let addr = addr.trim().to_string();
        if addr.is_empty() || addrs.contains(&addr) {
            continue;
        }
        addrs.push(addr);
        if addrs.len() == MAX_INSTANCES {
            break;
        }
    }
    addrs
}

/// One instance's runtime-config answer, for the settings surface.
#[derive(Clone, Debug, Serialize)]
pub struct TransferConfigSnapshot {
    pub addr: String,
    pub reachable: bool,
    pub error: Option<String>,
    pub entries: Vec<curvine_proto::RuntimeConfigEntryProto>,
}

/// Reads the writable knobs from every configured instance.
///
/// Each instance is asked individually because overrides are **not** shared:
/// the registry lives in the process, so "what is the current value" is a
/// per-instance question even though the file value is the same everywhere.
pub async fn read_transfer_config(
    conf: &ClusterConf,
    context: &Arc<FsContext>,
) -> Vec<TransferConfigSnapshot> {
    if !conf.transfer.enabled {
        return Vec::new();
    }
    let queries = instance_addrs(conf).into_iter().map(|addr| {
        let context = context.clone();
        async move {
            let client = match TransferClient::with_context_single_endpoint(&context, addr.clone())
            {
                Ok(client) => client,
                Err(err) => {
                    return TransferConfigSnapshot {
                        addr,
                        reachable: false,
                        error: Some(err.to_string()),
                        entries: Vec::new(),
                    }
                }
            };
            match timeout(INSTANCE_STATUS_TIMEOUT, client.get_runtime_config(None)).await {
                Ok(Ok(response)) => TransferConfigSnapshot {
                    addr,
                    reachable: true,
                    error: None,
                    entries: response.entries,
                },
                Ok(Err(err)) => TransferConfigSnapshot {
                    addr,
                    reachable: false,
                    error: Some(err.to_string()),
                    entries: Vec::new(),
                },
                Err(_) => TransferConfigSnapshot {
                    addr,
                    reachable: false,
                    error: Some(format!(
                        "no answer within {}s",
                        INSTANCE_STATUS_TIMEOUT.as_secs()
                    )),
                    entries: Vec::new(),
                },
            }
        }
    });
    join_all(queries).await
}

/// What one instance did with a pushed value.
#[derive(Clone, Debug, Serialize)]
pub struct TransferConfigPushResult {
    pub addr: String,
    pub reachable: bool,
    pub applied: bool,
    pub error: Option<String>,
    pub previous_value_json: Option<String>,
    pub effective_value_json: Option<String>,
}

/// Applies one knob to every configured instance.
///
/// `apply_to_reachable_only` is the two-phase contract the console's dialog
/// depends on: with `false` (the default) a partially reachable fleet is
/// reported back untouched, so a half-applied cluster needs an explicit second
/// request. There is no rollback — instances are independent processes — so the
/// only honest guarantee is "nothing was written unless you said so".
pub async fn push_transfer_config(
    conf: &ClusterConf,
    context: &Arc<FsContext>,
    key: &str,
    value: Option<&str>,
    apply_to_reachable_only: bool,
) -> (Vec<TransferConfigPushResult>, Vec<String>) {
    let addrs = instance_addrs(conf);
    let unreachable = if apply_to_reachable_only {
        Vec::new()
    } else {
        let snapshots = read_transfer_config(conf, context).await;
        let unreachable: Vec<String> = snapshots
            .into_iter()
            .filter(|snapshot| !snapshot.reachable)
            .map(|snapshot| snapshot.addr)
            .collect();
        if !unreachable.is_empty() {
            return (Vec::new(), unreachable);
        }
        Vec::new()
    };

    let pushes = addrs.into_iter().map(|addr| {
        let context = context.clone();
        let value = value.map(str::to_string);
        let key = key.to_string();
        async move {
            let client = match TransferClient::with_context_single_endpoint(&context, addr.clone())
            {
                Ok(client) => client,
                Err(err) => {
                    return TransferConfigPushResult {
                        addr,
                        reachable: false,
                        applied: false,
                        error: Some(err.to_string()),
                        previous_value_json: None,
                        effective_value_json: None,
                    }
                }
            };
            let call = async {
                match value {
                    Some(value) => client
                        .update_runtime_config(&key, &value)
                        .await
                        .map(|response| (Some(response.previous_value_json), response.value_json)),
                    None => client
                        .reset_runtime_config(&key)
                        .await
                        .map(|response| (None, response.effective_value_json)),
                }
            };
            match timeout(INSTANCE_STATUS_TIMEOUT, call).await {
                Ok(Ok((previous, effective))) => TransferConfigPushResult {
                    addr,
                    reachable: true,
                    applied: true,
                    error: None,
                    previous_value_json: previous,
                    effective_value_json: Some(effective),
                },
                Ok(Err(err)) => TransferConfigPushResult {
                    addr,
                    reachable: true,
                    applied: false,
                    error: Some(err.to_string()),
                    previous_value_json: None,
                    effective_value_json: None,
                },
                Err(_) => TransferConfigPushResult {
                    addr,
                    reachable: true,
                    applied: false,
                    error: Some(format!(
                        "no answer within {}s",
                        INSTANCE_STATUS_TIMEOUT.as_secs()
                    )),
                    previous_value_json: None,
                    effective_value_json: None,
                },
            }
        }
    });

    (join_all(pushes).await, unreachable)
}

/// Asks every configured instance for its status, concurrently.
///
/// Failures are per row: one dead instance never hides the others, and it keeps
/// its place in the list because "this member is down" is what an operator needs
/// to see.
pub async fn probe_transfer_instances(
    conf: &ClusterConf,
    context: &Arc<FsContext>,
) -> Vec<TransferInstanceStatus> {
    if !conf.transfer.enabled {
        return Vec::new();
    }

    let queries = instance_addrs(conf).into_iter().map(|addr| {
        let context = context.clone();
        async move {
            let client = match TransferClient::with_context_single_endpoint(&context, addr.clone())
            {
                Ok(client) => client,
                Err(err) => return TransferInstanceStatus::unreachable(addr, err.to_string()),
            };
            match timeout(INSTANCE_STATUS_TIMEOUT, client.service_info()).await {
                Ok(Ok(info)) => TransferInstanceStatus {
                    addr,
                    reachable: true,
                    error: None,
                    info: Some(info),
                },
                Ok(Err(err)) => TransferInstanceStatus::unreachable(addr, err.to_string()),
                Err(_) => TransferInstanceStatus::unreachable(
                    addr,
                    format!("no answer within {}s", INSTANCE_STATUS_TIMEOUT.as_secs()),
                ),
            }
        }
    });

    join_all(queries).await
}

/// Last fan-out answer, shared by every request of one cluster.
///
/// A miss is not coalesced: two concurrent misses both probe, and the later
/// answer wins. That is deliberate — coordinating them would mean holding a lock
/// across an await, and a duplicate round of read-only RPCs is cheaper than the
/// deadlock risk.
pub struct TransferServicesCache {
    ttl: Duration,
    state: Mutex<Option<(i64, Arc<Vec<TransferInstanceStatus>>)>>,
}

impl TransferServicesCache {
    pub fn new(ttl: Duration) -> Self {
        Self {
            ttl,
            state: Mutex::new(None),
        }
    }

    /// Cached status for this cluster, refreshing when older than the TTL.
    pub async fn instances(
        &self,
        conf: &ClusterConf,
        context: &Arc<FsContext>,
    ) -> Arc<Vec<TransferInstanceStatus>> {
        if let Some(hit) = self.fresh() {
            return hit;
        }
        let items = Arc::new(probe_transfer_instances(conf, context).await);
        if let Ok(mut state) = self.state.lock() {
            *state = Some((now_ms(), items.clone()));
        }
        items
    }

    fn fresh(&self) -> Option<Arc<Vec<TransferInstanceStatus>>> {
        let state = self.state.lock().ok()?;
        let (filled_ms, items) = state.as_ref()?;
        let age = now_ms().saturating_sub(*filled_ms);
        // `>=` rather than `>`: a zero TTL means "never reuse", and clock skew
        // (negative age) is treated as stale rather than as fresh forever.
        if age < 0 || age as u128 >= self.ttl.as_millis() {
            return None;
        }
        Some(items.clone())
    }
}

impl Default for TransferServicesCache {
    fn default() -> Self {
        Self::new(CACHE_TTL)
    }
}

fn now_ms() -> i64 {
    i64::try_from(LocalTime::mills()).unwrap_or(i64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn conf_with(endpoints: &[&str]) -> ClusterConf {
        let mut conf = ClusterConf::default();
        conf.transfer.enabled = true;
        conf.transfer.hostname = "10.0.0.9".to_string();
        conf.transfer.rpc_port = 9010;
        conf.transfer.endpoints = endpoints.iter().map(|value| value.to_string()).collect();
        conf
    }

    #[test]
    fn instance_addrs_follows_configuration_order() {
        let conf = conf_with(&["cv-transfer-0:9010", "cv-transfer-1:9010"]);
        assert_eq!(
            instance_addrs(&conf),
            vec!["cv-transfer-0:9010", "cv-transfer-1:9010"]
        );
    }

    #[test]
    fn instance_addrs_falls_back_to_this_process() {
        // Single instance: `transfer.endpoints` is empty on disk and
        // `TransferConf::init` normalizes it to hostname:rpc_port.
        let conf = conf_with(&[]);
        assert_eq!(instance_addrs(&conf), vec!["10.0.0.9:9010"]);
    }

    #[test]
    fn instance_addrs_drops_duplicates_and_blanks() {
        let conf = conf_with(&[
            "cv-transfer-0:9010",
            " cv-transfer-0:9010 ",
            "",
            "cv-transfer-1:9010",
        ]);
        assert_eq!(
            instance_addrs(&conf),
            vec!["cv-transfer-0:9010", "cv-transfer-1:9010"]
        );
    }

    #[test]
    fn instance_addrs_is_bounded() {
        let endpoints: Vec<String> = (0..MAX_INSTANCES + 10)
            .map(|index| format!("cv-transfer-{index}:9010"))
            .collect();
        let refs: Vec<&str> = endpoints.iter().map(String::as_str).collect();
        assert_eq!(instance_addrs(&conf_with(&refs)).len(), MAX_INSTANCES);
    }

    #[tokio::test]
    async fn probing_a_disabled_transfer_reports_no_instances() {
        let mut conf = conf_with(&["cv-transfer-0:9010"]);
        conf.transfer.enabled = false;
        let context = test_context();
        assert!(probe_transfer_instances(&conf, &context).await.is_empty());
    }

    #[tokio::test]
    async fn a_silent_instance_keeps_its_row() {
        // 127.0.0.1:1 is closed, so the probe must report the row as unreachable
        // rather than dropping it — that row is the whole point of the page.
        let conf = conf_with(&["127.0.0.1:1"]);
        let context = test_context();
        let items = probe_transfer_instances(&conf, &context).await;
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].addr, "127.0.0.1:1");
        assert!(!items[0].reachable);
        assert!(items[0].info.is_none());
        assert!(items[0].error.is_some());
    }

    #[tokio::test]
    async fn cache_answers_from_memory_within_the_ttl() {
        let conf = conf_with(&["127.0.0.1:1"]);
        let context = test_context();
        let cache = TransferServicesCache::new(Duration::from_secs(60));
        let first = cache.instances(&conf, &context).await;
        // A second cluster config must not be consulted while the answer is fresh:
        // the returned rows still describe the first probe.
        let changed = conf_with(&["127.0.0.1:2"]);
        let second = cache.instances(&changed, &context).await;
        assert_eq!(first[0].addr, second[0].addr);
    }

    #[tokio::test]
    async fn cache_expires_after_the_ttl() {
        let conf = conf_with(&["127.0.0.1:1"]);
        let context = test_context();
        let cache = TransferServicesCache::new(Duration::from_millis(0));
        let first = cache.instances(&conf, &context).await;
        let changed = conf_with(&["127.0.0.1:2"]);
        let second = cache.instances(&changed, &context).await;
        assert_eq!(first[0].addr, "127.0.0.1:1");
        assert_eq!(second[0].addr, "127.0.0.1:2");
    }

    #[test]
    fn reported_addr_prefers_the_instance_answer() {
        let silent =
            TransferInstanceStatus::unreachable("cv-transfer-0:9010".to_string(), "x".into());
        assert_eq!(silent.reported_addr(), "cv-transfer-0:9010");
    }

    fn test_context() -> Arc<FsContext> {
        let conf = ClusterConf::default();
        let fs = curvine_client_core::file::CurvineFileSystem::with_rt(
            conf,
            Arc::new(curvine_runtime::runtime::Runtime::single()),
        )
        .expect("curvine fs");
        fs.fs_context().clone()
    }
}
