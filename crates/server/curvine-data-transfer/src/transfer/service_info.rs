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

//! Per-instance transfer status, answered by the instance itself.
//!
//! The console already knows *which* transfer instances exist — `transfer.endpoints`
//! is the authoritative list (no registry, no heartbeat; see
//! [`crate::transfer::build_forwarder`]). This module is how each address in that
//! list answers "what are you doing": one struct, one builder, two transports.
//!
//! Both the RPC (`GetTransferServiceInfo`) and the HTTP (`GET /api/v1/transfer/info`)
//! surfaces call [`TransferServiceProbe::snapshot`], so an operator reading `curl`
//! output and a console table can never disagree, and neither of them has to parse
//! `/metrics` for state.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use curvine_config::ClusterConf;
use curvine_proto::TransferServiceInfoResponse;
use curvine_runtime::common::LocalTime;
use serde_json::{json, Value};

use crate::transfer::{
    ClusterMetadataCache, TransferRuntimeConfig, TransferService, TransferStoreBackend,
};

/// Job/task counts as seen by one instance.
///
/// `active_jobs` / `executing_jobs` come from the store and are therefore
/// identical on every instance sharing it (they answer "how big is the cluster's
/// backlog"). The `in_memory_*` pair is per process and answers "how much of it
/// is this instance carrying" — file-level task state never leaves the owner's
/// memory, so it is the only honest per-instance workload number.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct TransferWorkload {
    pub active_jobs: u64,
    pub executing_jobs: u64,
    pub in_memory_jobs: u64,
    pub in_memory_running_tasks: u64,
}

impl TransferWorkload {
    pub fn new(
        active_jobs: u64,
        executing_jobs: u64,
        in_memory_jobs: u64,
        in_memory_running_tasks: u64,
    ) -> Self {
        Self {
            active_jobs,
            executing_jobs,
            in_memory_jobs,
            in_memory_running_tasks,
        }
    }
}

/// `ready` and healthy.
pub const STATE_NORMAL: &str = "normal";
/// Serving, but a dependency is degraded: jobs are still accepted and existing
/// leases keep running, yet new plans may fail.
pub const STATE_DEGRADED: &str = "degraded";
/// Cannot do useful work: starting up, store down, or metadata unusable.
pub const STATE_NOT_READY: &str = "not_ready";

/// One instance's status at one instant.
#[derive(Clone, Debug)]
pub struct TransferServiceState {
    pub role: &'static str,
    pub instance_id: String,
    pub addr: String,
    pub web_addr: String,
    pub version: String,
    pub version_commit: String,
    pub start_time_ms: i64,
    pub uptime_ms: i64,
    pub ready: bool,
    pub state: &'static str,
    pub state_reason: String,
    pub store_backend: String,
    pub max_running_transfers: u64,
    /// Whether this instance currently accepts new submissions.
    pub accepts_jobs: bool,
    pub worker_task_limit: u64,
    pub max_tasks_per_transfer: u64,
    pub max_plan_entries: u64,
    pub workload: TransferWorkload,
    pub snapshot_version: u64,
    pub snapshot_updated_at: i64,
    pub snapshot_staleness_ms: i64,
    pub snapshot_live_workers: u64,
    pub snapshot_capable_workers: u64,
    pub ring_self: String,
    pub ring_members: Vec<String>,
}

impl TransferServiceState {
    /// RPC form, for the console's fan-out over `transfer.endpoints`.
    pub fn to_proto(&self) -> TransferServiceInfoResponse {
        TransferServiceInfoResponse {
            role: Some(self.role.to_string()),
            instance_id: Some(self.instance_id.clone()),
            addr: Some(self.addr.clone()),
            web_addr: Some(self.web_addr.clone()),
            version: Some(self.version.clone()),
            version_commit: Some(self.version_commit.clone()),
            start_time_ms: Some(self.start_time_ms),
            uptime_ms: Some(self.uptime_ms),
            ready: Some(self.ready),
            state: Some(self.state.to_string()),
            state_reason: Some(self.state_reason.clone()),
            store_backend: Some(self.store_backend.clone()),
            max_running_transfers: Some(self.max_running_transfers),
            accepts_jobs: Some(self.accepts_jobs),
            worker_task_limit: Some(self.worker_task_limit),
            max_tasks_per_transfer: Some(self.max_tasks_per_transfer),
            max_plan_entries: Some(self.max_plan_entries),
            active_jobs: Some(self.workload.active_jobs),
            executing_jobs: Some(self.workload.executing_jobs),
            in_memory_jobs: Some(self.workload.in_memory_jobs),
            in_memory_running_tasks: Some(self.workload.in_memory_running_tasks),
            snapshot_version: Some(self.snapshot_version),
            snapshot_updated_at: Some(self.snapshot_updated_at),
            snapshot_staleness_ms: Some(self.snapshot_staleness_ms),
            snapshot_live_workers: Some(self.snapshot_live_workers),
            snapshot_capable_workers: Some(self.snapshot_capable_workers),
            ring_self: Some(self.ring_self.clone()),
            ring_members: self.ring_members.clone(),
        }
    }

    /// HTTP form. Field names mirror the proto exactly, so one client-side type
    /// can read both surfaces.
    pub fn to_json(&self) -> Value {
        json!({
            "role": self.role,
            "instance_id": self.instance_id,
            "addr": self.addr,
            "web_addr": self.web_addr,
            "version": self.version,
            "version_commit": self.version_commit,
            "start_time_ms": self.start_time_ms,
            "uptime_ms": self.uptime_ms,
            "ready": self.ready,
            "state": self.state,
            "state_reason": self.state_reason,
            "store_backend": self.store_backend,
            "max_running_transfers": self.max_running_transfers,
            "accepts_jobs": self.accepts_jobs,
            "worker_task_limit": self.worker_task_limit,
            "max_tasks_per_transfer": self.max_tasks_per_transfer,
            "max_plan_entries": self.max_plan_entries,
            "active_jobs": self.workload.active_jobs,
            "executing_jobs": self.workload.executing_jobs,
            "in_memory_jobs": self.workload.in_memory_jobs,
            "in_memory_running_tasks": self.workload.in_memory_running_tasks,
            "snapshot_version": self.snapshot_version,
            "snapshot_updated_at": self.snapshot_updated_at,
            "snapshot_staleness_ms": self.snapshot_staleness_ms,
            "snapshot_live_workers": self.snapshot_live_workers,
            "snapshot_capable_workers": self.snapshot_capable_workers,
            "ring_self": self.ring_self,
            "ring_members": self.ring_members,
        })
    }
}

/// Everything needed to answer "how are you" without reaching for globals.
///
/// Held by both the RPC handler and the web router of the same process, so the
/// two surfaces are the same answer by construction.
#[derive(Clone)]
pub struct TransferServiceProbe {
    instance_id: String,
    ready: Arc<AtomicBool>,
    store_backend: &'static str,
    service: TransferService<TransferStoreBackend>,
    cluster_cache: ClusterMetadataCache,
    runtime_config: Arc<TransferRuntimeConfig>,
    conf: Arc<ClusterConf>,
    start_time_ms: i64,
}

impl TransferServiceProbe {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        instance_id: String,
        ready: Arc<AtomicBool>,
        store_backend: &'static str,
        service: TransferService<TransferStoreBackend>,
        cluster_cache: ClusterMetadataCache,
        runtime_config: Arc<TransferRuntimeConfig>,
        conf: Arc<ClusterConf>,
    ) -> Self {
        let start_time_ms = now_ms();
        Self {
            instance_id,
            ready,
            store_backend,
            service,
            cluster_cache,
            runtime_config,
            conf,
            start_time_ms,
        }
    }

    /// Collects one consistent-enough picture. Never fails: a dependency that
    /// cannot answer changes `state` / `state_reason` instead of dropping the
    /// whole response, because "store is down" is exactly what an operator
    /// opened this page to see.
    pub fn snapshot(&self) -> TransferServiceState {
        let now = now_ms();
        let snapshot = self.cluster_cache.snapshot();
        // -1 means "never refreshed", which the console renders as unknown
        // instead of a confident "0s old".
        let snapshot_staleness_ms = if snapshot.updated_at > 0 {
            now.saturating_sub(snapshot.updated_at)
        } else {
            -1
        };
        let (live_workers, capable_workers) = worker_counts(&snapshot.workers);
        let workload = self.service.workload().unwrap_or_default();
        let (state, state_reason) = self.evaluate(snapshot.updated_at, snapshot_staleness_ms);

        TransferServiceState {
            role: "transfer",
            instance_id: self.instance_id.clone(),
            addr: self.self_addr(),
            web_addr: format!(
                "{}:{}",
                self.conf.transfer.hostname, self.conf.transfer.web_port
            ),
            version: curvine_sys::version::PKG_VERSION.to_string(),
            version_commit: curvine_sys::version::GIT_VERSION.to_string(),
            start_time_ms: self.start_time_ms,
            uptime_ms: now.saturating_sub(self.start_time_ms),
            ready: self.ready.load(Ordering::Relaxed),
            state,
            state_reason,
            store_backend: self.store_backend.to_string(),
            // The live knobs, not the file values: this is a status answer.
            max_running_transfers: self.runtime_config.max_running_transfers() as u64,
            accepts_jobs: self.runtime_config.accepts_jobs(),
            worker_task_limit: self.runtime_config.worker_task_limit() as u64,
            max_tasks_per_transfer: self.runtime_config.max_tasks_per_transfer() as u64,
            max_plan_entries: self.runtime_config.max_plan_entries() as u64,
            workload,
            snapshot_version: snapshot.version,
            snapshot_updated_at: snapshot.updated_at,
            snapshot_staleness_ms,
            snapshot_live_workers: live_workers,
            snapshot_capable_workers: capable_workers,
            ring_self: self.self_addr(),
            ring_members: self.conf.transfer.endpoints.clone(),
        }
    }

    /// This instance's rpc address, i.e. the value the ring holds for it.
    pub fn self_addr(&self) -> String {
        format!(
            "{}:{}",
            self.conf.transfer.hostname, self.conf.transfer.rpc_port
        )
    }

    /// Same three questions `/readyz` asks, but answered with a label instead of
    /// a 503 body. The one deliberate difference: a stale snapshot that
    /// `allow_submit_with_stale_snapshot` lets through is `degraded`, not
    /// `not_ready` — the instance does accept work, plans may just fail.
    fn evaluate(&self, snapshot_updated_at: i64, staleness_ms: i64) -> (&'static str, String) {
        if !self.ready.load(Ordering::Relaxed) {
            return (STATE_NOT_READY, "starting".to_string());
        }
        if let Err(err) = self.service.check_store_available() {
            return (
                STATE_NOT_READY,
                format!(
                    "{} transfer metadata store is unavailable: {}",
                    self.store_backend, err
                ),
            );
        }
        if snapshot_updated_at <= 0 {
            return (
                STATE_NOT_READY,
                "cluster metadata snapshot has not been refreshed yet".to_string(),
            );
        }
        let max_staleness_ms = duration_ms(self.conf.transfer.cluster_snapshot_max_staleness);
        if staleness_ms > max_staleness_ms {
            let reason = format!(
                "cluster metadata snapshot is {}ms stale (limit {}ms)",
                staleness_ms, max_staleness_ms
            );
            return if self.conf.transfer.allow_submit_with_stale_snapshot {
                (
                    STATE_DEGRADED,
                    format!("{reason}; submissions still accepted"),
                )
            } else {
                (STATE_NOT_READY, reason)
            };
        }
        (STATE_NORMAL, String::new())
    }
}

fn worker_counts(workers: &[curvine_model::WorkerInfo]) -> (u64, u64) {
    let live = workers.iter().filter(|worker| worker.writable()).count();
    let capable = workers
        .iter()
        .filter(|worker| worker.writable() && worker.transfer_capabilities.supports_transfer())
        .count();
    (live as u64, capable as u64)
}

fn duration_ms(value: std::time::Duration) -> i64 {
    i64::try_from(value.as_millis()).unwrap_or(i64::MAX)
}

fn now_ms() -> i64 {
    i64::try_from(LocalTime::mills()).unwrap_or(i64::MAX)
}

/// Maps a collected workload into the shape [`TransferServiceState`] expects.
///
/// Kept as a free function so the service and the probe agree on the derivation
/// without the probe having to know which counts are cheap.
pub fn workload_from_counts(
    active_jobs: u64,
    executing_jobs: u64,
    cache: Option<&crate::transfer::TaskCache>,
) -> TransferWorkload {
    let Some(cache) = cache else {
        return TransferWorkload::new(active_jobs, executing_jobs, 0, 0);
    };
    let jobs = cache.active_jobs();
    let now = now_ms();
    let mut running_tasks = 0u64;
    for (job_id, run_id) in &jobs {
        if let Some(summary) = cache.summary(job_id, *run_id, now) {
            running_tasks = running_tasks.saturating_add(summary.counts.running);
        }
    }
    TransferWorkload::new(
        active_jobs,
        executing_jobs,
        jobs.len() as u64,
        running_tasks,
    )
}

#[cfg(test)]
impl TransferServiceState {
    pub fn fixture(state: &'static str) -> Self {
        Self {
            role: "transfer",
            instance_id: "instance-1".to_string(),
            addr: "cv-transfer-0:9010".to_string(),
            web_addr: "cv-transfer-0:9011".to_string(),
            version: "0.1.0".to_string(),
            version_commit: "abc123".to_string(),
            start_time_ms: 1_000,
            uptime_ms: 61_000,
            ready: true,
            state,
            state_reason: String::new(),
            store_backend: "sqlite".to_string(),
            max_running_transfers: 8,
            accepts_jobs: true,
            worker_task_limit: 16,
            max_tasks_per_transfer: 500_000,
            max_plan_entries: 500_000,
            workload: TransferWorkload::new(5, 2, 1, 3),
            snapshot_version: 7,
            snapshot_updated_at: 62_000,
            snapshot_staleness_ms: 500,
            snapshot_live_workers: 3,
            snapshot_capable_workers: 3,
            ring_self: "cv-transfer-0:9010".to_string(),
            ring_members: vec![
                "cv-transfer-0:9010".to_string(),
                "cv-transfer-1:9010".to_string(),
            ],
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn json_mirrors_proto_field_for_field() {
        let state = TransferServiceState::fixture(STATE_NORMAL);
        let json = state.to_json();
        let proto = state.to_proto();

        assert_eq!(json["role"], proto.role.unwrap());
        assert_eq!(json["addr"], proto.addr.unwrap());
        assert_eq!(json["web_addr"], proto.web_addr.unwrap());
        assert_eq!(json["state"], proto.state.unwrap());
        assert_eq!(
            json["active_jobs"].as_u64().unwrap(),
            proto.active_jobs.unwrap()
        );
        assert_eq!(
            json["in_memory_running_tasks"].as_u64().unwrap(),
            proto.in_memory_running_tasks.unwrap()
        );
        assert_eq!(
            json["ring_members"].as_array().unwrap().len(),
            proto.ring_members.len()
        );
    }

    #[test]
    fn workload_without_a_cache_reports_no_in_memory_state() {
        let workload = workload_from_counts(5, 2, None);
        assert_eq!(workload.active_jobs, 5);
        assert_eq!(workload.executing_jobs, 2);
        assert_eq!(workload.in_memory_jobs, 0);
        assert_eq!(workload.in_memory_running_tasks, 0);
    }

    #[test]
    fn not_ready_is_distinct_from_degraded() {
        assert_ne!(STATE_NOT_READY, STATE_DEGRADED);
        assert_ne!(STATE_DEGRADED, STATE_NORMAL);
    }
}
