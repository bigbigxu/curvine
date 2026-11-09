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

use std::time::Duration;

use curvine_config::ClusterConf;
use curvine_model::{FilesystemInfo, WorkerInfo};
use curvine_unified_fs::UnifiedFileSystem;
use tokio::time::timeout;

use crate::util::addr::{
    fuse_metrics_instance, fuse_metrics_scrape_url, master_metrics_scrape_url,
    worker_metrics_display_hostname, worker_metrics_instance, worker_metrics_scrape_url,
};

use super::model::{MetricTarget, ACTIVE_MASTER_INSTANCE};

const MASTER_INFO_TIMEOUT_SECS: u64 = 5;

pub struct MasterDiscovery {
    pub target: MetricTarget,
    pub error: Option<String>,
}

pub struct WorkerDiscovery {
    pub target: MetricTarget,
    pub warning: Option<String>,
}

pub struct DiscoveryResult {
    pub master: MasterDiscovery,
    pub workers: Vec<WorkerDiscovery>,
    pub fuses: Vec<MetricTarget>,
    pub transfers: Vec<MetricTarget>,
    /// Populated when `get_master_info` succeeds; used to backfill master gauge
    /// metrics when the master `/metrics` HTTP scrape is unavailable.
    pub master_info: Option<FilesystemInfo>,
}

pub fn build_master_metrics_target(
    conf: &ClusterConf,
    active_master: &str,
    active_raft_id: Option<u64>,
) -> MetricTarget {
    let trimmed = active_master.trim();
    let url = master_metrics_scrape_url(conf, trimmed, active_raft_id);
    let active_master_rpc = if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_string())
    };
    MetricTarget::master(ACTIVE_MASTER_INSTANCE.to_string(), url, active_master_rpc)
}

pub fn build_worker_metrics_target(
    worker: &WorkerInfo,
    worker_state: &str,
    conf: &ClusterConf,
) -> WorkerDiscovery {
    let worker_id = worker.worker_id();
    let instance = worker_metrics_instance(&worker.address, conf);
    let hostname = worker_metrics_display_hostname(&worker.address);
    let mut warning = None;
    if hostname.is_empty() {
        warning = Some(format!(
            "worker {worker_id} has no routable hostname or IP for metrics scrape"
        ));
    } else if crate::util::addr::is_loopback_hostname(&hostname) {
        warning = Some(format!(
            "worker {worker_id} metrics endpoint uses loopback host {hostname}"
        ));
    }
    let url = if hostname.is_empty() {
        format!("http://invalid-worker-{worker_id}/metrics")
    } else {
        worker_metrics_scrape_url(&worker.address, conf)
    };
    WorkerDiscovery {
        target: MetricTarget::worker(instance, url, worker_id, worker_state),
        warning,
    }
}

fn discover_workers(
    workers: &[WorkerInfo],
    worker_state: &str,
    conf: &ClusterConf,
) -> Vec<WorkerDiscovery> {
    workers
        .iter()
        .map(|worker| build_worker_metrics_target(worker, worker_state, conf))
        .collect()
}

fn discover_workers_from_master_info(
    info: &FilesystemInfo,
    conf: &ClusterConf,
) -> Vec<WorkerDiscovery> {
    let mut workers = Vec::new();
    workers.extend(discover_workers(&info.live_workers, "live", conf));
    workers.extend(discover_workers(&info.blacklist_workers, "blacklist", conf));
    workers.extend(discover_workers(
        &info.decommission_workers,
        "decommission",
        conf,
    ));
    workers.extend(discover_workers(&info.lost_workers, "lost", conf));
    workers
}

pub fn discover_fuse_targets(conf: &ClusterConf) -> Vec<MetricTarget> {
    let fuse_conf = &conf.web.observability.fuse;
    if !fuse_conf.enabled {
        return Vec::new();
    }
    let endpoints: Vec<String> = if fuse_conf.targets.is_empty() {
        vec![format!("127.0.0.1:{}", conf.fuse.web_port)]
    } else {
        fuse_conf.targets.clone()
    };
    endpoints
        .into_iter()
        .map(|endpoint| {
            let instance = fuse_metrics_instance(&endpoint);
            let url = fuse_metrics_scrape_url(&endpoint);
            MetricTarget::fuse(instance, url)
        })
        .collect()
}

/// Scrape targets for every configured transfer instance.
///
/// `transfer.endpoints` only carries RPC ports, so the web port each instance
/// scraper needs is asked for rather than configured: one
/// `GetTransferServiceInfo` per instance, the same call the Runtime page makes.
/// An instance that does not answer cannot report a port, so it falls back to
/// the console's own `transfer.web_port` — the URL then fails to scrape, which
/// is the honest outcome for an instance that is down.
pub async fn discover_transfer_targets(
    conf: &ClusterConf,
    unified_fs: &UnifiedFileSystem,
    cache: &crate::transfer_services::TransferServicesCache,
) -> Vec<MetricTarget> {
    if !conf.transfer.enabled {
        return Vec::new();
    }
    let instances = cache.instances(conf, unified_fs.fs_context()).await;
    transfer_targets_from_instances(conf, &instances)
}

/// Pure mapping from probe answers to scrape targets.
///
/// Split out so the address rules — reported web address, bind-all hosts, and
/// the down-instance fallback — are testable without a live transfer service.
pub fn transfer_targets_from_instances(
    conf: &ClusterConf,
    instances: &[crate::transfer_services::TransferInstanceStatus],
) -> Vec<MetricTarget> {
    if !conf.transfer.enabled {
        return Vec::new();
    }
    let mut targets: Vec<MetricTarget> = Vec::new();
    for status in instances {
        let instance = transfer_metrics_addr(
            status.reported_addr(),
            status
                .info
                .as_ref()
                .and_then(|info| info.web_addr.as_deref()),
            conf.transfer.web_port,
        );
        if targets.iter().any(|target| target.instance == instance) {
            continue;
        }
        let url = format!("http://{instance}/metrics");
        targets.push(MetricTarget::transfer(instance, url));
    }
    targets
}

/// `host:port` a scraper can actually reach.
///
/// The instance reports its own `web_addr`, but its hostname may be a bind-all
/// address (`0.0.0.0`, `::`) chosen so the process can listen on every
/// interface. Scraping that is meaningless, so the host comes from the endpoint
/// the console connected to — which is by definition reachable from here — while
/// the port comes from the answer, because it differs per instance.
fn transfer_metrics_addr(endpoint: &str, web_addr: Option<&str>, fallback_port: u16) -> String {
    let (endpoint_host, _) = split_host_port(endpoint);
    let (web_host, web_port) = split_host_port(web_addr.unwrap_or(""));
    let host = if web_host.is_empty() || is_bind_all(&web_host) {
        endpoint_host
    } else {
        web_host
    };
    let port = web_port.unwrap_or(fallback_port);
    format!("{host}:{port}")
}

/// Splits `host:port`, tolerating bracketed IPv6 and a missing port.
fn split_host_port(value: &str) -> (String, Option<u16>) {
    let value = value.trim();
    if value.is_empty() {
        return (String::new(), None);
    }
    // `[::1]:9011`: the colon inside the brackets is not the separator.
    if let Some(rest) = value.strip_prefix('[') {
        if let Some((host, tail)) = rest.split_once(']') {
            let port = tail.strip_prefix(':').and_then(|port| port.parse().ok());
            return (host.to_string(), port);
        }
    }
    // More than one colon means an unbracketed IPv6 literal (`::1`), which has no
    // port to split off: every colon belongs to the address.
    if value.matches(':').count() > 1 {
        return (value.to_string(), None);
    }
    match value.rsplit_once(':') {
        Some((host, port)) => match port.parse() {
            Ok(port) => (host.to_string(), Some(port)),
            // Not a port: the whole value is a host.
            Err(_) => (value.to_string(), None),
        },
        None => (value.to_string(), None),
    }
}

fn is_bind_all(host: &str) -> bool {
    matches!(host, "0.0.0.0" | "::" | "[::]" | "*")
}

pub async fn discover_targets(
    conf: &ClusterConf,
    unified_fs: &UnifiedFileSystem,
    transfer_cache: &crate::transfer_services::TransferServicesCache,
) -> DiscoveryResult {
    match timeout(
        Duration::from_secs(MASTER_INFO_TIMEOUT_SECS),
        unified_fs.get_filesystem_info(),
    )
    .await
    {
        Ok(Ok(info)) => DiscoveryResult {
            master: MasterDiscovery {
                target: build_master_metrics_target(
                    conf,
                    info.active_master.trim(),
                    info.active_raft_id,
                ),
                error: None,
            },
            workers: discover_workers_from_master_info(&info, conf),
            fuses: discover_fuse_targets(conf),
            transfers: discover_transfer_targets(conf, unified_fs, transfer_cache).await,
            master_info: Some(info),
        },
        Ok(Err(err)) => DiscoveryResult {
            master: MasterDiscovery {
                target: build_master_metrics_target(conf, "", None),
                error: Some(err.to_string()),
            },
            workers: Vec::new(),
            fuses: discover_fuse_targets(conf),
            transfers: discover_transfer_targets(conf, unified_fs, transfer_cache).await,
            master_info: None,
        },
        Err(_) => DiscoveryResult {
            master: MasterDiscovery {
                target: build_master_metrics_target(conf, "", None),
                error: Some(format!(
                    "master info request timed out after {MASTER_INFO_TIMEOUT_SECS}s"
                )),
            },
            workers: Vec::new(),
            fuses: discover_fuse_targets(conf),
            transfers: discover_transfer_targets(conf, unified_fs, transfer_cache).await,
            master_info: None,
        },
    }
}

#[cfg(test)]
mod tests {
    use curvine_model::WorkerAddress;
    use curvine_net::net::InetAddr;

    use super::*;
    use crate::util::addr::{fuse_metrics_scrape_url, master_metrics_scrape_url};

    fn test_conf() -> ClusterConf {
        let mut conf = ClusterConf::default();
        conf.master.hostname = "localhost".to_string();
        conf.master.rpc_port = 8995;
        conf.worker.web_port = 9001;
        conf.client.master_addrs = vec![InetAddr::new("cv-master-0", 8995)];
        conf
    }

    fn sample_worker(
        worker_id: u32,
        hostname: &str,
        ip_addr: &str,
        rpc_port: u32,
        web_port: u32,
    ) -> WorkerInfo {
        WorkerInfo::new(
            WorkerAddress {
                worker_id,
                hostname: hostname.to_string(),
                ip_addr: ip_addr.to_string(),
                rpc_port,
                web_port,
            },
            WorkerInfo::default_weight(),
        )
    }

    #[test]
    fn builds_master_target_for_host_port_mapped_cluster() {
        let mut conf = ClusterConf::default();
        conf.master.rpc_port = 8995;
        conf.master.web_port = 9000;
        conf.client.master_addrs = vec![
            InetAddr::new("cv-master-1", 18995),
            InetAddr::new("cv-master-2", 28995),
            InetAddr::new("cv-master-3", 38995),
        ];
        conf.client.master_web_addrs = vec![
            InetAddr::new("cv-master-1", 19000),
            InetAddr::new("cv-master-2", 29000),
            InetAddr::new("cv-master-3", 39000),
        ];
        let target = build_master_metrics_target(&conf, "cv-master-2:8995", None);
        assert_eq!(target.component, "master");
        assert_eq!(target.instance, ACTIVE_MASTER_INSTANCE);
        assert_eq!(
            target.url,
            master_metrics_scrape_url(&conf, "cv-master-2:8995", None)
        );
        assert_eq!(target.id, "master:active-master");
        assert_eq!(target.active_master.as_deref(), Some("cv-master-2:8995"));
    }

    #[test]
    fn builds_master_target_from_configured_master_when_active_is_loopback() {
        let conf = test_conf();
        let target = build_master_metrics_target(&conf, "127.0.0.1:8995", None);
        assert_eq!(target.component, "master");
        assert_eq!(target.instance, ACTIVE_MASTER_INSTANCE);
        assert_eq!(target.url, "http://cv-master-0:9010/metrics");
        assert_eq!(target.id, "master:active-master");
    }

    #[test]
    fn builds_master_target_from_active_master_in_ha() {
        let mut conf = test_conf();
        conf.master.hostname = "cv-master-0".to_string();
        let target = build_master_metrics_target(&conf, "cv-master-1:8995", None);
        assert_eq!(target.instance, ACTIVE_MASTER_INSTANCE);
        assert_eq!(target.url, "http://cv-master-1:9010/metrics");
    }

    #[test]
    fn worker_target_uses_web_port_not_rpc_port() {
        let conf = test_conf();
        let worker = sample_worker(1, "cv-worker-0", "10.0.0.1", 8996, 9008);
        let discovery = build_worker_metrics_target(&worker, "live", &conf);
        assert_eq!(discovery.target.instance, "cv-worker-0:9008");
        assert_eq!(
            discovery.target.url,
            crate::util::addr::worker_metrics_scrape_url(&worker.address, &conf)
        );
        assert!(!discovery.target.url.contains(":8996"));
    }

    #[test]
    fn worker_target_prefers_reported_web_port_over_conf() {
        let conf = test_conf();
        let worker = sample_worker(2, "w2", "10.0.0.2", 8996, 9010);
        let discovery = build_worker_metrics_target(&worker, "live", &conf);
        assert_eq!(discovery.target.instance, "w2:9010");
    }

    #[test]
    fn worker_target_falls_back_to_conf_web_port_when_reported_zero() {
        let conf = test_conf();
        let worker = sample_worker(3, "w3", "10.0.0.3", 8996, 0);
        let discovery = build_worker_metrics_target(&worker, "live", &conf);
        assert_eq!(discovery.target.instance, "w3:9001");
    }

    #[test]
    fn worker_target_falls_back_to_ip_when_hostname_loopback() {
        let conf = test_conf();
        let worker = sample_worker(4, "localhost", "10.0.0.4", 8996, 9001);
        let discovery = build_worker_metrics_target(&worker, "live", &conf);
        assert_eq!(discovery.target.instance, "10.0.0.4:9001");
    }

    #[test]
    fn worker_target_preserves_worker_state() {
        let conf = test_conf();
        let worker = sample_worker(5, "w5", "10.0.0.5", 8996, 9001);
        for state in ["live", "blacklist", "decommission", "lost"] {
            let discovery = build_worker_metrics_target(&worker, state, &conf);
            assert_eq!(discovery.target.worker_state.as_deref(), Some(state));
            assert_eq!(discovery.target.worker_id, Some(5));
        }
    }

    #[test]
    fn invalid_worker_does_not_abort_discovery_batch() {
        let conf = test_conf();
        let valid = sample_worker(6, "w6", "10.0.0.6", 8996, 9001);
        let invalid = sample_worker(7, "", "", 8996, 9001);
        let workers: Vec<_> = [valid, invalid]
            .iter()
            .map(|worker| build_worker_metrics_target(worker, "live", &conf))
            .collect();
        assert_eq!(workers.len(), 2);
        assert_eq!(workers[0].target.instance, "w6:9001");
        assert!(workers[1].warning.is_some());
        assert_eq!(workers[1].target.worker_id, Some(7));
    }

    #[test]
    fn discovers_all_worker_states_from_master_info() {
        let conf = test_conf();
        let info = FilesystemInfo {
            live_workers: vec![sample_worker(1, "w1", "10.0.0.1", 8996, 9001)],
            blacklist_workers: vec![sample_worker(2, "w2", "10.0.0.2", 8996, 9001)],
            decommission_workers: vec![sample_worker(3, "w3", "10.0.0.3", 8996, 9001)],
            lost_workers: vec![sample_worker(4, "w4", "10.0.0.4", 8996, 9001)],
            ..Default::default()
        };
        let workers = discover_workers_from_master_info(&info, &conf);
        assert_eq!(workers.len(), 4);
        let states: Vec<_> = workers
            .iter()
            .filter_map(|worker| worker.target.worker_state.clone())
            .collect();
        assert!(states.contains(&"live".to_string()));
        assert!(states.contains(&"blacklist".to_string()));
        assert!(states.contains(&"decommission".to_string()));
        assert!(states.contains(&"lost".to_string()));
    }

    #[test]
    fn discovers_no_fuse_targets_when_disabled_by_default() {
        let conf = test_conf();
        assert!(!conf.web.observability.fuse.enabled);
        assert!(discover_fuse_targets(&conf).is_empty());
    }

    #[test]
    fn discovers_fuse_targets_from_default_local_endpoint() {
        let mut conf = test_conf();
        conf.web.observability.fuse.enabled = true;
        let targets = discover_fuse_targets(&conf);
        assert_eq!(targets.len(), 1);
        assert_eq!(targets[0].component, "fuse");
        assert_eq!(
            targets[0].instance,
            format!("127.0.0.1:{}", conf.fuse.web_port)
        );
        assert_eq!(
            targets[0].url,
            fuse_metrics_scrape_url(&format!("127.0.0.1:{}", conf.fuse.web_port))
        );
    }

    #[test]
    fn discovers_configured_fuse_targets() {
        let mut conf = test_conf();
        conf.web.observability.fuse.enabled = true;
        conf.web.observability.fuse.targets = vec!["10.0.0.9:9002".to_string()];
        let targets = discover_fuse_targets(&conf);
        assert_eq!(targets.len(), 1);
        assert_eq!(targets[0].instance, "10.0.0.9:9002");
    }

    fn transfer_status(
        addr: &str,
        web_addr: Option<&str>,
    ) -> crate::transfer_services::TransferInstanceStatus {
        crate::transfer_services::TransferInstanceStatus {
            addr: addr.to_string(),
            reachable: web_addr.is_some(),
            error: web_addr.is_none().then(|| "no answer".to_string()),
            info: web_addr.map(|web_addr| curvine_proto::TransferServiceInfoResponse {
                role: Some("transfer".to_string()),
                addr: Some(addr.to_string()),
                web_addr: Some(web_addr.to_string()),
                ..Default::default()
            }),
        }
    }

    #[test]
    fn discovers_no_transfer_target_when_disabled() {
        let mut conf = test_conf();
        conf.transfer.enabled = false;
        assert!(transfer_targets_from_instances(&conf, &[]).is_empty());
        // Enabled instances still produce nothing while transfer is off: the
        // console must not scrape a service the cluster does not run.
        let instances = vec![transfer_status(
            "cv-transfer-0:9010",
            Some("cv-transfer-0:9011"),
        )];
        assert!(transfer_targets_from_instances(&conf, &instances).is_empty());
    }

    #[test]
    fn discovers_one_target_per_configured_instance() {
        let mut conf = test_conf();
        conf.transfer.enabled = true;
        let instances = vec![
            transfer_status("cv-transfer-0:9010", Some("cv-transfer-0:9011")),
            transfer_status("10.0.0.9:9010", Some("10.0.0.9:9021")),
            transfer_status(
                "cv-transfer-1.svc.local:9010",
                Some("cv-transfer-1.svc.local:9011"),
            ),
        ];
        let targets = transfer_targets_from_instances(&conf, &instances);
        assert_eq!(targets.len(), 3);
        assert_eq!(targets[0].component, "transfer");
        assert_eq!(targets[0].id, "transfer:cv-transfer-0:9011");
        assert_eq!(targets[0].url, "http://cv-transfer-0:9011/metrics");
        // The port comes from the instance, not from this console's config.
        assert_eq!(targets[1].instance, "10.0.0.9:9021");
        assert_eq!(
            targets[2].url,
            "http://cv-transfer-1.svc.local:9011/metrics"
        );
    }

    #[test]
    fn replaces_a_bind_all_host_with_the_configured_endpoint_host() {
        // `hostname = 0.0.0.0` is a legal listen address but a useless scrape
        // target, so the host comes from the endpoint the console connected to.
        let mut conf = test_conf();
        conf.transfer.enabled = true;
        let instances = vec![transfer_status("10.0.0.9:9010", Some("0.0.0.0:9011"))];
        let targets = transfer_targets_from_instances(&conf, &instances);
        assert_eq!(targets[0].instance, "10.0.0.9:9011");
        assert_eq!(targets[0].url, "http://10.0.0.9:9011/metrics");
    }

    #[test]
    fn a_silent_instance_still_gets_a_target() {
        // No answer means no reported port: keep the row visible with the
        // configured web port so the failure surfaces as up=0 rather than as a
        // silently missing instance.
        let mut conf = test_conf();
        conf.transfer.enabled = true;
        conf.transfer.web_port = 9011;
        let instances = vec![transfer_status("10.0.0.9:9010", None)];
        let targets = transfer_targets_from_instances(&conf, &instances);
        assert_eq!(targets.len(), 1);
        assert_eq!(targets[0].instance, "10.0.0.9:9011");
    }

    #[test]
    fn duplicate_instances_collapse_into_one_target() {
        let mut conf = test_conf();
        conf.transfer.enabled = true;
        let instances = vec![
            transfer_status("cv-transfer-0:9010", Some("cv-transfer-0:9011")),
            transfer_status("cv-transfer-0:9010", Some("cv-transfer-0:9011")),
        ];
        assert_eq!(transfer_targets_from_instances(&conf, &instances).len(), 1);
    }

    #[test]
    fn parses_host_and_port_for_every_address_shape() {
        assert_eq!(
            split_host_port("host:9011"),
            ("host".to_string(), Some(9011))
        );
        assert_eq!(split_host_port("host"), ("host".to_string(), None));
        assert_eq!(
            split_host_port("[::1]:9011"),
            ("::1".to_string(), Some(9011))
        );
        assert_eq!(split_host_port("::1"), ("::1".to_string(), None));
        assert_eq!(split_host_port(""), (String::new(), None));
    }
}
