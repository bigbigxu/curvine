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

use std::sync::Arc;
use std::time::Duration;

use crate::common::UfsFactory;
use curvine_client_core::file::CurvineFileSystem;
use curvine_config::{ClusterConf, ConfigSourceMap, RuntimeConfigRegistry, TransferStoreType};
use curvine_core_error::CommonResult;
use curvine_error::FsError;
use curvine_net::net::ConnState;
use curvine_rpc::handler::HandlerService;
use curvine_rpc::server::{RpcServer, ServerStateListener};
use curvine_runtime::common::Logger;
use curvine_runtime::runtime::RpcRuntime;
use curvine_web::server::{WebHandlerService, WebServer};
use log::info;
use std::sync::atomic::{AtomicBool, Ordering};

use crate::transfer::{
    build_transfer_runtime_registry, ClusterMetadataCache, Forwarder, MemoryTransferStore,
    MysqlTransferStore, PostgresTransferStore, SqliteTransferStore, TaskCache, TransferHandler,
    TransferMetrics, TransferPlanner, TransferRouterHandler, TransferRuntimeConfig,
    TransferScheduler, TransferService, TransferServiceProbe, TransferStoreBackend,
};

const MIN_SNAPSHOT_REFRESH: Duration = Duration::from_secs(1);
const MAX_SNAPSHOT_REFRESH: Duration = Duration::from_secs(10);

fn snapshot_refresh_interval(mount_ttl: Duration, max_staleness: Duration) -> Duration {
    mount_ttl
        .clamp(MIN_SNAPSHOT_REFRESH, MAX_SNAPSHOT_REFRESH)
        .min(max_staleness / 2)
}

#[derive(Clone)]
pub struct TransferRpcService {
    service: TransferService<TransferStoreBackend>,
    forwarder: Option<Arc<Forwarder>>,
    ready: Arc<AtomicBool>,
    store_backend: &'static str,
    cluster_cache: ClusterMetadataCache,
    runtime_config: Arc<RuntimeConfigRegistry>,
    /// Layer attribution from the loader, for the config endpoint.
    config_sources: ConfigSourceMap,
    conf: Arc<ClusterConf>,
    /// One status source for both transports (RPC `GetTransferServiceInfo`
    /// and `GET /api/v1/transfer/info`).
    probe: Arc<TransferServiceProbe>,
}

impl TransferRpcService {
    #[allow(clippy::too_many_arguments)]
    fn new(
        service: TransferService<TransferStoreBackend>,
        forwarder: Option<Arc<Forwarder>>,
        ready: Arc<AtomicBool>,
        store_backend: &'static str,
        cluster_cache: ClusterMetadataCache,
        runtime_config: Arc<RuntimeConfigRegistry>,
        conf: Arc<ClusterConf>,
        config_sources: ConfigSourceMap,
        probe: Arc<TransferServiceProbe>,
    ) -> Self {
        Self {
            service,
            forwarder,
            ready,
            store_backend,
            cluster_cache,
            runtime_config,
            config_sources,
            conf,
            probe,
        }
    }
}

impl HandlerService for TransferRpcService {
    type Item = TransferHandler<TransferStoreBackend>;

    fn get_message_handler(&self, _: Option<ConnState>) -> Self::Item {
        let handler = TransferHandler::new(self.service.clone())
            .with_probe(self.probe.clone())
            .with_runtime_config(self.runtime_config.clone());
        match &self.forwarder {
            Some(forwarder) => handler.with_forwarder(forwarder.clone()),
            None => handler,
        }
    }
}

impl WebHandlerService for TransferRpcService {
    type Item = TransferRouterHandler;

    fn get_handler(&self) -> Self::Item {
        TransferRouterHandler::new(
            self.ready.clone(),
            self.store_backend,
            self.service.clone(),
            self.cluster_cache.clone(),
            self.runtime_config.clone(),
            &self.conf,
            self.config_sources.clone(),
            self.probe.clone(),
        )
    }
}

pub struct TransferServer {
    rpc_server: RpcServer<TransferRpcService>,
    web_server: WebServer<TransferRpcService>,
    cluster_cache: ClusterMetadataCache,
    scheduler: TransferScheduler<TransferStoreBackend>,
    ready: Arc<AtomicBool>,
    stop: Arc<AtomicBool>,
}

impl TransferServer {
    /// Starts a transfer server without configuration-layer attribution.
    pub fn with_conf(conf: ClusterConf) -> CommonResult<Self> {
        Self::with_conf_and_sources(conf, ConfigSourceMap::new())
    }

    /// Same as [`TransferServer::with_conf`] but keeps the loader's layer map so
    /// `GET /api/v1/transfer/config` can report where each value came from.
    pub fn with_conf_and_sources(
        conf: ClusterConf,
        config_sources: ConfigSourceMap,
    ) -> CommonResult<Self> {
        if !conf.transfer.enabled {
            return Err(FsError::common("curvine-transfer requires transfer.enabled=true").into());
        }
        Logger::init(conf.transfer.log.clone());
        let _ = TransferMetrics::get()?;
        info!("allocator: {}", curvine_alloc::allocator_type_name());
        info!("git version: {}", curvine_sys::version::GIT_VERSION);

        let rt = Arc::new(conf.transfer_server_conf().create_runtime());
        let stop = Arc::new(AtomicBool::new(false));
        let store = Arc::new(match conf.transfer.effective_store_type() {
            TransferStoreType::Auto => unreachable!("resolved transfer store type cannot be auto"),
            TransferStoreType::Memory => TransferStoreBackend::Memory(MemoryTransferStore::new()),
            TransferStoreType::Sqlite => TransferStoreBackend::Sqlite(SqliteTransferStore::open(
                conf.transfer.sqlite_store_path(),
            )?),
            TransferStoreType::Mysql => TransferStoreBackend::Mysql(MysqlTransferStore::open(
                conf.transfer.mysql_store_url(),
            )?),
            TransferStoreType::Postgres => TransferStoreBackend::Postgres(
                PostgresTransferStore::open(conf.transfer.postgres_store_url())?,
            ),
        });
        let store_backend = store.backend_label();
        // TransferConf::init always materializes endpoints (explicit config or
        // hostname:rpc_port). These are advertised to workers as progress report
        // targets — never substitute a fresh localhost default here.
        let report_endpoints = conf.transfer.endpoints.clone();
        if report_endpoints.is_empty() {
            return Err(FsError::common(
                "transfer.endpoints is empty after config init; set transfer.hostname / \
                 transfer.endpoints to a worker-reachable host:port",
            )
            .into());
        }
        info!(
            "configured curvine-transfer store_backend={} rpc_addr={}:{} web_addr={}:{} report_endpoints={:?} max_running={} max_tasks={} task_stale_timeout_ms={}",
            store_backend,
            conf.transfer.hostname,
            conf.transfer.rpc_port,
            conf.transfer.hostname,
            conf.transfer.web_port,
            report_endpoints,
            conf.transfer.max_running_transfers,
            conf.transfer.max_tasks_per_transfer,
            conf.transfer.task_stale_timeout.as_millis(),
        );
        let fs = CurvineFileSystem::with_rt(conf.clone(), rt.clone())?;
        let factory = Arc::new(UfsFactory::with_rt(&conf.client, rt.clone()));
        let cache = ClusterMetadataCache::with_snapshot_policy(
            fs.clone(),
            conf.transfer.cluster_snapshot_max_staleness,
            conf.transfer.allow_submit_with_stale_snapshot,
        );
        let snapshot_refresh = snapshot_refresh_interval(
            Duration::from_millis(conf.client.mount_update_ttl_ms),
            conf.transfer.cluster_snapshot_max_staleness,
        );
        cache
            .clone()
            .start_refresh_loop(snapshot_refresh, stop.clone());
        // Lease owners are process-unique. A configured static instance id
        // can be reused by two HA replicas and defeat owner-only fencing.
        let owner = uuid::Uuid::new_v4().to_string();
        // One state object, two consumers: the planner reads the task cap while
        // planning and the scheduler reads the worker ceiling on every dispatch
        // decision; the registry below is its only write path.
        let runtime_state = Arc::new(TransferRuntimeConfig::from_conf(&conf));
        let planner = TransferPlanner::new(
            fs.clone(),
            factory.clone(),
            cache.clone(),
            conf.client.clone(),
            runtime_state.clone(),
            conf.transfer.ufs_max_concurrency_per_endpoint,
            curvine_config::LoadTransferDefaults::from_job(&conf.job),
        );
        // One cache shared by the scheduler (dispatch) and the service
        // (reports/reads): file-level tasks exist only here.
        let task_cache = Arc::new(TaskCache::new());
        // Placement and relay: discovery mode only.
        let forwarder = build_forwarder(&conf, rt.clone());
        let scheduler = TransferScheduler::new(
            store.clone(),
            planner,
            cache.clone(),
            factory,
            fs,
            owner.clone(),
            // Workers report back to whoever dispatched the task: that is the
            // lease owner and the only instance holding the job's in-memory
            // task state.
            format!("{}:{}", conf.transfer.hostname, conf.transfer.rpc_port),
            conf.transfer.clone(),
            runtime_state.clone(),
            task_cache.clone(),
            forwarder.clone(),
        );

        let ready = Arc::new(AtomicBool::new(false));
        let runtime_config = Arc::new(build_transfer_runtime_registry(
            runtime_state.clone(),
            &conf,
        ));
        let transfer_service = TransferService::with_cache_and_report_queue(
            store,
            cache.clone(),
            conf.transfer.task_stale_timeout,
            conf.transfer.task_report_queue_size(),
            conf.transfer.worker_threads,
        )?
        .with_job_defaults(
            i64::try_from(conf.transfer.job_default_ttl.as_millis()).unwrap_or(86_400_000),
            conf.transfer.job_default_max_runs,
        )
        .with_task_cache(task_cache.clone())
        // The submit brake and the executing ceiling live here, so the service
        // must read the same object the config registry writes.
        .with_runtime_config(runtime_state.clone());
        // One probe, two transports: the RPC handler and the web router both
        // answer `GetTransferServiceInfo` / `GET /api/v1/transfer/info` from it,
        // so the two surfaces cannot drift apart. It clones the service, which
        // shares the same store, report queue and task cache.
        let probe = Arc::new(TransferServiceProbe::new(
            owner,
            ready.clone(),
            store_backend,
            transfer_service.clone(),
            cache.clone(),
            runtime_state,
            Arc::new(conf.clone()),
        ));
        let service = TransferRpcService::new(
            transfer_service,
            forwarder,
            ready.clone(),
            store_backend,
            cache.clone(),
            runtime_config,
            Arc::new(conf.clone()),
            config_sources,
            probe,
        );
        let rpc_server =
            RpcServer::with_rt(rt.clone(), conf.transfer_server_conf(), service.clone());
        let web_server = WebServer::with_rt(rt, conf.transfer_web_conf(), service);
        let stop_on_rpc_stop = stop.clone();
        rpc_server.add_shutdown_hook(move || {
            stop_on_rpc_stop.store(true, Ordering::Relaxed);
        });
        Ok(Self {
            rpc_server,
            web_server,
            cluster_cache: cache,
            scheduler,
            ready,
            stop,
        })
    }

    pub async fn start(self) -> CommonResult<ServerStateListener> {
        let ready = self.ready.clone();
        let web_name = self.web_server.server_name().to_string();
        let bind_addr = self.web_server.resolve_bind_addr();
        let mut web_status = self.web_server.start();
        WebServer::<TransferRpcService>::wait_bind(&mut web_status, &web_name, &bind_addr).await?;

        let result = async {
            self.cluster_cache.refresh().await?;
            self.scheduler
                .start(self.rpc_server.clone_rt(), self.stop.clone());
            info!("Starting curvine-transfer rpc server");
            let mut rpc_status = self.rpc_server.start();
            rpc_status.wait_running().await?;
            ready.store(true, Ordering::Relaxed);
            Ok(rpc_status)
        }
        .await;

        if result.is_err() {
            self.stop.store(true, Ordering::Relaxed);
        }
        result
    }

    pub fn block_on_start(self) -> CommonResult<()> {
        let rt = self.rpc_server.clone_rt();
        rt.block_on(async move {
            let mut status = self.start().await?;
            if let Err(err) = status.wait_stop().await {
                log::warn!("curvine-transfer wait stop failed: {}", err);
            }
            Ok(())
        })
    }
}

/// Placement ring over the configured endpoint list.
///
/// `transfer.endpoints` is the authoritative instance list: no registry, no
/// heartbeat, no refresh. Every instance must be configured with the same list;
/// instance changes are config changes.
fn build_forwarder(
    conf: &ClusterConf,
    rt: Arc<curvine_runtime::runtime::Runtime>,
) -> Option<Arc<Forwarder>> {
    if conf.transfer.endpoints.is_empty() {
        return None;
    }
    let self_addr = format!("{}:{}", conf.transfer.hostname, conf.transfer.rpc_port);
    let factory = Arc::new(curvine_rpc::client::ClientFactory::with_rt(
        conf.client_rpc_conf(),
        rt,
    ));
    Some(Arc::new(Forwarder::new(
        conf.transfer.endpoints.clone(),
        self_addr,
        factory,
    )))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snapshot_refresh_keeps_default_cadence() {
        assert_eq!(
            snapshot_refresh_interval(Duration::from_secs(60), Duration::from_secs(60)),
            Duration::from_secs(10)
        );
        assert_eq!(
            snapshot_refresh_interval(Duration::from_secs(10), Duration::from_secs(60)),
            Duration::from_secs(10)
        );
    }

    #[test]
    fn snapshot_refresh_is_capped_by_staleness_budget() {
        // The staleness bound must win even when staleness / 2 is below the
        // refresh floor, otherwise snapshots age past the freshness limit.
        assert_eq!(
            snapshot_refresh_interval(Duration::from_secs(60), Duration::from_secs(1)),
            Duration::from_millis(500)
        );
        assert_eq!(
            snapshot_refresh_interval(Duration::from_millis(100), Duration::from_millis(500)),
            Duration::from_millis(250)
        );
    }

    #[test]
    fn snapshot_refresh_is_bounded_by_floor_and_ceiling() {
        assert_eq!(
            snapshot_refresh_interval(Duration::from_millis(1), Duration::from_secs(60)),
            Duration::from_secs(1)
        );
        assert_eq!(
            snapshot_refresh_interval(Duration::from_secs(600), Duration::from_secs(300)),
            Duration::from_secs(10)
        );
    }
}
