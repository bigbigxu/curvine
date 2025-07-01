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

#![allow(unused)]

use crate::worker::block::{BlockActor, BlockStore};
use crate::worker::handler::{WorkerHandler, WorkerRouterHandler};
use crate::worker::load::FileLoadService;
use crate::worker::WorkerMetrics;
use curvine_client::file::FsContext;
use curvine_common::conf::ClusterConf;
use curvine_common::state::{HeartbeatStatus, WorkerAddress};
use curvine_web::server::{WebHandlerService, WebServer};
use log::{error, info};
use once_cell::sync::OnceCell;
use orpc::common::{LocalTime, Logger, Metrics};
use orpc::handler::HandlerService;
use orpc::io::net::ConnState;
use orpc::runtime::{RpcRuntime, Runtime};
use orpc::server::RpcServer;
use orpc::CommonResult;
use std::sync::Arc;

static CLUSTER_CONF: OnceCell<ClusterConf> = OnceCell::new();

static WORKER_METRICS: OnceCell<WorkerMetrics> = OnceCell::new();

#[derive(Clone)]
pub struct WorkerService {
    store: BlockStore,
    conf: ClusterConf,
    file_loader: Arc<FileLoadService>,
    rt: Arc<Runtime>,
}

impl WorkerService {
    pub fn from_conf(conf: &ClusterConf) -> CommonResult<Self> {
        let store: BlockStore = BlockStore::new(&conf.cluster_id, conf)?;
        let rt = Arc::new(conf.worker_server_conf().create_runtime());
        let fs_context = FsContext::with_rt(conf.clone(), rt.clone())?;
        let mut file_loader =
            FileLoadService::from_cluster_conf(Arc::from(fs_context), rt.clone(), conf);

        if let Err(e) = file_loader.start() {
            error!("Failed to start FileLoadService: {}", e);
            return Err(e);
        }

        let ws = Self {
            store,
            conf: conf.clone(),
            file_loader: Arc::from(file_loader),
            rt: rt.clone(),
        };
        Ok(ws)
    }
}

impl HandlerService for WorkerService {
    type Item = WorkerHandler;

    fn get_message_handler(&self, _: Option<ConnState>) -> Self::Item {
        WorkerHandler {
            store: self.store.clone(),
            handler: None,
            file_loader: self.file_loader.clone(),
            rt: self.rt.clone(),
        }
    }
}

impl WebHandlerService for WorkerService {
    type Item = WorkerRouterHandler;

    fn get_handler(&self) -> Self::Item {
        WorkerRouterHandler {}
    }
}

// block data start service.
pub struct Worker {
    start_ms: u64,
    worker_id: u32,
    rpc_server: RpcServer<WorkerService>,
    web_server: WebServer<WorkerService>,
    conf: ClusterConf,
    addr: WorkerAddress,
    block_actor: BlockActor,
}

impl Worker {
    pub fn new(conf: ClusterConf) -> CommonResult<Self> {
        Logger::init(conf.worker.log.clone());
        Metrics::init();

        let service: WorkerService = WorkerService::from_conf(&conf)?;
        let worker_id = service.store.worker_id();

        // The test cluster is started locally, and multiple worker global variables can only be registered once, so there is this judgment.
        if CLUSTER_CONF.get().is_none() {
            CLUSTER_CONF.set(conf.clone()).unwrap();
        }
        if WORKER_METRICS.get().is_none() {
            let metrics = WorkerMetrics::new(service.store.clone())?;
            WORKER_METRICS.set(metrics).unwrap();
        }

        conf.print();

        let block_store = service.store.clone();
        let rpc_server = RpcServer::with_rt(
            service.rt.clone(),
            conf.worker_server_conf(),
            service.clone(),
        );
        let web_server = WebServer::with_rt(service.rt.clone(), conf.worker_web_conf(), service);

        let net_addr = rpc_server.bind_addr();
        let addr = WorkerAddress {
            worker_id,
            hostname: net_addr.hostname.to_owned(),
            ip_addr: net_addr.hostname.to_owned(),
            rpc_port: net_addr.port as u32,
            web_port: conf.worker.web_port as u32,
        };
        let block_actor = BlockActor::new(
            rpc_server.new_rt(),
            &conf,
            addr.clone(),
            block_store,
            rpc_server.new_state_ctl(),
        );

        let master_client = block_actor.client.clone();
        rpc_server.add_shutdown_hook(move || {
            if let Err(e) = master_client.heartbeat(HeartbeatStatus::End, vec![]) {
                info!("error unregister {}", e)
            }
        });

        let worker = Self {
            worker_id,
            start_ms: LocalTime::mills(),
            rpc_server,
            web_server,
            conf: conf.clone(),
            addr,
            block_actor,
        };

        Ok(worker)
    }

    pub fn block_on_start(self) {
        let rt = self.rpc_server.new_rt();
        let mut listener = rt.block_on(async move {
            // step 2: Start rpc server
            let mut listener = self.rpc_server.start();
            listener.wait_running().await.unwrap();

            listener
        });

        // step 1: Start block heartbeat check service
        self.block_actor.start();

        // step3: Start the web server
        self.web_server.start();

        rt.block_on(listener.wait_stop()).unwrap();
    }

    // Start a standalone worker.
    pub fn start_standalone(&self) {
        self.rpc_server.block_on_start();
    }

    pub fn get_conf<'a>() -> &'a ClusterConf {
        CLUSTER_CONF.get().expect("Worker get conf error!")
    }

    pub fn get_metrics<'a>() -> &'a WorkerMetrics {
        WORKER_METRICS.get().expect("Worker get metrics error!")
    }

    pub fn get_block_store(&self) -> BlockStore {
        self.rpc_server.service().store.clone()
    }
}
