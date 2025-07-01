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

use once_cell::sync::OnceCell;
use std::sync::Arc;

use crate::master::fs::{FsRetryCache, MasterActor, MasterFilesystem};
use crate::master::journal::JournalSystem;
use crate::master::mount::MountManager;
use crate::master::router_handler::MasterRouterHandler;
use crate::master::MasterMetrics;
use crate::master::SyncMountManager;
use crate::master::{LoadManager, MasterHandler};
use curvine_common::conf::{ClusterConf, MasterConf};
use curvine_web::server::{WebHandlerService, WebServer};
use orpc::common::{DurationUnit, Logger, Metrics};
use orpc::handler::HandlerService;
use orpc::io::net::ConnState;
use orpc::runtime::{RpcRuntime, Runtime};
use orpc::server::RpcServer;
use orpc::CommonResult;

static MASTER_METRICS: OnceCell<MasterMetrics> = OnceCell::new();

#[derive(Clone)]
pub struct MasterHandlerService {
    conf: ClusterConf,
    fs: MasterFilesystem,
    retry_cache: Option<FsRetryCache>,
    mount_manager: SyncMountManager,
    load_manager: Arc<LoadManager>,
    rt: Arc<Runtime>,
}

impl MasterHandlerService {
    pub fn new(
        conf: ClusterConf,
        fs: MasterFilesystem,
        retry_cache: Option<FsRetryCache>,
        mount_manager: SyncMountManager,
        load_manager: Arc<LoadManager>,
        rt: Arc<Runtime>,
    ) -> Self {
        Self {
            conf,
            fs,
            retry_cache,
            mount_manager,
            load_manager,
            rt,
        }
    }
}

impl HandlerService for MasterHandlerService {
    type Item = MasterHandler;

    fn has_conn_state(&self) -> bool {
        true
    }

    fn get_message_handler(&self, client_state: Option<ConnState>) -> Self::Item {
        MasterHandler::new(
            &self.conf,
            self.fs.clone(),
            self.retry_cache.clone(),
            client_state,
            self.mount_manager.clone(),
            Arc::clone(&self.load_manager),
            self.rt.clone(),
        )
    }
}

impl WebHandlerService for MasterHandlerService {
    type Item = MasterRouterHandler;

    fn get_handler(&self) -> Self::Item {
        MasterRouterHandler::new(self.conf.clone(), self.fs.clone())
    }
}

pub struct Master {
    rpc_server: RpcServer<MasterHandlerService>,
    web_server: WebServer<MasterHandlerService>,
    journal_system: JournalSystem,
    actor: MasterActor,
    mount_manager: SyncMountManager,
    load_manager: Arc<LoadManager>,
}

impl Master {
    pub fn new(conf: ClusterConf) -> CommonResult<Self> {
        let mut log = conf.master.log.clone();
        if conf.master.audit_logging_enabled {
            log.targets = vec!["audit".to_string()]
        }
        Logger::init(log);

        Metrics::init();
        if MASTER_METRICS.get().is_none() {
            let metrics = MasterMetrics::new()?;
            MASTER_METRICS.set(metrics).unwrap();
        }

        conf.print();

        // step1: Create a journal system, the journal system determines how to create a fs dir.
        let journal_system = JournalSystem::from_conf(&conf)?;

        // step2: Create filesystem.
        let fs = MasterFilesystem::new(&conf, &journal_system)?;

        let actor = MasterActor::new(
            fs.clone(),
            journal_system.master_monitor(),
            conf.master.new_executor(),
        );

        let rt = Arc::new(conf.master_server_conf().create_runtime());

        let mount_manager = journal_system.mount_manager();
        mount_manager.write().set_master_fs(fs.clone());

        let load_manager = Arc::new(LoadManager::from_cluster_conf(
            Arc::new(fs.clone()),
            rt.clone(),
            &conf,
        ));

        // step3: Create rpc server.
        let retry_cache = Self::create_fs_retry_cache(&conf.master);
        let service = MasterHandlerService::new(
            conf.clone(),
            fs,
            retry_cache,
            mount_manager.clone(),
            Arc::clone(&load_manager),
            rt.clone(),
        );

        let rpc_conf = conf.master_server_conf();
        let rpc_server = RpcServer::with_rt(rt.clone(), rpc_conf, service.clone());

        // step4: Create a web server
        let web_conf = conf.master_web_conf();
        let web_server = WebServer::new(web_conf, service);

        Ok(Self {
            rpc_server,
            web_server,
            journal_system,
            actor,
            mount_manager,
            load_manager,
        })
    }

    pub fn block_on_start(self) {
        let rt = self.rpc_server.new_rt();
        rt.block_on(async move {
            // step 1: Start journal_system, raft server and raft node will be started internally
            let mut listener = self.journal_system.start().await.unwrap();
            listener.wait_role().await.unwrap();

            // step 2: Start rpc server
            let mut rpc_status = self.rpc_server.start();
            rpc_status.wait_running().await.unwrap();

            // step3: Start the web server
            self.web_server.start();

            // step4: Start master actor
            self.actor.start();

            // reload mountinfo
            self.mount_manager.read().restore();

            // step5: Start load manager
            self.load_manager.start();

            rpc_status.wait_stop().await.unwrap();
        });
    }

    pub fn create_fs_retry_cache(conf: &MasterConf) -> Option<FsRetryCache> {
        if conf.retry_cache_enable {
            let ttl = DurationUnit::from_str(&conf.retry_cache_ttl)
                .unwrap()
                .as_duration();
            let cache = FsRetryCache::new(conf.retry_cache_size, ttl);
            Some(cache)
        } else {
            None
        }
    }
    // for test
    pub fn get_fs(&self) -> MasterFilesystem {
        self.rpc_server.service().fs.clone()
    }

    pub fn get_metrics<'a>() -> &'a MasterMetrics {
        MASTER_METRICS.get().expect("Master get metrics error!")
    }

    // Instantiate metrics during testing
    pub fn init_test_metrics() {
        Metrics::init();
        let metrics = MasterMetrics::new().unwrap();
        MASTER_METRICS.get_or_init(|| metrics);
    }

    /// Get all active Worker nodes
    pub async fn get_active_workers(&self) -> Vec<String> {
        // Get active Worker node from MasterActor
        // Note: This is a simplified implementation and should actually be obtained from the Master Actor or Worker Manager
        // Currently, a test address is returned temporarily
        vec!["localhost:8081".to_string()]
    }
}

impl Default for Master {
    fn default() -> Self {
        Self::new(ClusterConf::format()).unwrap()
    }
}
