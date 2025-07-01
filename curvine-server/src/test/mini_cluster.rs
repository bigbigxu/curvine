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

use crate::master::fs::MasterFilesystem;
use crate::master::Master;
use crate::worker::block::BlockStore;
use crate::worker::Worker;
use curvine_client::file::CurvineFileSystem;
use curvine_common::conf::ClusterConf;
use curvine_common::raft::{NodeId, RaftPeer};
use curvine_common::FsResult;
use dashmap::DashMap;
use orpc::client::RpcClient;
use orpc::common::LocalTime;
use orpc::io::net::{InetAddr, NetUtils};
use orpc::runtime::{RpcRuntime, Runtime};
use orpc::{err_box, CommonResult};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

// A cluster of unit tests.
pub struct MiniCluster {
    // Cluster default configuration file
    pub cluster_conf: ClusterConf,
    pub master_conf: Vec<ClusterConf>,
    pub worker_conf: Vec<ClusterConf>,

    pub master_fs: DashMap<usize, MasterFilesystem>,
    pub worker_store: DashMap<u32, BlockStore>,
    pub client_rt: Arc<Runtime>,
}

impl MiniCluster {
    pub fn new(master_conf: Vec<ClusterConf>, worker_conf: Vec<ClusterConf>) -> Self {
        let client_rt = Arc::new(master_conf[0].client_rpc_conf().create_runtime());
        Self {
            cluster_conf: master_conf[0].clone(),
            master_conf,
            worker_conf,
            master_fs: DashMap::new(),
            worker_store: DashMap::new(),
            client_rt,
        }
    }

    pub fn with_num(conf: &ClusterConf, master_num: u16, worker_num: u16) -> Self {
        let master_conf = Self::default_master_conf(conf, master_num);
        let worker_conf = Self::default_worker_conf(&master_conf[0], worker_num);
        Self::new(master_conf, worker_conf)
    }

    pub fn start_master(&self) {
        for (index, conf) in self.master_conf.iter().enumerate() {
            let master = Master::new(conf.clone()).unwrap();
            self.master_fs.insert(index, master.get_fs());
            thread::spawn(move || master.block_on_start());
        }
    }

    pub fn start_worker(&self) {
        for conf in &self.worker_conf {
            let worker = Worker::new(conf.clone()).unwrap();
            let store = worker.get_block_store();
            self.worker_store.insert(store.worker_id(), store);
            thread::spawn(move || worker.block_on_start());
        }
    }

    pub fn start_cluster(&self) {
        self.start_master();
        self.start_worker();

        self.client_rt.block_on(self.wait_ready()).unwrap();
    }

    async fn wait_ready(&self) -> FsResult<()> {
        let fs = self.new_fs();
        let wait_time = LocalTime::mills() + 20 * 10000;
        while LocalTime::mills() <= wait_time {
            let info = fs.get_master_info().await?;
            if info.live_workers.len() == self.worker_conf.len() {
                return Ok(());
            } else {
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }

        err_box!("The cluster is not ready")
    }

    pub async fn master_client(&self) -> CommonResult<RpcClient> {
        let conf = self.master_conf().client_rpc_conf();
        let addr = self.master_conf().master_addr();
        let client = RpcClient::with_raw(&addr, &conf).await?;
        Ok(client)
    }

    pub fn master_conf(&self) -> &ClusterConf {
        &self.cluster_conf
    }

    pub fn new_fs(&self) -> CurvineFileSystem {
        CurvineFileSystem::with_rt(self.cluster_conf.clone(), self.client_rt.clone()).unwrap()
    }

    pub fn clone_client_rt(&self) -> Arc<Runtime> {
        self.client_rt.clone()
    }

    pub fn get_active_master_fs(&self) -> MasterFilesystem {
        self.master_fs
            .iter()
            .find(|x| x.master_monitor.is_active())
            .map(|x| x.clone())
            .unwrap()
    }

    // Create the default master node configuration.
    // Multiple masters can be created
    fn default_master_conf(conf: &ClusterConf, num: u16) -> Vec<ClusterConf> {
        let mut list = vec![];
        let mut journal_addrs = vec![];
        let mut master_addrs = vec![];
        for index in 0..num {
            let mut master = conf.clone();
            master.master.rpc_port = NetUtils::get_available_port();
            master.journal.rpc_port = NetUtils::get_available_port();
            master.master.web_port = NetUtils::get_available_port();
            master.master.meta_dir = format!("{}/{}", master.master.meta_dir, index);
            master.journal.journal_dir = format!("{}/{}", master.journal.journal_dir, index);

            journal_addrs.push(RaftPeer::new(
                (index + 1) as NodeId,
                &master.journal.hostname,
                master.journal.rpc_port,
            ));

            master_addrs.push(InetAddr::new(
                &master.master.hostname,
                master.master.rpc_port,
            ));

            list.push(master);
        }

        for item in list.iter_mut() {
            item.journal.journal_addrs = journal_addrs.clone();
            item.client.master_addrs = master_addrs.clone();
        }

        list
    }

    pub fn default_worker_conf(conf: &ClusterConf, num: u16) -> Vec<ClusterConf> {
        let mut worker_conf = vec![];

        for index in 0..num {
            let mut worker = conf.clone();
            worker.worker.rpc_port = NetUtils::get_available_port();
            worker.worker.web_port = NetUtils::get_available_port();

            let mut dirs = vec![];
            for item in worker.worker.data_dir {
                dirs.push(format!("{}/{}", item, index));
            }
            worker.worker.data_dir = dirs;
            worker_conf.push(worker)
        }

        worker_conf
    }
}

impl Default for MiniCluster {
    fn default() -> Self {
        Self::with_num(&ClusterConf::default(), 1, 1)
    }
}
