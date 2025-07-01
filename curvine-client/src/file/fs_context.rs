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

use crate::block::BlockClient;
use crate::ClientMetrics;
use curvine_common::conf::ClusterConf;
use curvine_common::state::{ClientAddress, WorkerAddress};
use curvine_common::FsResult;
use fxhash::FxHasher;
use moka::policy::EvictionPolicy;
use moka::sync::{Cache, CacheBuilder};
use once_cell::sync::OnceCell;
use orpc::client::{ClientConf, GroupFactory, RpcClient};
use orpc::common::{Metrics, Utils};
use orpc::io::net::{InetAddr, NetUtils};
use orpc::io::IOResult;
use orpc::runtime::Runtime;
use orpc::sys::CacheManager;
use std::hash::BuildHasherDefault;
use std::sync::Arc;

static CLIENT_METRICS: OnceCell<ClientMetrics> = OnceCell::new();

// The core feature of the file system is thread-safe, which can be shared between multiple threads through Arc.
// 1. The cluster configuration file is saved.
// 2. Create client.
// 3. Perceive master switching.
pub struct FsContext {
    pub(crate) conf: ClusterConf,
    pub(crate) factory: GroupFactory,
    pub(crate) client_addr: ClientAddress,
    pub(crate) os_cache: CacheManager,
    pub(crate) failed_workers: Cache<u32, WorkerAddress, BuildHasherDefault<FxHasher>>,
}

impl FsContext {
    pub fn new(conf: ClusterConf) -> FsResult<Self> {
        let rt = Arc::new(conf.client_rpc_conf().create_runtime());
        Self::with_rt(conf, rt)
    }

    pub fn with_rt(conf: ClusterConf, rt: Arc<Runtime>) -> FsResult<Self> {
        let hostname = conf.client.hostname.to_owned();
        let ip = NetUtils::local_ip(&hostname);
        let client_addr = ClientAddress {
            client_name: Utils::uuid(),
            hostname,
            ip_addr: ip,
            port: 0,
        };

        // Initialize metrics
        Metrics::init();
        CLIENT_METRICS.get_or_init(|| ClientMetrics::new().unwrap());

        let factory = GroupFactory::with_rt(conf.client_rpc_conf(), rt.clone());
        for node in conf.master_nodes() {
            factory.add_node(node)?;
        }

        let os_cache = CacheManager::new(
            conf.client.enable_read_ahead,
            conf.client.read_ahead_len,
            conf.client.drop_cache_len,
        );

        let exclude_workers = CacheBuilder::default()
            .time_to_live(conf.client.failed_worker_ttl)
            .eviction_policy(EvictionPolicy::lru())
            .build_with_hasher(BuildHasherDefault::<FxHasher>::default());

        let context = Self {
            conf,
            factory,
            client_addr,
            os_cache,
            failed_workers: exclude_workers,
        };
        Ok(context)
    }

    pub async fn block_client(&self, addr: &WorkerAddress) -> IOResult<BlockClient> {
        let addr = InetAddr::new(addr.hostname.clone(), addr.rpc_port as u16);
        let client = self.factory.create_client(&addr, false).await?;
        Ok(BlockClient::new(client, self))
    }

    pub fn clone_runtime(&self) -> Arc<Runtime> {
        self.factory.clone_runtime()
    }

    pub fn is_local_worker(&self, addr: &WorkerAddress) -> bool {
        addr.is_local(&self.client_addr.hostname)
    }

    pub fn read_chunk_size(&self) -> usize {
        self.conf.client.read_chunk_size
    }

    pub fn read_chunk_num(&self) -> usize {
        self.conf.client.read_chunk_num
    }

    pub fn read_parallel(&self) -> i64 {
        self.conf.client.read_parallel
    }

    pub fn read_since_size(&self) -> i64 {
        self.conf.client.read_slice_size
    }

    pub fn write_chunk_size(&self) -> usize {
        self.conf.client.write_chunk_size
    }

    pub fn write_chunk_num(&self) -> usize {
        self.conf.client.write_chunk_num
    }

    pub fn block_size(&self) -> i64 {
        self.conf.client.block_size
    }

    pub fn clone_client_name(&self) -> String {
        self.client_addr.client_name.clone()
    }

    pub async fn get_client(&self, id: u64) -> IOResult<RpcClient> {
        self.factory.get_client(id).await
    }

    pub fn get_addr_string(&self, id: u64) -> String {
        self.factory.get_addr_string(id)
    }

    pub fn remove_client(&self, id: u64) {
        self.factory.remove_client(id);
    }

    pub fn leader_id(&self) -> Option<u64> {
        self.factory.leader_id()
    }

    pub fn change_leader(&self, id: u64) {
        self.factory.change_leader(id);
    }

    pub fn node_list(&self, leader_first: bool) -> Vec<u64> {
        self.factory.node_list(leader_first)
    }

    pub fn cluster_conf(&self) -> ClusterConf {
        self.conf.clone()
    }

    pub fn rpc_conf(&self) -> &ClientConf {
        self.factory.conf()
    }

    pub fn clone_os_cache(&self) -> CacheManager {
        self.os_cache.clone()
    }

    pub fn get_metrics<'a>() -> &'a ClientMetrics {
        CLIENT_METRICS.get().expect("client get metrics error!")
    }

    // Exclude a worker
    pub fn add_failed_worker(&self, addr: &WorkerAddress) {
        self.failed_workers.insert(addr.worker_id, addr.clone())
    }

    pub fn is_failed_worker(&self, addr: &WorkerAddress) -> bool {
        self.failed_workers.contains_key(&addr.worker_id)
    }

    pub fn get_failed_workers(&self) -> Vec<WorkerAddress> {
        let mut res = vec![];
        for item in self.failed_workers.iter() {
            res.push(item.1.clone());
        }

        res
    }
}
