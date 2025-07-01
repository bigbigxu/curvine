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

use crate::client::{ClientConf, ClientFactory, RpcClient, SyncClient};
use crate::err_box;
use crate::io::net::{InetAddr, NodeAddr};
use crate::io::IOResult;
use crate::runtime::Runtime;
use dashmap::DashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

// Manage the creation of a group of agents.
// Thread safety
pub struct GroupFactory {
    pub(crate) inner: ClientFactory,
    pub(crate) leader_id: AtomicU64,
    pub(crate) group: DashMap<u64, Arc<NodeAddr>>,
}

impl GroupFactory {
    pub const DEFAULT_LEADER_ID: u64 = 0;

    pub fn with_rt(conf: ClientConf, rt: Arc<Runtime>) -> Self {
        Self {
            inner: ClientFactory::with_rt(conf, rt),
            leader_id: AtomicU64::new(Self::DEFAULT_LEADER_ID),
            group: Default::default(),
        }
    }

    pub fn add_node(&self, peer: NodeAddr) -> IOResult<()> {
        if peer.id == Self::DEFAULT_LEADER_ID {
            err_box!("Node id cannot be {}", Self::DEFAULT_LEADER_ID)
        } else {
            self.group.insert(peer.id, Arc::new(peer));
            Ok(())
        }
    }

    pub fn leader_id(&self) -> Option<u64> {
        let id = self.leader_id.load(Ordering::SeqCst);
        if id == Self::DEFAULT_LEADER_ID {
            None
        } else {
            Some(id)
        }
    }

    pub fn change_leader(&self, id: u64) {
        self.leader_id.store(id, Ordering::SeqCst)
    }

    pub async fn create_client(&self, addr: &InetAddr, buffer: bool) -> IOResult<RpcClient> {
        self.inner.create(addr, buffer).await
    }

    pub async fn create(&self, addr: &InetAddr, buffer: bool) -> IOResult<RpcClient> {
        self.inner.create(addr, buffer).await
    }

    pub fn create_sync(&self, addr: &InetAddr) -> IOResult<SyncClient> {
        self.inner.create_sync(addr)
    }

    pub fn create_sync_with_id(&self, id: u64) -> IOResult<SyncClient> {
        let addr = self.get_addr(id)?;
        self.inner.create_sync(&addr.addr)
    }

    pub async fn create_with_id(&self, id: u64, buffer: bool) -> IOResult<RpcClient> {
        let addr = self.get_addr(id)?;
        self.inner.create(&addr.addr, buffer).await
    }

    pub async fn get_client(&self, id: u64) -> IOResult<RpcClient> {
        let addr = self.get_addr(id)?;
        self.inner.get(&addr.addr).await
    }

    pub fn get_addr(&self, id: u64) -> IOResult<Arc<NodeAddr>> {
        match self.group.get(&id) {
            None => err_box!("Node {} not exist", id),
            Some(v) => Ok(v.clone()),
        }
    }

    /// When obtaining node_list, it means that the leader id access failed and all nodes need to be traversed.
    pub fn node_list(&self, leader_first: bool) -> Vec<u64> {
        let mut vec = vec![];

        let leader_id = self.leader_id.load(Ordering::SeqCst);
        if leader_id != Self::DEFAULT_LEADER_ID && leader_first {
            vec.push(leader_id);
        }

        for item in self.group.iter() {
            if leader_id == item.id {
                continue;
            }
            vec.push(item.id)
        }

        if leader_id != Self::DEFAULT_LEADER_ID && !leader_first {
            vec.push(leader_id);
        }

        vec
    }

    pub fn conf(&self) -> &ClientConf {
        &self.inner.conf
    }

    pub fn get_addr_string(&self, id: u64) -> String {
        self.group
            .get(&id)
            .map(|x| x.addr.to_string())
            .unwrap_or("None".to_string())
    }

    pub fn remove_client(&self, id: u64) {
        match self.group.get(&id) {
            None => (),
            Some(v) => self.inner.remove(&v.addr),
        }
    }

    pub fn clone_runtime(&self) -> Arc<Runtime> {
        self.inner.clone_runtime()
    }
}
