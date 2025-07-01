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

use crate::conf::JournalConf;
use crate::proto::raft::*;
use crate::raft::raft_error::RaftError;
use crate::raft::{LibRaftMessage, NodeId, RaftCode, RaftGroup, RaftResult};
use crate::utils::SerdeUtils;
use log::debug;
use orpc::client::{ClientConf, GroupFactory, SyncClient};
use orpc::io::net::{InetAddr, NodeAddr};
use orpc::io::retry::TimeBondedRetryBuilder;
use orpc::message::{BoxMessage, Builder, RefMessage};
use orpc::runtime::{RpcRuntime, Runtime};
use orpc::CommonResult;
use prost::Message as PMessage;
use raft::eraftpb::{ConfChange, ConfChangeType};
use std::mem;
use std::sync::Arc;
use std::time::Duration;

/// Raft message processing client.
/// Key points of core functions:
/// 1. Send a message.
/// 2. Select leader and switch leader.
/// 3. Automatically retry the exception and try to switch the leader or re-establish the connection.
#[derive(Clone)]
pub struct RaftClient {
    rt: Arc<Runtime>,
    factory: Arc<GroupFactory>,
    timeout: Duration,
    retry_builder: TimeBondedRetryBuilder,
}

impl RaftClient {
    pub fn new(rt: Arc<Runtime>, group: &RaftGroup, conf: ClientConf) -> Self {
        let timeout = Duration::from_millis(conf.io_timeout_ms);
        let retry_builder = TimeBondedRetryBuilder::from_conf(&conf);

        let factory = GroupFactory::with_rt(conf, rt.clone());

        for node in group.peers.values() {
            let node_addr = NodeAddr::new(node.id, &node.hostname, node.port);

            factory.add_node(node_addr).unwrap();
        }

        Self {
            rt,
            factory: Arc::new(factory),
            timeout,
            retry_builder,
        }
    }

    pub fn from_conf(rt: Arc<Runtime>, conf: &JournalConf) -> Self {
        let group = RaftGroup::from_conf(conf);
        Self::new(rt, &group, conf.new_client_conf())
    }

    pub fn add_node(&self, id: u64, addr: &InetAddr) -> RaftResult<()> {
        let node = NodeAddr::new(id, &addr.hostname, addr.port);

        self.factory.add_node(node)?;
        Ok(())
    }

    // Cluster configuration modification, usually used to join the cluster with new nodes.
    pub async fn conf_change(&self, req: ConfChange) -> RaftResult<()> {
        let _: ConfChangeResponse = self.leader_rpc(RaftCode::ConfChange, &req).await?;
        Ok(())
    }

    // Send raft internal message.
    pub async fn send_raft(&self, req: LibRaftMessage) -> CommonResult<()> {
        let _: RaftResponse = self.leader_rpc(RaftCode::Raft, &req).await?;
        Ok(())
    }

    // Send application layer messages.
    pub async fn send_propose(&self, data: Vec<u8>) -> RaftResult<()> {
        let req = ProposeRequest { data };
        let _: ProposeResponse = self.leader_rpc(RaftCode::Propose, &req).await?;
        Ok(())
    }

    pub fn block_on_send_propose(&self, data: Vec<u8>) -> RaftResult<()> {
        self.rt.block_on(self.send_propose(data))
    }

    // Join the cluster.
    pub async fn join_cluster(&self, id: NodeId, addr: &InetAddr) -> RaftResult<()> {
        let change = ConfChange {
            change_type: ConfChangeType::AddNode.into(),
            node_id: id,
            context: SerdeUtils::serialize(addr)?,
            id,
        };
        let header = ConfChangeRequest { change };
        let _: ConfChangeResponse = self.leader_rpc(RaftCode::ConfChange, &header).await?;
        Ok(())
    }

    pub async fn ping(&self, id: NodeId) -> RaftResult<PingResponse> {
        let header = PingRequest::default();
        let req = Builder::new_rpc(RaftCode::Ping)
            .proto_header(header)
            .build()
            .into_arc();

        let rep_header: PingResponse = self.retry_rpc(id, req).await?;
        Ok(rep_header)
    }

    /// Initiate a rpc request to the specified node
    pub async fn timeout_rpc<R>(
        &self,
        id: u64,
        msg: impl RefMessage,
    ) -> Result<R, (bool, RaftError)>
    where
        R: PMessage + Default,
    {
        let client = match self.factory.get_client(id).await {
            Err(e) => return Err((true, e.into())),
            Ok(v) => v,
        };

        // Send a request
        let msg = match client.timeout_rpc(self.timeout, msg).await {
            Ok(v) => match v.check_error_ext::<RaftError>() {
                Err(e) => return Err((e.retry_leader(), e)),
                Ok(_) => v,
            },

            Err(e) => {
                client.set_closed();
                self.factory.remove_client(id);
                return Err((true, e.into()));
            }
        };

        match msg.parse_header::<R>() {
            Err(e) => Err((false, e.into())),
            Ok(v) => Ok(v),
        }
    }

    // Send a request to the leader.
    pub async fn leader_rpc<T, R>(&self, code: RaftCode, header: &T) -> RaftResult<R>
    where
        T: PMessage + Default,
        R: PMessage + Default,
    {
        let msg = Builder::new_rpc(code)
            .proto_header_ref(header)
            .build()
            .into_arc();

        let mut last_error: Option<RaftError> = None;

        // Send a request to the current leader node.
        if let Some(id) = self.factory.leader_id() {
            match self.timeout_rpc::<R>(id, msg.clone()).await {
                Ok(v) => return Ok(v),

                Err((retry, e)) => {
                    if !retry {
                        return Err(e);
                    } else {
                        debug!(
                            "Rpc({}) call failed to leader {}: {}",
                            msg.req_id(),
                            self.factory.get_addr_string(id),
                            e
                        );
                        let _ = mem::replace(&mut last_error, Some(e));
                    }
                }
            }
        }

        // Poll to send requests to all nodes until timeout.
        let mut policy = self.retry_builder.build();
        let node_list = self.factory.node_list(false);
        let mut index = 0;
        while policy.attempt().await {
            let id = node_list[index];
            index = (index + 1) % node_list.len();

            match self.timeout_rpc::<R>(id, msg.clone()).await {
                Ok(v) => {
                    self.factory.change_leader(id);
                    return Ok(v);
                }

                Err((retry, e)) => {
                    if !retry {
                        self.factory.change_leader(id);
                        return Err(e);
                    } else {
                        debug!(
                            "Rpc({}) call failed to leader {}: {}",
                            msg.req_id(),
                            self.factory.get_addr_string(id),
                            e
                        );
                        let _ = mem::replace(&mut last_error, Some(e));
                    }
                }
            }
        }

        let msg = format!("Failed to determine after {} attempts", policy.count());
        Err(RaftError::with_opt_msg(last_error, msg))
    }

    // Send a retry request to the specified node.
    pub async fn retry_rpc<R>(&self, id: u64, msg: BoxMessage) -> RaftResult<R>
    where
        R: PMessage + Default,
    {
        let mut policy = self.retry_builder.build();
        let mut last_error: Option<RaftError> = None;
        while policy.attempt().await {
            match self.timeout_rpc::<R>(id, msg.clone()).await {
                Ok(v) => return Ok(v),

                Err((retry, e)) => {
                    if !retry {
                        return Err(e);
                    } else {
                        debug!(
                            "Rpc({}) call failed to leader {}: {}",
                            msg.req_id(),
                            self.factory.get_addr_string(id),
                            e
                        );
                        let _ = mem::replace(&mut last_error, Some(e));
                    }
                }
            }
        }

        let msg = format!(
            "Failed to determine address for {} after {} attempts",
            self.factory.get_addr_string(id),
            policy.count()
        );
        Err(RaftError::with_opt_msg(last_error, msg))
    }

    pub fn create_snapshot_client(&self, id: u64) -> RaftResult<SyncClient> {
        let client = self.factory.create_sync_with_id(id)?;
        Ok(client)
    }
}
