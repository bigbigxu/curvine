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

use crate::block::block_writer::WriterAdapter::{Local, Remote};
use crate::block::{BlockWriterLocal, BlockWriterRemote};
use crate::file::FsContext;
use curvine_common::state::{BlockLocation, CommitBlock, LocatedBlock, WorkerAddress};
use curvine_common::FsResult;
use orpc::err_box;
use orpc::runtime::{RpcRuntime, Runtime};
use orpc::sys::DataSlice;
use std::sync::Arc;

enum WriterAdapter {
    Local(BlockWriterLocal),
    Remote(BlockWriterRemote),
}

impl WriterAdapter {
    fn worker_address(&self) -> &WorkerAddress {
        match self {
            Local(f) => f.worker_address(),
            Remote(f) => f.worker_address(),
        }
    }

    async fn write(&mut self, buf: DataSlice) -> FsResult<()> {
        match self {
            Local(f) => f.write(buf).await,
            Remote(f) => f.write(buf).await,
        }
    }

    fn blocking_write(&mut self, rt: &Runtime, buf: DataSlice) -> FsResult<()> {
        match self {
            Local(f) => f.blocking_write(buf.clone()),
            Remote(f) => rt.block_on(f.write(buf.clone())),
        }
    }

    async fn flush(&mut self) -> FsResult<()> {
        match self {
            Local(f) => f.flush().await,
            Remote(f) => f.flush().await,
        }
    }

    async fn complete(&mut self) -> FsResult<()> {
        match self {
            Local(f) => f.complete().await,
            Remote(f) => f.complete().await,
        }
    }

    async fn cancel(&mut self) -> FsResult<()> {
        match self {
            Local(f) => f.cancel().await,
            Remote(f) => f.cancel().await,
        }
    }

    fn remaining(&self) -> i64 {
        match self {
            Local(f) => f.remaining(),
            Remote(f) => f.remaining(),
        }
    }

    fn pos(&self) -> i64 {
        match self {
            Local(f) => f.pos(),
            Remote(f) => f.pos(),
        }
    }
}

pub struct BlockWriter {
    inners: Vec<WriterAdapter>,
    locate: LocatedBlock,
    fs_context: Arc<FsContext>,
}

impl BlockWriter {
    pub async fn new(fs_context: Arc<FsContext>, locate: LocatedBlock) -> FsResult<Self> {
        if locate.locs.is_empty() {
            return err_box!("There is no available worker");
        }

        let conf = &fs_context.conf.client;
        let mut inners = Vec::with_capacity(locate.locs.len());
        for addr in &locate.locs {
            let short_circuit = conf.short_circuit && fs_context.is_local_worker(addr);

            let adapter = if short_circuit {
                let writer =
                    BlockWriterLocal::new(fs_context.clone(), locate.block.clone(), addr.clone())
                        .await?;
                Local(writer)
            } else {
                let writer =
                    BlockWriterRemote::new(&fs_context, locate.block.clone(), addr.clone()).await?;
                Remote(writer)
            };
            inners.push(adapter);
        }

        let writer = Self {
            inners,
            locate,
            fs_context,
        };
        Ok(writer)
    }

    pub async fn write(&mut self, buf: DataSlice) -> FsResult<()> {
        for writer in &mut self.inners {
            if let Err(e) = writer.write(buf.clone()).await {
                self.fs_context.add_failed_worker(writer.worker_address());
                return Err(e);
            }
        }
        Ok(())
    }

    pub fn blocking_write(&mut self, rt: &Runtime, buf: DataSlice) -> FsResult<()> {
        for writer in &mut self.inners {
            if let Err(e) = writer.blocking_write(rt, buf.clone()) {
                self.fs_context.add_failed_worker(writer.worker_address());
                return Err(e);
            }
        }
        Ok(())
    }

    pub async fn flush(&mut self) -> FsResult<()> {
        for writer in &mut self.inners {
            if let Err(e) = writer.flush().await {
                self.fs_context.add_failed_worker(writer.worker_address());
                return Err(e);
            }
        }
        Ok(())
    }

    pub async fn complete(&mut self) -> FsResult<CommitBlock> {
        for writer in &mut self.inners {
            if let Err(e) = writer.complete().await {
                self.fs_context.add_failed_worker(writer.worker_address());
                return Err(e);
            }
        }
        Ok(self.to_commit_block())
    }

    pub async fn cancel(&mut self) -> FsResult<()> {
        for writer in &mut self.inners {
            if let Err(e) = writer.cancel().await {
                self.fs_context.add_failed_worker(writer.worker_address());
                return Err(e);
            }
        }
        Ok(())
    }

    pub fn remaining(&self) -> i64 {
        self.inners[0].remaining()
    }

    pub fn has_remaining(&self) -> bool {
        self.inners[0].remaining() > 0
    }

    pub fn pos(&self) -> i64 {
        self.inners[0].pos()
    }

    pub fn to_commit_block(&self) -> CommitBlock {
        let locs = self
            .locate
            .locs
            .iter()
            .map(|x| BlockLocation {
                worker_id: x.worker_id,
                storage_type: self.locate.block.storage_type,
            })
            .collect();

        CommitBlock {
            block_id: self.locate.block.id,
            block_len: self.pos(),
            locations: locs,
        }
    }
}
