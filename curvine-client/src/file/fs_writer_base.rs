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

use crate::block::BlockWriter;
use crate::file::{FsClient, FsContext};
use curvine_common::fs::Path;
use curvine_common::state::{FileStatus, LocatedBlock};
use curvine_common::FsResult;
use orpc::runtime::{RpcRuntime, Runtime};
use orpc::sys::DataSlice;
use orpc::try_option_mut;
use std::sync::Arc;

pub struct FsWriterBase {
    fs_context: Arc<FsContext>,
    fs_client: FsClient,
    path: Path,
    pos: i64,
    status: FileStatus,
    last_block: Option<LocatedBlock>,
    cur_writer: Option<BlockWriter>,
}

impl FsWriterBase {
    pub fn new(
        fs_context: Arc<FsContext>,
        path: Path,
        status: FileStatus,
        last_block: Option<LocatedBlock>,
    ) -> Self {
        let fs_client = FsClient::new(fs_context.clone());

        Self {
            fs_context,
            fs_client,
            pos: status.len,
            path,
            status,
            last_block,
            cur_writer: None,
        }
    }

    pub fn pos(&self) -> i64 {
        self.pos
    }

    pub fn status(&self) -> &FileStatus {
        &self.status
    }

    pub fn path_str(&self) -> &str {
        self.path.path()
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn fs_context(&self) -> &FsContext {
        &self.fs_context
    }

    pub async fn write(&mut self, mut chunk: DataSlice) -> FsResult<()> {
        if chunk.is_empty() {
            return Ok(());
        }

        let mut remaining = chunk.len();
        while remaining > 0 {
            let cur_writer = self.get_writer().await?;
            let write_len = remaining.min(cur_writer.remaining() as usize);
            // Write data request.
            cur_writer.write(chunk.split_to(write_len)).await?;

            remaining -= write_len;
            self.pos += write_len as i64;
        }

        Ok(())
    }

    /// Block write.
    /// Explain why there is a separate blocking_write instead of rt.block_on(self.write)
    /// We hope to reduce thread switching for writing local files, and the logic of network writing and rt.block_on(self.write) is consistent.
    /// Local write will directly write to the file, without any thread switching.
    pub fn blocking_write(&mut self, rt: &Runtime, mut chunk: DataSlice) -> FsResult<()> {
        if chunk.is_empty() {
            return Ok(());
        }

        let mut remaining = chunk.len();
        while remaining > 0 {
            let cur_writer = rt.block_on(self.get_writer())?;
            let write_len = remaining.min(cur_writer.remaining() as usize);

            // Write data request.
            cur_writer.blocking_write(rt, chunk.split_to(write_len))?;

            remaining -= write_len;
            self.pos += write_len as i64;
        }

        Ok(())
    }

    pub async fn flush(&mut self) -> FsResult<()> {
        // Just flush the block writer in the current write state.
        match &mut self.cur_writer {
            None => Ok(()),
            Some(writer) => writer.flush().await,
        }
    }

    // Write is completed, perform the following operations
    // 1. Submit the last block.
    pub async fn complete(&mut self) -> FsResult<()> {
        let last_block = match self.cur_writer.take() {
            None => None,
            Some(mut writer) => {
                writer.complete().await?;
                Some(writer.to_commit_block())
            }
        };

        self.fs_client
            .complete_file(&self.path, self.pos, last_block)
            .await?;
        Ok(())
    }

    async fn get_writer(&mut self) -> FsResult<&mut BlockWriter> {
        match &mut self.cur_writer {
            Some(v) if v.has_remaining() => (),

            _ => {
                let commit_block = if let Some(mut writer) = self.cur_writer.take() {
                    writer.complete().await?;
                    Some(writer.to_commit_block())
                } else {
                    None
                };

                // new block
                let lb = if let Some(lb) = self.last_block.take() {
                    lb
                } else {
                    self.fs_client
                        .add_block(&self.path, commit_block, &self.fs_context.client_addr)
                        .await?
                };

                let writer = BlockWriter::new(self.fs_context.clone(), lb).await?;
                let _ = self.cur_writer.insert(writer);
            }
        }

        Ok(try_option_mut!(self.cur_writer))
    }
}
