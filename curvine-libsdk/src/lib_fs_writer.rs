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

use curvine_client::unified::UnifiedWriter;
use curvine_common::fs::{Path, Writer};
use curvine_common::state::FileStatus;
use curvine_common::FsResult;
use orpc::runtime::{RpcRuntime, Runtime};
use orpc::sys::DataSlice;
use std::sync::Arc;

pub struct LibFsWriter {
    pub(crate) rt: Arc<Runtime>,
    pub(crate) inner: UnifiedWriter,
}

impl LibFsWriter {
    pub fn new(rt: Arc<Runtime>, writer: UnifiedWriter) -> Self {
        Self { rt, inner: writer }
    }

    pub fn write(&mut self, buf: DataSlice) -> FsResult<()> {
        self.inner.blocking_write(&self.rt, buf)
    }

    pub fn flush(&mut self) -> FsResult<()> {
        self.rt.block_on(self.inner.flush())
    }

    pub fn complete(&mut self) -> FsResult<()> {
        self.rt.block_on(self.inner.complete())
    }

    pub fn pos(&self) -> i64 {
        self.inner.pos()
    }

    pub fn status(&self) -> &FileStatus {
        self.inner.status()
    }

    pub fn path(&self) -> &Path {
        self.inner.path()
    }

    pub fn block_size(&self) -> i64 {
        self.inner.status().block_size
    }
}
