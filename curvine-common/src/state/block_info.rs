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

use crate::proto::BlockWriteRequest;
use crate::state::{FileStatus, FileType, StorageType, WorkerAddress};
use crate::FsResult;
use orpc::common::ByteUnit;
use orpc::{err_box, CommonResult};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

// block location information.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlockLocation {
    pub worker_id: u32,
    pub storage_type: StorageType,
}

impl BlockLocation {
    pub fn new(id: u32, storage_type: StorageType) -> Self {
        Self {
            worker_id: id,
            storage_type,
        }
    }

    pub fn with_id(id: u32) -> Self {
        Self::new(id, StorageType::Disk)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommitBlock {
    pub block_id: i64,
    pub block_len: i64,
    pub locations: Vec<BlockLocation>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ExtendedBlock {
    pub id: i64,
    pub len: i64,
    pub storage_type: StorageType,
    pub file_type: FileType,
}

impl ExtendedBlock {
    pub fn new(id: i64, len: i64, storage_type: StorageType, file_type: FileType) -> Self {
        Self {
            id,
            len,
            storage_type,
            file_type,
        }
    }

    pub fn with_id(id: i64) -> Self {
        Self::new(id, 0, StorageType::Disk, FileType::File)
    }

    pub fn with_size_str(id: i64, size: &str, stg_type: StorageType) -> CommonResult<Self> {
        let bytes = ByteUnit::from_str(size)?.as_byte();
        Ok(Self::new(id, bytes as i64, stg_type, FileType::File))
    }

    pub fn with_mem(id: i64, size: &str) -> CommonResult<Self> {
        Self::with_size_str(id, size, StorageType::Mem)
    }

    pub fn with_ssd(id: i64, size: &str) -> CommonResult<Self> {
        Self::with_size_str(id, size, StorageType::Ssd)
    }

    pub fn from_req(req: &BlockWriteRequest) -> Self {
        Self::new(
            req.id,
            req.len,
            StorageType::from(req.storage_type),
            FileType::from(req.file_type),
        )
    }

    pub fn size_string(&self) -> String {
        ByteUnit::byte_to_string(self.len as u64)
    }

    pub fn is_stream(&self) -> bool {
        self.file_type != FileType::File && self.file_type != FileType::Dir
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct LocatedBlock {
    pub block: ExtendedBlock,
    pub locs: Vec<WorkerAddress>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct FileBlocks {
    pub status: FileStatus,
    pub block_ids: Vec<i64>,
    pub block_locs: HashMap<i64, LocatedBlock>,
}

impl FileBlocks {
    pub fn get_block_id(&self, file_pos: i64) -> FsResult<i64> {
        let index = file_pos / self.status.block_size;
        let block_id = match self.block_ids.get(index as usize) {
            Some(v) => v,
            None => return err_box!("Not found block for pos {}, index {}", file_pos, index),
        };
        Ok(*block_id)
    }

    // According to the file reading location, get the block that needs to be read and the offset in the block.
    pub fn get_read_block(&self, file_pos: i64) -> FsResult<(i64, LocatedBlock)> {
        let block_id = self.get_block_id(file_pos)?;
        let locs = match self.block_locs.get(&block_id) {
            Some(v) => v.clone(),
            None => return err_box!("Not fond block {}", block_id),
        };
        let block_off = file_pos % self.status.block_size;
        Ok((block_off, locs))
    }
}
