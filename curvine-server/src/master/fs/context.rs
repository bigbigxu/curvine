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

use crate::master::meta::inode::InodePtr;
use curvine_common::proto::CreateFileRequest;
use curvine_common::state::{FileType, StoragePolicy};
use curvine_common::utils::ProtoUtils;
use orpc::common::ByteUnit;
use std::collections::HashMap;

// adBlock request verification result.
#[derive(Debug, Clone)]
pub struct ValidateAddBlock {
    pub replicas: u16,
    pub block_size: i64,
    pub storage_policy: StoragePolicy,
    pub client_host: String,
    pub file_inode: InodePtr,
}

#[derive(Debug, Clone)]
pub struct CreateFileContext {
    pub path: String,
    pub file_type: FileType,
    pub replicas: u16,
    pub block_size: i64,
    pub x_attr: HashMap<String, Vec<u8>>,
    pub storage_policy: StoragePolicy,

    pub overwrite: bool,
    pub create_parent: bool,
    pub client_name: String,
}

impl CreateFileContext {
    pub fn from_pb(req: CreateFileRequest) -> CreateFileContext {
        CreateFileContext {
            path: req.path,
            file_type: FileType::from(req.file_type),
            replicas: req.replicas as u16,
            block_size: req.block_size,
            x_attr: req.x_attr,
            storage_policy: ProtoUtils::storage_policy_from_pb(req.storage_policy),
            overwrite: req.overwrite,
            create_parent: req.create_parent,
            client_name: req.client_name,
        }
    }

    pub fn with_path<T: AsRef<str>>(path: T, create_parent: bool) -> Self {
        Self {
            path: path.as_ref().to_string(),
            file_type: FileType::Dir,
            replicas: 1,
            block_size: ByteUnit::MB as i64 * 64,
            x_attr: Default::default(),
            storage_policy: Default::default(),
            overwrite: false,
            create_parent,
            client_name: "".to_string(),
        }
    }
}
