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

use curvine_common::conf::ClientConf;
use curvine_common::state::*;
use orpc::common::ByteUnit;
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct CreateFileOpts {
    pub overwrite: bool,
    pub create_parent: bool,
    pub replicas: i32,
    pub block_size: i64,
    pub file_type: FileType,
    pub x_attr: HashMap<String, Vec<u8>>,
    pub storage_policy: StoragePolicy,
}

#[derive(Debug, Clone)]
pub struct CreateFileOptsBuilder {
    pub overwrite: bool,
    create_parent: bool,
    replicas: i32,
    block_size: i64,
    file_type: FileType,
    x_attr: HashMap<String, Vec<u8>>,
    storage_policy: StoragePolicy,
}

impl Default for CreateFileOptsBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl CreateFileOptsBuilder {
    pub fn new() -> Self {
        Self {
            overwrite: false,
            create_parent: false,
            replicas: 1,
            block_size: (64 * ByteUnit::MB) as i64,
            file_type: FileType::File,
            x_attr: HashMap::new(),
            storage_policy: StoragePolicy::default(),
        }
    }

    pub fn with_conf(conf: &ClientConf) -> Self {
        Self {
            overwrite: false,
            create_parent: false,
            replicas: conf.replicas,
            block_size: conf.block_size,
            file_type: FileType::File,
            x_attr: Default::default(),
            storage_policy: StoragePolicy {
                storage_type: StorageType::from_str_name(&conf.storage_type)
                    .unwrap_or(StorageType::Disk),
                ..Default::default()
            },
        }
    }

    pub fn overwrite(mut self, overwrite: bool) -> Self {
        self.overwrite = overwrite;
        self
    }

    pub fn create_parent(mut self, flag: bool) -> Self {
        self.create_parent = flag;
        self
    }

    pub fn replicas(mut self, replicas: i32) -> Self {
        self.replicas = replicas;
        self
    }

    pub fn block_size(mut self, block_size: i64) -> Self {
        self.block_size = block_size;
        self
    }

    pub fn file_type(mut self, file_type: FileType) -> Self {
        self.file_type = file_type;
        self
    }

    pub fn add_x_attr(mut self, key: String, value: Vec<u8>) -> Self {
        self.x_attr.insert(key, value);
        self
    }

    pub fn storage_type(mut self, t: StorageType) -> Self {
        self.storage_policy.storage_type = t;
        self
    }

    pub fn ttl_ms(mut self, ms: i64) -> Self {
        self.storage_policy.ttl_ms = ms;
        self
    }

    pub fn ttl_action(mut self, action: TtlAction) -> Self {
        self.storage_policy.ttl_action = action;
        self
    }

    pub fn ufs_mtime(mut self, mtime: i64) -> Self {
        self.storage_policy.ufs_mtime = mtime;
        self
    }

    pub fn build(self) -> CreateFileOpts {
        CreateFileOpts {
            overwrite: self.overwrite,
            create_parent: self.create_parent,
            replicas: self.replicas,
            block_size: self.block_size,
            file_type: self.file_type,
            x_attr: self.x_attr,
            storage_policy: self.storage_policy,
        }
    }
}
