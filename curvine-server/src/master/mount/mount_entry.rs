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

use crate::master::mount::ConsistencyStrategy;
use curvine_common::proto::MountOptions;
use curvine_common::proto::MountPointInfo;
use std::collections::HashMap;
use std::fmt;

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
pub struct MountPointEntry {
    pub(crate) id: u32,
    pub(crate) curvine_uri: String,
    pub(crate) ufs_uri: String,
    pub(crate) properties: HashMap<String, String>,
    //cache related
    pub(crate) auto_cache: bool,
    pub(crate) cache_ttl_secs: Option<u64>,
    pub(crate) consistency_strategy: ConsistencyStrategy,
}

impl MountPointEntry {
    pub fn new(id: u32, curvine_uri: String, ufs_uri: String, mnt_opt: MountOptions) -> Self {
        let consistency_conf = mnt_opt.consistency_config.unwrap_or_default();
        let strategy = ConsistencyStrategy::from(consistency_conf);

        MountPointEntry {
            id,
            curvine_uri,
            ufs_uri,
            properties: mnt_opt.properties,
            auto_cache: mnt_opt.auto_cache,
            cache_ttl_secs: mnt_opt.cache_ttl_secs,
            consistency_strategy: strategy,
        }
    }
}

impl fmt::Display for MountPointEntry {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(
            fmt,
            "id: {}, curvine_uri: {}, ufs_uri: {}, properties: {:?}, auto_cache: {}, cache_ttl_secs: {:?}, consistency_strategy: {:?}",
            self.id, self.curvine_uri, self.ufs_uri, self.properties, self.auto_cache, self.cache_ttl_secs, self.consistency_strategy
        )
    }
}

impl From<MountPointEntry> for MountPointInfo {
    fn from(val: MountPointEntry) -> Self {
        MountPointInfo {
            curvine_path: val.curvine_uri,
            ufs_path: val.ufs_uri,
            mount_id: val.id,
            properties: val.properties,
            auto_cache: val.auto_cache,
            cache_ttl_secs: val.cache_ttl_secs,
            consistency_config: ConsistencyStrategy::into(val.consistency_strategy),
        }
    }
}
