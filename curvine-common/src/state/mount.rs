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

use std::collections::HashMap;

#[derive(Clone, Debug, Default)]
pub struct MountInfo {
    pub mount_id: u32,
    pub curvine_path: String,
    pub ufs_path: String,
    pub properties: HashMap<String, String>,
    pub auto_cache: bool,
    pub cache_ttl_secs: Option<u64>,
    pub consistency_strategy: ConsistencyStrategy,
}

impl MountInfo {
    pub fn get_ttl(&self) -> Option<String> {
        self.cache_ttl_secs.map(|ttl| format!("{}s", ttl))
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum ConsistencyStrategy {
    #[default]
    None,
    Always,
    Period(u64),
}
