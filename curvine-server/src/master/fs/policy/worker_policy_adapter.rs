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

use crate::master::fs::context::ValidateAddBlock;
use crate::master::fs::policy::WorkerPolicyAdapter::{LoadBased, Local, Random, Robin};
use crate::master::fs::policy::{
    LoadBasedWorkerPolicy, LocalWorkerPolicy, RandomWorkerPolicy, RobinWorkerPolicy, WorkerPolicy,
};
use curvine_common::conf::ClusterConf;
use curvine_common::state::{WorkerAddress, WorkerInfo};
use indexmap::IndexMap;
use orpc::{err_box, CommonResult};
use std::collections::HashSet;

pub enum WorkerPolicyAdapter {
    Robin(RobinWorkerPolicy),
    Local(LocalWorkerPolicy),
    Random(RandomWorkerPolicy),
    LoadBased(LoadBasedWorkerPolicy),
}

impl WorkerPolicyAdapter {
    pub const ROBIN: &'static str = "robin";
    pub const LOCAL: &'static str = "local";
    pub const RANDOM: &'static str = "random";
    pub const LOAD_BASED: &'static str = "load_based";

    pub fn from_conf(conf: &ClusterConf) -> CommonResult<Self> {
        let policy_str = conf.master.worker_policy.as_str();
        let policy = match policy_str {
            Self::LOCAL => Local(LocalWorkerPolicy::new()),
            Self::ROBIN => Robin(RobinWorkerPolicy::new()),
            Self::RANDOM => Random(RandomWorkerPolicy::new()),
            Self::LOAD_BASED => LoadBased(LoadBasedWorkerPolicy::new()),
            _ => return err_box!("Unsupported worker policy {}", policy_str),
        };
        Ok(policy)
    }

    pub fn choose(
        &self,
        workers: &IndexMap<u32, WorkerInfo>,
        block: &ValidateAddBlock,
        exclude_workers: Option<HashSet<u32>>,
    ) -> CommonResult<Vec<WorkerAddress>> {
        match self {
            Robin(ref f) => f.choose(workers, block, exclude_workers),
            Local(ref f) => f.choose(workers, block, exclude_workers),
            Random(ref f) => f.choose(workers, block, exclude_workers),
            LoadBased(ref f) => f.choose(workers, block, exclude_workers),
        }
    }

    pub fn choose_workers(
        &self,
        workers: &IndexMap<u32, WorkerInfo>,
        count: usize,
        exclude_workers: Option<HashSet<u32>>,
    ) -> CommonResult<Vec<WorkerAddress>> {
        match self {
            Robin(ref f) => f.choose_workers(workers, Some(count), exclude_workers),
            Local(ref f) => f.choose_workers(workers, Some(count), exclude_workers),
            Random(ref f) => f.choose_workers(workers, Some(count), exclude_workers),
            LoadBased(ref f) => f.choose_workers(workers, Some(count), exclude_workers),
        }
    }
}
