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
use crate::master::fs::policy::WorkerPolicy;
use curvine_common::state::{WorkerAddress, WorkerInfo};
use indexmap::IndexMap;
use orpc::sync::AtomicLen;
use orpc::{err_box, try_option, CommonResult};
use std::collections::HashSet;

// Poll selector.
pub struct RobinWorkerPolicy {
    index: AtomicLen,
}

impl Default for RobinWorkerPolicy {
    fn default() -> Self {
        Self::new()
    }
}

impl RobinWorkerPolicy {
    pub fn new() -> Self {
        Self {
            index: AtomicLen::new(0),
        }
    }
}

impl WorkerPolicy for RobinWorkerPolicy {
    fn choose(
        &self,
        workers: &IndexMap<u32, WorkerInfo>,
        block: &ValidateAddBlock,
        exclude_workers: Option<HashSet<u32>>,
    ) -> CommonResult<Vec<WorkerAddress>> {
        if workers.is_empty() {
            return err_box!("No workers available");
        }
        if block.replicas < 1 {
            return err_box!("The number of replicas cannot be 0");
        }

        let start_index = self.index.get();
        let mut index = start_index;
        let mut res = vec![];
        let mut excluded = exclude_workers.unwrap_or_default();

        while res.len() < block.replicas as usize {
            let (id, worker) = try_option!(workers.get_index(index));

            if !excluded.contains(id) && worker.available > block.block_size && worker.is_live() {
                excluded.insert(*id);
                res.push(worker.address.clone())
            }

            index = (index + 1) % workers.len();
            if index == start_index {
                break;
            }
        }

        self.index.set(index);
        Ok(res)
    }

    fn choose_workers(
        &self,
        workers: &IndexMap<u32, WorkerInfo>,
        count: Option<usize>,
        exclude_workers: Option<HashSet<u32>>,
    ) -> CommonResult<Vec<WorkerAddress>> {
        if workers.is_empty() {
            return err_box!("No workers available");
        }
        let count = count.unwrap_or(1);
        if count < 1 {
            return err_box!("The number of workers to choose cannot be 0");
        }

        let start_index = self.index.get();
        let mut index = start_index;
        let mut res = vec![];
        let exclude_workers = exclude_workers.unwrap_or_default();

        while res.len() < count {
            let (id, worker) = try_option!(workers.get_index(index));

            if !exclude_workers.contains(id) {
                res.push(worker.address.clone())
            }

            index = (index + 1) % workers.len();
            if index == start_index {
                break;
            }
        }

        self.index.set(index);

        if res.is_empty() {
            return err_box!("No available workers found");
        }

        Ok(res)
    }
}
