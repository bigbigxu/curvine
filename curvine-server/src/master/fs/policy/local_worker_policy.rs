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
use crate::master::fs::policy::{RobinWorkerPolicy, WorkerPolicy};
use curvine_common::state::{WorkerAddress, WorkerInfo};
use indexmap::IndexMap;
use orpc::{err_box, CommonResult};
use std::collections::HashSet;

/// Local workers are preferred, and polling policies are used if there are no local workers
pub struct LocalWorkerPolicy {
    inner: RobinWorkerPolicy,
}

impl Default for LocalWorkerPolicy {
    fn default() -> Self {
        Self::new()
    }
}

impl LocalWorkerPolicy {
    pub fn new() -> Self {
        Self {
            inner: RobinWorkerPolicy::new(),
        }
    }
}

impl WorkerPolicy for LocalWorkerPolicy {
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

        let mut res = vec![];
        let mut excluded = exclude_workers.unwrap_or_default();

        // step1: Detect whether the local worker exists
        for (id, worker) in workers {
            if !excluded.contains(id)
                && worker.address.is_local(&block.client_host)
                && worker.available > block.block_size
                && worker.is_live()
            {
                res.push(worker.address.clone());
                excluded.insert(*id);
                break;
            }
        }

        // @todo Follow-up optimization.
        if res.len() < block.replicas as usize {
            let remote_res = self.inner.choose(workers, block, Some(excluded))?;
            for item in remote_res {
                res.push(item);
                if res.len() == block.replicas as usize {
                    break;
                }
            }
        }

        Ok(res)
    }

    fn choose_workers(
        &self,
        workers: &IndexMap<u32, WorkerInfo>,
        count: Option<usize>,
        exclude_workers: Option<HashSet<u32>>,
    ) -> CommonResult<Vec<WorkerAddress>> {
        // Since you do not rely on block information, you cannot judge the local worker, and use the polling strategy directly
        self.inner.choose_workers(workers, count, exclude_workers)
    }
}
