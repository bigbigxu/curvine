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

use crate::master::fs::MasterFilesystem;
use crate::master::MasterMonitor;
use curvine_common::error::FsError;
use curvine_common::FsResult;
use log::{info, warn};
use orpc::common::{LocalTime, TimeSpent};
use orpc::runtime::{GroupExecutor, LoopTask};
use orpc::try_log;
use std::sync::Arc;

pub struct HeartbeatChecker {
    fs: MasterFilesystem,
    monitor: MasterMonitor,
    executor: Arc<GroupExecutor>,
    worker_blacklist_ms: u64,
    worker_lost_ms: u64,
}

impl HeartbeatChecker {
    pub fn new(fs: MasterFilesystem, monitor: MasterMonitor, executor: Arc<GroupExecutor>) -> Self {
        let worker_blacklist_ms = fs.conf.worker_blacklist_interval_ms();
        let worker_lost_ms = fs.conf.worker_lost_interval_ms();
        Self {
            fs,
            monitor,
            executor,
            worker_blacklist_ms,
            worker_lost_ms,
        }
    }
}

impl LoopTask for HeartbeatChecker {
    type Error = FsError;

    fn run(&self) -> FsResult<()> {
        if !self.monitor.is_active() {
            return Ok(());
        }

        let mut wm = self.fs.worker_manager.write();
        let workers = wm.get_last_heartbeat();
        let now = LocalTime::mills();

        for (id, last_update) in workers {
            if now > last_update + self.worker_blacklist_ms {
                // Worker blacklist timeout
                let worker = wm.add_blacklist_worker(id);
                warn!(
                    "Worker {:?} has no heartbeat for more than {} ms and will be blacklisted",
                    worker, self.worker_blacklist_ms
                );
            }

            if now > last_update + self.worker_lost_ms {
                // Heartbeat timeout
                let removed = wm.remove_expired_worker(id);
                warn!(
                    "Worker {:?} has no heartbeat for more than {} ms and will be removed",
                    removed, self.worker_lost_ms
                );
                // Asynchronously delete all block location data.
                let fs = self.fs.clone();
                let res = self.executor.spawn(move || {
                    let spend = TimeSpent::new();
                    let _ = try_log!(fs.delete_locations(id));
                    info!(
                        "Delete worker {} all locations used {} ms",
                        id,
                        spend.used_ms()
                    );
                });
                let _ = try_log!(res);
            }
        }

        Ok(())
    }

    fn terminate(&self) -> bool {
        self.monitor.is_stop()
    }
}
