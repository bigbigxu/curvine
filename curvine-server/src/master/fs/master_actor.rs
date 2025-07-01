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

use crate::master::fs::{HeartbeatChecker, MasterFilesystem};
use crate::master::MasterMonitor;
use curvine_common::executor::ScheduledExecutor;
use log::info;
use orpc::runtime::GroupExecutor;
use orpc::CommonResult;
use std::sync::Arc;

pub struct MasterActor {
    fs: MasterFilesystem,
    master_monitor: MasterMonitor,
    executor: Arc<GroupExecutor>,
}

impl MasterActor {
    pub fn new(
        fs: MasterFilesystem,
        master_monitor: MasterMonitor,
        executor: Arc<GroupExecutor>,
    ) -> Self {
        Self {
            fs,
            master_monitor,
            executor,
        }
    }

    pub fn start(&self) {
        info!("start master actor");
        Self::start_heartbeat_checker(
            self.fs.clone(),
            self.master_monitor.clone(),
            self.executor.clone(),
        )
        .unwrap();
    }

    fn start_heartbeat_checker(
        fs: MasterFilesystem,
        master_monitor: MasterMonitor,
        executor: Arc<GroupExecutor>,
    ) -> CommonResult<()> {
        let check_ms = fs.conf.worker_check_interval_ms();
        let scheduler = ScheduledExecutor::new("worker-heartbeat", check_ms);

        let task = HeartbeatChecker::new(fs, master_monitor, executor);

        scheduler.start(task)?;
        Ok(())
    }
}
