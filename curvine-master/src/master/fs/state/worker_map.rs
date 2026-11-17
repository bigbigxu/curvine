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

use crate::master::fs::state::WorkerIntentTable;
use curvine_core_error::err_box;
use curvine_error::{FsError, FsResult};
use curvine_model::{
    apply, AdminState, Facts, Liveness, StorageInfo, TransferWorkerCapabilities, Trigger,
    WorkerAddress, WorkerInfo, WorkerState,
};
use curvine_proto::ComponentInfoProto;
use curvine_runtime::common::LocalTime;
use indexmap::IndexMap;
use log::{error, info, warn};

// Store all worker information.
pub struct WorkerMap {
    pub(crate) workers: IndexMap<u32, WorkerInfo>,
    pub(crate) lost_workers: IndexMap<u32, WorkerInfo>,
    // Operator intent keyed by hostname; outlives worker entries, sessions and ids.
    pub(crate) intents: WorkerIntentTable,
}

impl Default for WorkerMap {
    fn default() -> Self {
        Self::new()
    }
}

impl WorkerMap {
    pub fn new() -> Self {
        Self {
            workers: IndexMap::new(),
            lost_workers: IndexMap::new(),
            intents: WorkerIntentTable::new(),
        }
    }

    pub fn is_decommissioned(&self, hostname: &str) -> bool {
        self.intents.is_decommissioned(hostname)
    }

    /// Admin state the operator asked for `hostname`. `Normal` when there is no
    /// record, which is what makes `Normal` disappear from the intent table.
    pub fn admin_of(&self, hostname: &str) -> AdminState {
        self.intents.admin_of(hostname)
    }

    /// Applies a trigger to every entry registered for `hostname` and returns the
    /// new state. Heartbeat paths use this for liveness triggers; operator paths
    /// go through `MasterFilesystem`, which also journals the intent.
    pub fn apply_trigger(
        &mut self,
        hostname: &str,
        trigger: Trigger,
        facts: &Facts,
    ) -> FsResult<Option<(WorkerState, WorkerState)>> {
        let mut transition = None;
        for worker in self.host_workers_mut(hostname) {
            let from = worker.state();
            let (to, _effects) = apply(from, trigger, facts)
                .map_err(|refusal| FsError::common(refusal.to_string()))?;
            worker.apply_state(to);
            transition = Some((from, to));
        }
        Ok(transition)
    }

    /// Sets both axes of one worker.
    pub fn set_state_for(&mut self, worker_id: u32, state: WorkerState) {
        if let Some(entry) = self.workers.get_mut(&worker_id) {
            entry.apply_state(state);
        }
        if let Some(entry) = self.lost_workers.get_mut(&worker_id) {
            entry.apply_state(state);
        }
    }

    /// Sets the admin axis of one worker, keeping its liveness.
    pub fn set_admin_for(&mut self, worker_id: u32, admin: AdminState) {
        if let Some(entry) = self.workers.get_mut(&worker_id) {
            entry.apply_state(WorkerState::new(admin, entry.liveness()));
        }
        if let Some(entry) = self.lost_workers.get_mut(&worker_id) {
            entry.apply_state(WorkerState::new(admin, entry.liveness()));
        }
    }

    /// Sets the liveness axis of one worker, keeping its admin state.
    pub fn set_liveness_for(&mut self, worker_id: u32, liveness: Liveness) {
        if let Some(entry) = self.workers.get_mut(&worker_id) {
            entry.apply_state(WorkerState::new(entry.admin(), liveness));
        }
        if let Some(entry) = self.lost_workers.get_mut(&worker_id) {
            entry.apply_state(WorkerState::new(entry.admin(), liveness));
        }
    }

    /// Applies an already-computed state to every entry for `hostname` in both
    /// the live and the lost map.
    pub fn set_state_for_host(&mut self, hostname: &str, state: WorkerState) {
        for worker in self.host_workers_mut(hostname) {
            worker.apply_state(state);
        }
    }

    pub fn host_workers_mut<'a>(
        &'a mut self,
        hostname: &'a str,
    ) -> impl Iterator<Item = &'a mut WorkerInfo> + 'a {
        self.workers
            .values_mut()
            .chain(self.lost_workers.values_mut())
            .filter(move |worker| worker.address.hostname == hostname)
    }

    /// Every worker entry on `hostname`, live map first.
    pub fn host_workers(&self, hostname: &str) -> Vec<&WorkerInfo> {
        self.workers
            .values()
            .chain(self.lost_workers.values())
            .filter(|worker| worker.address.hostname == hostname)
            .collect()
    }

    /// Liveness of the host, taken from its entries. `Running` when the host has
    /// no entry at all: an operator decision about an unknown host must not be
    /// recorded as if the node were already dead.
    pub fn host_liveness(&self, hostname: &str) -> Liveness {
        self.host_workers(hostname)
            .into_iter()
            .map(|worker| worker.liveness())
            .next()
            .unwrap_or(Liveness::Running)
    }

    pub fn remove(&mut self, addr: &WorkerAddress) {
        self.workers.swap_remove(&addr.worker_id);
        self.lost_workers.swap_remove(&addr.worker_id);
    }

    pub fn remove_same_endpoint(&mut self, addr: &WorkerAddress) -> Vec<WorkerInfo> {
        let stale_worker_ids: Vec<u32> = self
            .workers
            .iter()
            .filter_map(|(worker_id, worker)| {
                if *worker_id != addr.worker_id && worker.address.same_endpoint(addr) {
                    Some(*worker_id)
                } else {
                    None
                }
            })
            .collect();

        let mut removed = Vec::with_capacity(stale_worker_ids.len());
        for worker_id in stale_worker_ids {
            if let Some(worker) = self.workers.swap_remove(&worker_id) {
                self.lost_workers.swap_remove(&worker_id);
                removed.push(worker);
            }
        }

        removed
    }

    pub fn ensure_worker_id_addr(&self, addr: &WorkerAddress) -> FsResult<()> {
        if let Some(v) = self.workers.get(&addr.worker_id) {
            if v.address != *addr {
                return err_box!(
                    "worker id addr mismatch,  expected {}, actual: {}",
                    v.address,
                    addr
                );
            }
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub fn insert(
        &mut self,
        addr: WorkerAddress,
        weight: u32,
        worker_session_id: String,
        transfer_capabilities: TransferWorkerCapabilities,
        transfer_inflight_budget_bytes: i64,
        worker_max_concurrent_tasks: usize,
        transfer_inflight_tasks: u64,
        transfer_inflight_bytes: i64,
        software_version: String,
        startup_time_ms: u64,
        storages: Vec<StorageInfo>,
        component_info: Option<ComponentInfoProto>,
    ) -> FsResult<()> {
        self.ensure_worker_id_addr(&addr)?;
        for stale in self.remove_same_endpoint(&addr) {
            warn!(
                "Remove stale worker {} before registering {} on the same endpoint",
                stale.simple_debug(),
                addr
            );
        }

        // Register the worker and update the storage information.
        // @todo Heartbeat may be too simple and directly covers historical data.
        let mut info = WorkerInfo::new(addr, weight);
        info.worker_session_id = worker_session_id;
        info.transfer_capabilities = transfer_capabilities;
        info.transfer_inflight_budget_bytes = transfer_inflight_budget_bytes;
        info.worker_max_concurrent_tasks = worker_max_concurrent_tasks;
        info.transfer_inflight_tasks = transfer_inflight_tasks;
        info.transfer_inflight_bytes = transfer_inflight_bytes;
        info.software_version = software_version;
        info.startup_time_ms = startup_time_ms;
        info.component_info = component_info;
        let worker_id = info.worker_id();
        for item in storages {
            info.add_storage(item);
        }

        let previous = match self.workers.get_mut(&worker_id) {
            Some(worker) => {
                info.carry_scheduled_from(worker);
                Some(worker.available)
            }
            None => None,
        };

        // The two axes come from their own sources: admin from the persisted
        // intent table, liveness from the fact that a heartbeat just arrived.
        // Nothing here may overwrite an operator decision (CV-60).
        let admin = self.admin_of(&info.address.hostname);
        info.apply_state(WorkerState::registered(admin));

        if let Some(previous_available) = previous {
            info.reclaim_scheduled_from_heartbeat(previous_available);
        }

        self.workers.insert(worker_id, info);
        if let Some(lost) = self.lost_workers.swap_remove(&worker_id) {
            info!("Lost Worker Recovery: {}", lost.simple_debug())
        }

        Ok(())
    }

    // Delete the worker with heartbeat timeout
    pub fn remove_expired(&mut self, id: u32) -> Option<WorkerInfo> {
        let worker = match self.workers.swap_remove(&id) {
            None => {
                warn!("Not found worker {}", id);
                return None;
            }

            Some(v) => v,
        };

        error!("remove expired worker {}", worker.address);
        // The liveness axis is changed by the caller's transition, which also
        // drops locations and asks for repairs. Moving the entry must not change
        // the state on its own, or the transition would look idempotent.
        self.lost_workers.insert(id, worker.clone());
        Some(worker)
    }

    // Delete blocks that have been offline
    pub fn remove_offline(&mut self, id: u32) -> Option<WorkerInfo> {
        let worker = match self.workers.swap_remove(&id) {
            None => {
                warn!("Not found worker {}", id);
                return None;
            }

            Some(v) => v,
        };

        info!("remove offline worker {}", worker.address);
        let mut worker = worker;
        let (state, _) =
            apply(worker.state(), Trigger::GracefulShutdown, &Facts::default()).ok()?;
        worker.apply_state(state);
        // The grace period is anchored at the moment the process said goodbye.
        worker.last_update = LocalTime::mills();
        self.lost_workers.insert(id, worker.clone());
        Some(worker)
    }

    pub fn workers(&self) -> &IndexMap<u32, WorkerInfo> {
        &self.workers
    }

    pub fn lost_workers(&self) -> &IndexMap<u32, WorkerInfo> {
        &self.lost_workers
    }

    pub fn is_lost_worker_session(&self, worker_id: u32, worker_session_id: &str) -> bool {
        self.lost_workers
            .get(&worker_id)
            .is_some_and(|worker| worker.worker_session_id == worker_session_id)
    }
}
