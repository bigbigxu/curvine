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

//! Worker state transitions and the drain loop they drive.
//!
//! The transition table lives in [`curvine_model::worker_admin`]; this module is
//! the only place that executes the [`Effect`]s it returns. Two entry points:
//!
//! - [`MasterFilesystem::apply_operator_action`] — an operator decision. It
//!   journals the intent and then projects it onto the live workers.
//! - [`MasterFilesystem::apply_heartbeat_action`] — a liveness transition. It
//!   never writes the intent table; that is what keeps a heartbeat from
//!   overwriting an operator decision (CV-60).
//!
//! Draining reuses the replication pipeline: each drain job copies one block off
//! the draining worker, and only when the copy is committed does the master move
//! the location from the draining worker to the target. Metadata therefore never
//! holds fewer replicas than before at any step, so an interrupted drain cannot
//! lose data.

use crate::master::fs::state::{WorkerIntent, WorkerIntentTable};
use crate::master::fs::MasterFilesystem;
use crate::master::{Master, MasterMetrics};
use curvine_error::{FsError, FsResult};
use curvine_model::{
    apply, AdminState, Effect, Facts, Liveness, Refusal, Trigger, WorkerState, WorkerStatus,
};
use curvine_runtime::common::LocalTime;
use log::{info, warn};
use std::collections::{BTreeSet, HashMap};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

/// How many orphan block ids are kept in the intent record. The journal entry is
/// bounded on purpose: a large worker can orphan millions of blocks, and the
/// count plus a sample is what alerting actually needs.
pub const ORPHAN_SAMPLE_LIMIT: usize = 1024;

/// Above this many pending blocks a drain tick stops checking each block for a
/// possible target and simply queues everything.
const DRAIN_TARGET_CHECK_LIMIT: usize = 128;

/// Why a drain is not making progress. The label goes straight into the
/// `worker_drain_stalled{reason}` gauge.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DrainStall {
    /// The node is `STALE`/`LOST`: it cannot move its own blocks.
    Stale,
    /// No other worker can receive the copy.
    NoTarget,
    /// Blocks remain past `worker_drain_timeout`. Only an operator may force the
    /// completion; the system never drops locations on its own (design §4.1 #6).
    /// Tracked separately from `stalled`: a drain can be past its deadline and
    /// still be making progress.
    DeadlineExceeded,
}

impl DrainStall {
    pub fn as_str_name(&self) -> &'static str {
        match self {
            DrainStall::Stale => "stale",
            DrainStall::NoTarget => "no_target_worker",
            DrainStall::DeadlineExceeded => "deadline_exceeded",
        }
    }

    pub fn all() -> [DrainStall; 3] {
        [
            DrainStall::Stale,
            DrainStall::NoTarget,
            DrainStall::DeadlineExceeded,
        ]
    }
}

/// Per-worker drain progress.
#[derive(Debug, Clone)]
pub struct DrainProgress {
    pub host: String,
    /// Block ids that still have a location on this worker and must be copied
    /// away. Maintained incrementally and re-scanned as a self-healing check.
    pub remaining: BTreeSet<i64>,
    /// When `remaining` was last reconciled against metadata.
    pub scanned_at_ms: u64,
    /// 0 means "no deadline": forced completion stays available to the operator
    /// but never happens on its own.
    pub deadline_ms: u64,
    /// Blocks already copied off this worker.
    pub moved: u64,
    /// Blocks whose only location is this worker: forcing the completion makes
    /// them unreadable.
    pub single_replica_blocks: u64,
    /// Why the last tick could not queue work, if it could not.
    pub stalled: Option<DrainStall>,
    /// Remaining blocks are past `worker_drain_timeout`.
    pub past_deadline: bool,
    /// The deadline warning was already logged (avoid one line per tick).
    pub deadline_reported: bool,
}

impl DrainProgress {
    pub fn new(host: impl Into<String>, deadline_ms: u64) -> Self {
        Self {
            host: host.into(),
            remaining: BTreeSet::new(),
            scanned_at_ms: 0,
            deadline_ms,
            moved: 0,
            single_replica_blocks: 0,
            stalled: None,
            past_deadline: false,
            deadline_reported: false,
        }
    }

    pub fn remaining_count(&self) -> u64 {
        self.remaining.len() as u64
    }
}

/// Registry of draining workers. Lives on the master and is rebuilt from
/// metadata after a restart, so it never needs to be journaled.
#[derive(Debug, Default)]
pub struct WorkerDrainRegistry {
    by_worker: HashMap<u32, DrainProgress>,
    /// Monotonic counter of metadata re-scans, asserted by tests.
    scans: AtomicU64,
}

impl WorkerDrainRegistry {
    pub fn entry(&self, worker_id: u32) -> Option<&DrainProgress> {
        self.by_worker.get(&worker_id)
    }

    pub fn remaining(&self, worker_id: u32) -> Option<u64> {
        self.by_worker
            .get(&worker_id)
            .map(DrainProgress::remaining_count)
    }

    pub fn total_remaining(&self) -> u64 {
        self.by_worker
            .values()
            .map(DrainProgress::remaining_count)
            .sum()
    }

    pub fn moved(&self, worker_id: u32) -> u64 {
        self.by_worker
            .get(&worker_id)
            .map(|entry| entry.moved)
            .unwrap_or(0)
    }

    /// Draining worker that still holds `block_id`, if any.
    pub fn source_of(&self, block_id: i64) -> Option<u32> {
        self.by_worker
            .iter()
            .find(|(_, entry)| entry.remaining.contains(&block_id))
            .map(|(worker_id, _)| *worker_id)
    }

    pub fn scans(&self) -> u64 {
        self.scans.load(Ordering::Relaxed)
    }

    /// Test hook: sets the deadline so a forced completion can be exercised
    /// without waiting for the configured timeout.
    pub fn set_deadline(&mut self, worker_id: u32, deadline_ms: u64) {
        if let Some(entry) = self.by_worker.get_mut(&worker_id) {
            entry.deadline_ms = deadline_ms;
        }
    }

    pub fn retain_workers(&mut self, keep: &[u32]) {
        self.by_worker
            .retain(|worker_id, _| keep.contains(worker_id));
    }
}

/// What one transition did, for the RPC response, the audit log and the tests.
#[derive(Debug, Clone, Default)]
pub struct WorkerTransition {
    pub host: String,
    pub worker_ids: Vec<u32>,
    pub from: Option<WorkerState>,
    pub to: Option<WorkerState>,
    pub trigger: Option<Trigger>,
    pub effects: Vec<Effect>,
    /// Locations dropped by a forced completion.
    pub orphan_blocks: u64,
    /// Block ids those dropped locations belonged to (bounded by the sample limit
    /// when written into the intent record).
    pub dropped_block_ids: Vec<i64>,
    /// Blocks whose replica count fell: the caller reports them to the
    /// replication manager, which owns the copy jobs.
    pub under_replicated: Vec<i64>,
}

impl WorkerTransition {
    /// Legacy single-axis status, for the RPC response and the console.
    pub fn legacy_status(&self) -> WorkerStatus {
        self.to
            .map(legacy_status_of)
            .unwrap_or(WorkerStatus::Unknown)
    }

    pub fn to_label(&self) -> String {
        self.to.map(|state| state.to_string()).unwrap_or_default()
    }

    pub fn from_label(&self) -> String {
        self.from.map(|state| state.to_string()).unwrap_or_default()
    }
}

/// Inverse of [`legacy_status_of`], for tests and for tests only: production
/// code must obtain states from the state machine.
pub fn state_from_legacy(status: WorkerStatus) -> WorkerState {
    match status {
        WorkerStatus::Live | WorkerStatus::Unknown => WorkerState::registered(AdminState::Normal),
        WorkerStatus::Blacklist => WorkerState::new(AdminState::Normal, Liveness::Stale),
        WorkerStatus::Decommission => {
            WorkerState::new(AdminState::Decommissioned, Liveness::Running)
        }
        WorkerStatus::Lost => WorkerState::new(AdminState::Normal, Liveness::Lost),
    }
}

/// Same projection as `WorkerInfo::status`, usable without a `WorkerInfo`.
pub fn legacy_status_of(state: WorkerState) -> WorkerStatus {
    if state.admin() == AdminState::Decommissioned {
        return WorkerStatus::Decommission;
    }
    match state.liveness() {
        Liveness::Lost | Liveness::Shutdown => WorkerStatus::Lost,
        Liveness::Stale => WorkerStatus::Blacklist,
        Liveness::Running => WorkerStatus::Live,
    }
}

/// What the promotion cleanup changed.
#[derive(Debug, Default)]
pub struct DecommissionedCleanup {
    /// Stale block locations dropped from this master's metadata.
    pub dropped: usize,
    /// Blocks that dropped below their replica count and need a repair job.
    pub under_replicated: Vec<i64>,
}

/// Result of one background drain tick.
#[derive(Debug, Default)]
pub struct DrainTick {
    /// `(draining worker, block)` pairs to hand to the replication manager.
    pub to_queue: Vec<(u32, i64)>,
    /// Workers whose drain finished during this tick.
    pub completed: Vec<WorkerTransition>,
}

impl DrainTick {
    pub fn is_empty(&self) -> bool {
        self.to_queue.is_empty() && self.completed.is_empty()
    }
}

/// Maps a legacy status string (and the new action names) onto a trigger.
///
/// `live` / `allow` is state dependent: it undoes whatever constraint is in
/// place, which is what the legacy console and CLI meant by it.
pub fn resolve_action(status: &str, current: AdminState) -> FsResult<Trigger> {
    let action = status.trim().to_ascii_lowercase();
    let trigger = match action.as_str() {
        "cordon" | "blacklist" | "block" => Trigger::Cordon,
        "uncordon" => Trigger::Uncordon,
        "decommission" | "retire" | "drain" => Trigger::Decommission,
        "cancel-decommission" | "cancel_decommission" | "cancel-drain" => {
            Trigger::CancelDecommission
        }
        "recommission" => Trigger::Recommission,
        "force-decommission" | "force_decommission" => Trigger::ForceDecommission,
        "live"
        | "allow"
        | "remove-blacklist"
        | "remove_blacklist"
        | "remove-decommission"
        | "remove_decommission" => match current {
            AdminState::Decommissioned => Trigger::Recommission,
            AdminState::Decommissioning => Trigger::CancelDecommission,
            // CORDONED and NORMAL: `Uncordon` either clears it or is refused with
            // "already in state", which is the honest answer for both.
            AdminState::Cordoned | AdminState::Normal => Trigger::Uncordon,
        },
        other => {
            return Err(FsError::common(format!(
                "unsupported worker status: {other} (expected cordon|uncordon|decommission|\
                 cancel-decommission|recommission|force-decommission|live)"
            )))
        }
    };
    Ok(trigger)
}

fn refusal_error(refusal: Refusal) -> FsError {
    FsError::common(format!("worker state transition refused: {refusal}"))
}

impl MasterFilesystem {
    /// The drain registry, owned by the filesystem and mutated under one lock.
    pub(crate) fn drains(&self) -> &Arc<Mutex<WorkerDrainRegistry>> {
        &self.worker_drains
    }

    fn metrics(&self) -> curvine_core_error::CommonResult<&'static MasterMetrics> {
        Master::get_metrics()
    }

    /// Facts the state machine needs for a guard.
    fn facts_for(&self, worker_ids: &[u32]) -> Facts {
        let blocks: u64 = worker_ids
            .iter()
            .map(|worker_id| self.drain_remaining(*worker_id))
            .sum();
        let deadline_passed = worker_ids.iter().any(|worker_id| {
            self.drain_deadline_ms(*worker_id)
                .is_some_and(|deadline| deadline != 0 && LocalTime::mills() > deadline)
        });
        Facts {
            blocks,
            deadline_passed,
        }
    }

    /// Locations still attached to `worker_id`: the drain counter when it exists,
    /// metadata otherwise.
    pub(crate) fn drain_remaining(&self, worker_id: u32) -> u64 {
        if let Some(remaining) = self.drains().lock().unwrap().remaining(worker_id) {
            return remaining;
        }
        match self.fs_dir.read().get_worker_block_ids(worker_id) {
            Ok(ids) => ids.len() as u64,
            Err(e) => {
                warn!(
                    "cannot count block locations of worker {}: {}",
                    worker_id, e
                );
                0
            }
        }
    }

    /// Drain deadline recorded in the intent, 0 when none is set. Used when a
    /// drain registry entry is rebuilt (master restart, worker recovery), so a
    /// long drain keeps its original deadline.
    fn intent_drain_deadline(&self, host: &str) -> u64 {
        self.worker_manager
            .read()
            .worker_map
            .intents
            .get(host)
            .map(|intent| intent.drain_deadline_ms.max(0) as u64)
            .unwrap_or(0)
    }

    fn drain_deadline_ms(&self, worker_id: u32) -> Option<u64> {
        if let Some(deadline) = self
            .drains()
            .lock()
            .unwrap()
            .entry(worker_id)
            .map(|entry| entry.deadline_ms)
        {
            return Some(deadline);
        }
        let wm = self.worker_manager.read();
        let worker = wm.get_worker(worker_id)?;
        let deadline = wm
            .worker_map
            .intents
            .get(&worker.address.hostname)
            .map(|intent| intent.drain_deadline_ms)
            .unwrap_or(0);
        (deadline > 0).then_some(deadline as u64)
    }

    fn worker_ids_on(&self, host: &str) -> Vec<u32> {
        self.worker_manager
            .read()
            .worker_map
            .host_workers(host)
            .iter()
            .map(|worker| worker.worker_id())
            .collect()
    }

    fn state_of(&self, worker_id: u32) -> Option<WorkerState> {
        self.worker_manager
            .read()
            .get_known_worker(worker_id)
            .map(|worker| worker.state())
    }

    /// Current state of a host: the intent when one exists, plus the liveness of
    /// its entries. Returns `None` for a host that was never seen and has no
    /// intent record.
    fn state_of_host(&self, host: &str) -> Option<WorkerState> {
        let wm = self.worker_manager.read();
        let has_intent = wm.worker_map.intents.get(host).is_some();
        let entries = wm.worker_map.host_workers(host);
        if entries.is_empty() && !has_intent {
            return None;
        }
        Some(WorkerState::new(
            wm.worker_map.admin_of(host),
            wm.worker_map.host_liveness(host),
        ))
    }

    /// Projects the admin axis onto every entry of the host, keeping each
    /// entry's liveness.
    fn project_admin(&self, worker_ids: &[u32], admin: AdminState) {
        let mut wm = self.worker_manager.write();
        for worker_id in worker_ids {
            wm.worker_map.set_admin_for(*worker_id, admin);
        }
    }

    /// Executes the metadata side of a transition. Replication reporting is
    /// returned to the caller: the replication manager is owned by the master
    /// actor, not by the filesystem.
    #[allow(clippy::too_many_arguments)]
    fn execute_effects(
        &self,
        host: &str,
        worker_ids: &[u32],
        from: WorkerState,
        to: WorkerState,
        trigger: Trigger,
        effects: Vec<Effect>,
        operator: &str,
        reason: &str,
    ) -> FsResult<WorkerTransition> {
        let mut outcome = WorkerTransition {
            host: host.to_string(),
            worker_ids: worker_ids.to_vec(),
            from: Some(from),
            to: Some(to),
            trigger: Some(trigger),
            effects: effects.clone(),
            ..Default::default()
        };

        for effect in effects {
            match effect {
                Effect::PersistIntent(admin) => {
                    self.write_worker_intent(host, admin, operator, reason)?;
                }
                Effect::ClearIntent => {
                    self.write_worker_intent(host, AdminState::Normal, operator, reason)?;
                }
                Effect::StartDrain => self.begin_drain(host, worker_ids),
                Effect::StopDrain => {
                    let keep: Vec<u32> = self
                        .worker_manager
                        .read()
                        .decommissioning_workers()
                        .iter()
                        .map(|worker| worker.worker_id())
                        .collect();
                    self.drains().lock().unwrap().retain_workers(&keep);
                }
                Effect::DropLocations | Effect::TriggerReplication => {
                    // Both travel together: dropping locations without asking for
                    // replication would silently lose replicas.
                    for worker_id in worker_ids {
                        let cleanup = self.delete_locations(*worker_id)?;
                        outcome.orphan_blocks += cleanup.removed_block_ids.len() as u64;
                        outcome
                            .dropped_block_ids
                            .extend(cleanup.removed_block_ids.iter().copied());
                        // A forced completion means the operator decided to retire
                        // this node for good: the residual bytes are deleted too, so
                        // the disk can be returned. A normal drain keeps them.
                        if trigger == Trigger::ForceDecommission {
                            let mut wm = self.worker_manager.write();
                            for block_id in &cleanup.removed_block_ids {
                                wm.remove_block(*worker_id, *block_id);
                            }
                        }
                        outcome
                            .under_replicated
                            .extend(cleanup.replication_block_ids.iter().copied());
                    }
                }
                // The orphan list is written by the force-completion path, which
                // knows how many blocks were left behind and keeps a bounded
                // sample. `StartShutdownGrace` is tracked by the heartbeat
                // checker, which owns the grace deadline.
                Effect::RecordOrphans | Effect::StartShutdownGrace => {}
            }
        }

        let metrics = self.metrics()?;
        metrics
            .worker_state_transition_count
            .with_label_values(&[from.as_label().as_str(), to.as_label().as_str()])
            .inc();
        if outcome.orphan_blocks > 0 {
            metrics
                .worker_orphan_blocks
                .inc_by(outcome.orphan_blocks as i64);
        }
        info!(
            "Worker state changed: host={}, workers={:?}, {} -> {}, trigger={:?}, operator={}, reason={}",
            host, worker_ids, from, to, trigger, operator, reason
        );
        Ok(outcome)
    }

    fn write_worker_intent(
        &self,
        host: &str,
        admin: AdminState,
        operator: &str,
        reason: &str,
    ) -> FsResult<()> {
        let mut intent = WorkerIntent::new(admin, operator, reason, 0);
        if admin == AdminState::Decommissioning {
            let timeout = self.conf.worker_drain_timeout_ms();
            intent.drain_deadline_ms = if timeout == 0 {
                0
            } else {
                LocalTime::mills().saturating_add(timeout) as i64
            };
        }
        if admin == AdminState::Decommissioned {
            // Keep the bookkeeping the forced completion produced.
            let wm = self.worker_manager.read();
            if let Some(previous) = wm.worker_map.intents.get(host) {
                intent.orphan_blocks = previous.orphan_blocks.clone();
                intent.orphan_count = previous.orphan_count;
                intent.forced_at_ms = previous.forced_at_ms;
            }
        }
        self.set_worker_intent(host, intent)?;
        Ok(())
    }

    /// Appends the orphan list (count + bounded sample) to the host's intent.
    ///
    /// `orphan_count` counts blocks that lost their **last** copy when the
    /// locations were dropped, i.e. the data the operator gave up by forcing the
    /// completion. Dropping a location that had another replica is not data loss.
    fn record_orphans(&self, host: &str, outcome: &WorkerTransition) {
        let lost: Vec<i64> = outcome
            .dropped_block_ids
            .iter()
            .copied()
            .filter(|block_id| {
                self.fs_dir
                    .read()
                    .get_block_locations(*block_id)
                    .map(|locations| locations.is_empty())
                    .unwrap_or(false)
            })
            .collect();
        let sample = lost
            .iter()
            .take(ORPHAN_SAMPLE_LIMIT)
            .copied()
            .collect::<Vec<_>>();
        let previous = self
            .worker_manager
            .read()
            .worker_map
            .intents
            .get(host)
            .cloned();
        let Some(mut intent) = previous else {
            warn!(
                "Dropped {} location(s) of {} with no intent record; orphans are not journaled",
                outcome.orphan_blocks, host
            );
            return;
        };
        intent.record_orphans(lost.len() as u64, sample, LocalTime::mills() as i64);
        if let Err(e) = self.set_worker_intent(host, intent) {
            warn!("cannot record drain orphans for {host}: {e}");
        }
        warn!(
            "Force-retired {}: {} block(s) deleted, {} lost their last copy",
            host,
            outcome.dropped_block_ids.len(),
            lost.len()
        );
    }

    /// Starts the drain and records the impact preflight in the intent record:
    /// how many locations must move and how many blocks would be lost if the
    /// drain were forced right now.
    fn begin_drain(&self, host: &str, worker_ids: &[u32]) {
        let deadline = {
            let wm = self.worker_manager.read();
            wm.worker_map
                .intents
                .get(host)
                .map(|intent| intent.drain_deadline_ms.max(0) as u64)
                .unwrap_or(0)
        };
        let now = LocalTime::mills();
        let mut total = 0u64;
        let mut single_replica = 0u64;
        {
            let mut registry = self.drains().lock().unwrap();
            for worker_id in worker_ids {
                let entry = registry
                    .by_worker
                    .entry(*worker_id)
                    .or_insert_with(|| DrainProgress::new(host.to_string(), deadline));
                entry.deadline_ms = deadline;
            }
        }
        for worker_id in worker_ids {
            self.refresh_drain_scan(*worker_id, host, now, true);
        }
        {
            let registry = self.drains().lock().unwrap();
            for worker_id in worker_ids {
                let Some(entry) = registry.entry(*worker_id) else {
                    continue;
                };
                total += entry.remaining_count();
                single_replica += entry.single_replica_blocks;
            }
        }

        let previous = self
            .worker_manager
            .read()
            .worker_map
            .intents
            .get(host)
            .cloned();
        if let Some(mut intent) = previous {
            intent.preflight_blocks = total;
            intent.preflight_single_replica = single_replica;
            if let Err(e) = self.set_worker_intent(host, intent) {
                warn!("cannot journal the drain preflight of {host}: {e}");
            }
        }
        if single_replica > 0 {
            warn!(
                "Draining {host}: {} of {} block(s) have no other replica; forcing this drain \
                 would make them unreadable",
                single_replica, total
            );
        } else {
            info!(
                "Draining {host}: {} block location(s) to move, every block has another copy",
                total
            );
        }
    }

    /// Operator action resolved by selector. Journaled, logged, and refused with
    /// a reason when the state machine says no.
    pub fn apply_operator_action(
        &self,
        selector: &str,
        trigger: Trigger,
        operator: &str,
        reason: &str,
    ) -> FsResult<WorkerTransition> {
        let host = self.resolve_operator_host(selector)?;
        let from = self
            .state_of_host(&host)
            .unwrap_or_else(|| WorkerState::registered(AdminState::Normal));
        let worker_ids = self.worker_ids_on(&host);
        let facts = self.facts_for(&worker_ids);

        if trigger == Trigger::Decommission && !self.conf.block_replication_enabled {
            return Err(FsError::common(
                "decommission needs the block replication pipeline: set \
                 master.block_replication_enabled = true, otherwise blocks cannot be \
                 copied off the node",
            ));
        }
        let (to, effects) = apply(from, trigger, &facts).map_err(refusal_error)?;
        self.project_admin(&worker_ids, to.admin());
        let outcome = self.execute_effects(
            &host,
            &worker_ids,
            from,
            to,
            trigger,
            effects,
            operator,
            reason,
        )?;

        // A forced completion drops locations that may hold the last copy of a
        // block: record what was left behind, for alerting and for a later
        // recommission (design §6.3).
        if outcome.orphan_blocks > 0 {
            self.record_orphans(&host, &outcome);
        }

        // Leaving DECOMMISSIONING drops the drain bookkeeping.
        if to.admin() != AdminState::Decommissioning {
            let keep: Vec<u32> = self
                .worker_manager
                .read()
                .decommissioning_workers()
                .iter()
                .map(|worker| worker.worker_id())
                .collect();
            self.drains().lock().unwrap().retain_workers(&keep);
        }
        Ok(outcome)
    }

    /// Liveness transition for one worker (heartbeat, timeout, shutdown grace).
    /// Never touches the intent table.
    pub fn apply_heartbeat_action(
        &self,
        worker_id: u32,
        trigger: Trigger,
        reason: &str,
    ) -> FsResult<Option<WorkerTransition>> {
        debug_assert!(
            trigger.is_heartbeat_driven(),
            "heartbeat transitions must not change the operator intent"
        );
        self.apply_automatic_action(worker_id, trigger, "", reason)
    }

    /// System-driven transition on one worker. `DrainCompleted` and the
    /// deadline-driven `ForceDecommission` are the only triggers that change the
    /// admin axis without an operator request, and they journal the resulting
    /// intent exactly like an operator action would.
    pub fn apply_automatic_action(
        &self,
        worker_id: u32,
        trigger: Trigger,
        actor: &str,
        reason: &str,
    ) -> FsResult<Option<WorkerTransition>> {
        let Some(from) = self.state_of(worker_id) else {
            return Ok(None);
        };
        let facts = self.facts_for(&[worker_id]);
        let (to, effects) = apply(from, trigger, &facts).map_err(refusal_error)?;
        if effects.is_empty() && to == from {
            return Ok(None);
        }
        // Heartbeat triggers only move the liveness axis, but the automatic
        // drain triggers also complete the admin transition.
        self.worker_manager
            .write()
            .worker_map
            .set_state_for(worker_id, to);
        let host = self
            .worker_manager
            .read()
            .get_known_worker(worker_id)
            .map(|worker| worker.address.hostname.clone())
            .unwrap_or_default();
        self.execute_effects(
            &host,
            &[worker_id],
            from,
            to,
            trigger,
            effects,
            actor,
            reason,
        )
        .map(Some)
    }

    /// Moves a worker whose heartbeat exceeded `lost_interval` to `LOST` and
    /// returns the transition (whose effects drop the locations and ask for
    /// repairs).
    ///
    /// The session check makes the decision safe against a worker that
    /// re-registered between the staleness verdict and the transition.
    pub fn retire_lost_worker(
        &self,
        worker_id: u32,
        worker_session_id: &str,
        reason: &str,
    ) -> FsResult<Option<WorkerTransition>> {
        let still_lost = {
            let wm = self.worker_manager.read();
            wm.get_worker(worker_id).is_none()
                && wm.is_lost_worker_session(worker_id, worker_session_id)
        };
        if !still_lost {
            return Ok(None);
        }
        self.apply_heartbeat_action(worker_id, Trigger::LostTimeout, reason)
    }

    /// Moves a `SHUTDOWN` worker to `LOST` once its grace period expired.
    pub fn expire_shutdown_worker(
        &self,
        worker_id: u32,
        worker_session_id: &str,
        now: u64,
        grace_ms: u64,
    ) -> FsResult<Option<WorkerTransition>> {
        {
            let wm = self.worker_manager.read();
            let Some(worker) = wm.get_known_worker(worker_id) else {
                return Ok(None);
            };
            if worker.liveness() != Liveness::Shutdown
                || worker.worker_session_id != worker_session_id
                || now < worker.last_update.saturating_add(grace_ms)
            {
                return Ok(None);
            }
        }
        self.apply_heartbeat_action(
            worker_id,
            Trigger::ShutdownGraceExpired,
            "shutdown grace expired",
        )
    }

    /// Records a transition performed outside `apply` (the worker map mutates the
    /// state under its own lock to stay atomic with the heartbeat).
    ///
    /// Used for `STALE`, which the heartbeat checker applies inside the write
    /// lock: without this the state would change with no log line and no counter.
    pub fn record_direct_state_change(
        &self,
        worker_id: u32,
        before: WorkerState,
        trigger: Trigger,
        reason: &str,
    ) {
        let Some((host, after)) = ({
            let wm = self.worker_manager.read();
            wm.get_known_worker(worker_id)
                .map(|worker| (worker.address.hostname.clone(), worker.state()))
        }) else {
            return;
        };
        if before == after {
            return;
        }
        if let Ok(metrics) = self.metrics() {
            metrics
                .worker_state_transition_count
                .with_label_values(&[before.as_label().as_str(), after.as_label().as_str()])
                .inc();
        }
        info!(
            "Worker state changed: host={}, workers=[{}], {} -> {}, trigger={:?}, reason={}",
            host, worker_id, before, after, trigger, reason
        );
    }

    /// Records a liveness change that happened on the registration path.
    ///
    /// `WorkerMap::insert` projects the new state directly (it runs under the
    /// worker map lock, where the state machine cannot take its own locks), so the
    /// transition is recorded here to keep the log and the counter complete.
    pub fn record_heartbeat_state_change(
        &self,
        worker_id: u32,
        before: WorkerState,
        after: WorkerState,
        heartbeat: curvine_model::HeartbeatStatus,
    ) {
        // Only the liveness axis can change on the registration path. An admin
        // difference observed here means the state machine transitioned the node
        // between the two reads, and it already recorded that transition; logging
        // it again would double count and blame the heartbeat.
        if before == after || before.liveness() == after.liveness() {
            return;
        }
        if let Ok(metrics) = self.metrics() {
            metrics
                .worker_state_transition_count
                .with_label_values(&[before.as_label().as_str(), after.as_label().as_str()])
                .inc();
        }
        let host = self
            .worker_manager
            .read()
            .get_known_worker(worker_id)
            .map(|worker| worker.address.hostname.clone())
            .unwrap_or_default();
        // Name the trigger the same way the state machine does, so a coverage
        // check can look for `trigger=HeartbeatOk` / `trigger=GracefulShutdown`.
        let trigger = match heartbeat {
            curvine_model::HeartbeatStatus::End => Trigger::GracefulShutdown,
            _ => Trigger::HeartbeatOk,
        };
        info!(
            "Worker state changed: host={}, workers=[{}], {} -> {}, trigger={:?}",
            host, worker_id, before, after, trigger
        );
    }

    /// Background drain tick: queue copy jobs, finish drained workers, enforce
    /// the deadline. Returns work the caller must hand to the replication
    /// manager.
    pub fn drain_tick(&self) -> FsResult<DrainTick> {
        let mut tick = DrainTick::default();
        let workers = self.worker_manager.read().decommissioning_workers();
        if workers.is_empty() {
            self.drains().lock().unwrap().by_worker.clear();
            self.publish_drain_metrics();
            return Ok(tick);
        }

        let now = LocalTime::mills();
        let scan_interval = self.conf.worker_drain_scan_interval_ms();
        let mut completions = Vec::new();

        for worker in workers {
            let worker_id = worker.worker_id();
            let host = worker.address.hostname.clone();
            let liveness = worker.liveness();

            // A drain can begin without the operator path: an intent recorded
            // while the node was away is applied on registration, and a master
            // restart rebuilds the map from the journal. The first tick then has
            // to run the same preflight and journal it, or the impact numbers and
            // the original deadline would be lost.
            let entry_missing = self.drains().lock().unwrap().entry(worker_id).is_none();
            if entry_missing {
                self.begin_drain(&host, &[worker_id]);
            }

            let needs_scan = {
                let registry = self.drains().lock().unwrap();
                match registry.entry(worker_id) {
                    None => true,
                    Some(entry) => {
                        scan_interval == 0
                            || now.saturating_sub(entry.scanned_at_ms) > scan_interval
                    }
                }
            };
            if needs_scan {
                self.refresh_drain_scan(worker_id, &host, now, false);
            }

            let Some(entry) = self.drains().lock().unwrap().entry(worker_id).cloned() else {
                continue;
            };

            if entry.remaining.is_empty() {
                // Confirm with a fresh scan before declaring completion, so a
                // half-applied move cannot end the drain early.
                self.refresh_drain_scan(worker_id, &host, now, false);
                let empty = self
                    .drains()
                    .lock()
                    .unwrap()
                    .entry(worker_id)
                    .is_some_and(|entry| entry.remaining.is_empty());
                if empty {
                    match self.apply_automatic_action(
                        worker_id,
                        Trigger::DrainCompleted,
                        "system",
                        "drained",
                    ) {
                        Ok(Some(outcome)) => completions.push(outcome),
                        Ok(None) => {}
                        Err(e) => warn!("drain completion refused for worker {worker_id}: {e}"),
                    }
                }
                continue;
            }

            // A deadline that passed does NOT complete the drain: forcing drops
            // the remaining locations, which may lose the last copy of a block, so
            // design §4.1 #6 makes it an operator decision. Keep draining, report
            // the stall, and let `cv node force-decommission` do the damage.
            if entry.deadline_ms != 0 && now > entry.deadline_ms {
                if let Some(state) = self.drains().lock().unwrap().by_worker.get_mut(&worker_id) {
                    state.past_deadline = true;
                }
                if !entry.deadline_reported {
                    if let Some(state) = self.drains().lock().unwrap().by_worker.get_mut(&worker_id)
                    {
                        state.deadline_reported = true;
                    }
                    warn!(
                        "Drain of worker {} is past its deadline ({} block(s) left); \
                         it will keep draining. Force the completion explicitly \
                         (`cv node force-decommission`) to drop the remaining locations",
                        worker_id,
                        entry.remaining_count()
                    );
                }
            }

            // A worker that is not running cannot move its blocks: pause, but keep
            // the progress and the deadline.
            if liveness != Liveness::Running {
                self.set_drain_stall(worker_id, Some(DrainStall::Stale));
                continue;
            }
            if !self.has_drain_target(worker_id) {
                self.set_drain_stall(worker_id, Some(DrainStall::NoTarget));
                continue;
            }

            // Small drains get a per-block target check so a fully blocked drain
            // reports `no_target_worker` instead of retrying impossible copies
            // forever. Larger sets skip it: the per-block location lookup would
            // be O(pending) on every tick.
            let mut movable = Vec::with_capacity(entry.remaining.len());
            let mut blocked = 0usize;
            if entry.remaining.len() <= DRAIN_TARGET_CHECK_LIMIT {
                for block_id in &entry.remaining {
                    if self.drain_target_exists_for_block(*block_id) {
                        movable.push(*block_id);
                    } else {
                        blocked += 1;
                    }
                }
                if movable.is_empty() {
                    self.set_drain_stall(worker_id, Some(DrainStall::NoTarget));
                    if blocked > 0 {
                        info!(
                            "Drain of worker {worker_id} is blocked: {blocked} block(s) have no \
                             writable worker left to copy to"
                        );
                    }
                    continue;
                }
                if blocked > 0 {
                    info!(
                        "Drain of worker {worker_id}: {blocked} of {} block(s) have no target; \
                         moving the other {}",
                        entry.remaining_count(),
                        movable.len()
                    );
                }
            } else {
                movable.extend(entry.remaining.iter().copied());
            }
            self.set_drain_stall(worker_id, None);
            tick.to_queue
                .extend(movable.into_iter().map(|block| (worker_id, block)));
        }

        self.publish_drain_metrics();

        tick.completed = completions;
        Ok(tick)
    }

    /// Is there any worker other than `worker_id` that can receive a copy?
    fn has_drain_target(&self, worker_id: u32) -> bool {
        let draining = self.drain_sources();
        self.worker_manager
            .read()
            .worker_map
            .workers
            .values()
            .any(|worker| {
                worker.writable()
                    && worker.worker_id() != worker_id
                    && !draining.contains(&worker.worker_id())
            })
    }

    /// Can this block be copied to some writable worker that does not hold it yet?
    ///
    /// `has_drain_target` only answers the fleet-level question. A block can still
    /// be unmovable when the only writable workers already hold it, which happens
    /// as soon as the operator cordons everything but one node: reporting that as
    /// "not stalled" would hide a drain that cannot converge.
    fn drain_target_exists_for_block(&self, block_id: i64) -> bool {
        let locations = match self.fs_dir.read().get_block_locations(block_id) {
            Ok(locations) => locations,
            Err(e) => {
                warn!("cannot read locations of block {block_id}: {e}");
                return false;
            }
        };
        let draining = self.drain_sources();
        self.worker_manager
            .read()
            .worker_map
            .workers
            .values()
            .any(|worker| {
                worker.writable()
                    && !draining.contains(&worker.worker_id())
                    && !locations
                        .iter()
                        .any(|location| location.worker_id == worker.worker_id())
            })
    }

    fn set_drain_stall(&self, worker_id: u32, stall: Option<DrainStall>) {
        if let Some(entry) = self.drains().lock().unwrap().by_worker.get_mut(&worker_id) {
            entry.stalled = stall;
        }
    }

    /// Publishes the drain backlog and stall reasons so a drain that cannot
    /// progress is visible instead of looking busy.
    ///
    /// Every known worker is published, not just the draining ones: a gauge that
    /// stops being updated would keep the last backlog value forever and make a
    /// finished drain look stuck.
    fn publish_drain_metrics(&self) {
        let Ok(metrics) = self.metrics() else {
            return;
        };
        let known: Vec<u32> = {
            let wm = self.worker_manager.read();
            wm.worker_map
                .workers
                .keys()
                .chain(wm.worker_map.lost_workers.keys())
                .copied()
                .collect()
        };
        let registry = self.drains().lock().unwrap();
        for worker_id in known {
            let remaining = registry.remaining(worker_id).unwrap_or(0);
            metrics
                .worker_drain_blocks_remaining
                .with_label_values(&[&worker_id.to_string()])
                .set(remaining as i64);
        }
        for reason in DrainStall::all() {
            let count = registry
                .by_worker
                .values()
                .filter(|entry| match reason {
                    DrainStall::DeadlineExceeded => entry.past_deadline,
                    other => entry.stalled == Some(other),
                })
                .count() as i64;
            metrics
                .worker_drain_stalled
                .with_label_values(&[reason.as_str_name()])
                .set(count);
        }
        metrics.worker_drain_single_replica_blocks.set(
            registry
                .by_worker
                .values()
                .map(|entry| entry.single_replica_blocks as i64)
                .sum(),
        );
    }

    /// Reconciles the drain's block set against metadata.
    ///
    /// `count_single_replica` is only set for the preflight scan: the exact count
    /// needs one location lookup per block, so it is not repeated on every
    /// periodic rescan. The periodic scan only walks the `CF_LOCATION` prefix.
    fn refresh_drain_scan(&self, worker_id: u32, host: &str, now: u64, count_single_replica: bool) {
        let (ids, single_replica) = {
            let fs_dir = self.fs_dir.read();
            let ids = match fs_dir.get_worker_block_ids(worker_id) {
                Ok(ids) => ids,
                Err(e) => {
                    warn!("cannot scan block locations of worker {worker_id}: {e}");
                    return;
                }
            };
            let single_replica = if count_single_replica {
                ids.iter()
                    .filter(|block_id| {
                        fs_dir
                            .get_block_locations(**block_id)
                            .map(|locations| locations.len() <= 1)
                            .unwrap_or(false)
                    })
                    .count() as u64
            } else {
                // keep the preflight value
                u64::MAX
            };
            (ids, single_replica)
        };
        let deadline = self.intent_drain_deadline(host);
        let mut registry = self.drains().lock().unwrap();
        let entry = registry
            .by_worker
            .entry(worker_id)
            .or_insert_with(|| DrainProgress::new(host.to_string(), deadline));
        entry.remaining = ids.into_iter().collect();
        if single_replica != u64::MAX {
            entry.single_replica_blocks = single_replica;
        }
        entry.scanned_at_ms = now;
        registry.scans.fetch_add(1, Ordering::Relaxed);
    }

    /// Called by the replication manager after a copy off a draining worker has
    /// been committed. Returns the number of blocks still to move.
    pub fn on_drain_block_moved(&self, worker_id: u32, block_id: i64) -> u64 {
        let mut registry = self.drains().lock().unwrap();
        let Some(entry) = registry.by_worker.get_mut(&worker_id) else {
            return 0;
        };
        if entry.remaining.remove(&block_id) {
            entry.moved = entry.moved.saturating_add(1);
        }
        entry.remaining_count()
    }

    /// Draining worker that still holds `block_id`. The replication manager uses
    /// it to force a copy even when the replica count is already satisfied.
    pub fn drain_source_of_block(&self, block_id: i64) -> Option<u32> {
        self.drains().lock().unwrap().source_of(block_id)
    }

    /// All workers currently draining, used to keep drain copies from landing on
    /// another draining worker.
    pub fn drain_sources(&self) -> Vec<u32> {
        let registry = self.drains().lock().unwrap();
        registry.by_worker.keys().copied().collect()
    }

    /// A snapshot of one drain, for tests and the console.
    pub fn drain_progress(&self, worker_id: u32) -> Option<DrainProgress> {
        self.drains().lock().unwrap().entry(worker_id).cloned()
    }

    /// Blocks queued for a draining worker. Used by tests and the console.
    pub fn drain_pending(&self, worker_id: u32) -> Vec<i64> {
        self.drains()
            .lock()
            .unwrap()
            .entry(worker_id)
            .map(|entry| entry.remaining.iter().copied().collect())
            .unwrap_or_default()
    }

    /// Moves one replica off a draining worker atomically in metadata: the new
    /// location is inserted before the old one is removed, so a block never has
    /// fewer copies than before the move.
    pub(crate) fn commit_drain_move(
        &self,
        block_id: i64,
        to: curvine_model::BlockLocation,
        from_worker: u32,
    ) -> FsResult<bool> {
        // CV-70：位置写入统一经 location_entry 汇聚（journal 落点）。
        let moved = self.commit_drain_move_location(block_id, to.clone(), from_worker)?;
        if moved {
            self.on_drain_block_moved(from_worker, block_id);
            info!(
                "Drained block {} off worker {} to worker {}",
                block_id, from_worker, to.worker_id
            );
        }
        Ok(moved)
    }

    /// Drops every location that belongs to a `DECOMMISSIONED` host.
    ///
    /// Block-location edits are not journaled (locations are rebuilt from block
    /// reports), so a follower that becomes leader holds the location set from
    /// before the drain: the blocks a drain moved away reappear on the node that
    /// was drained, which contradicts its intent. The intent *is* journaled, so
    /// the new leader can restore the invariant directly. Bytes on disk are
    /// untouched; a recommission re-adds them from the worker's full report.
    pub fn drop_locations_of_decommissioned_hosts(&self) -> FsResult<DecommissionedCleanup> {
        let decommissioned = {
            let wm = self.worker_manager.read();
            wm.worker_map
                .intents
                .iter()
                .filter(|(_, intent)| intent.is_decommissioned())
                .map(|(host, _)| host.clone())
                .collect::<Vec<_>>()
        };
        let mut cleanup_result = DecommissionedCleanup::default();
        for host in decommissioned {
            for worker_id in self.worker_ids_on(&host) {
                let cleanup = self.delete_locations(worker_id)?;
                cleanup_result.dropped += cleanup.removed_block_ids.len();
                cleanup_result
                    .under_replicated
                    .extend(cleanup.replication_block_ids.iter().copied());
                if !cleanup.removed_block_ids.is_empty() {
                    warn!(
                        "Decommissioned worker {} still had {} location(s) in this master's \
                         metadata (stale after a failover); dropped them",
                        worker_id,
                        cleanup.removed_block_ids.len()
                    );
                }
                if !cleanup.replication_block_ids.is_empty() {
                    warn!(
                        "Decommissioned worker {} left {} block(s) under-replicated after the \
                         stale-location cleanup; they must be repaired",
                        worker_id,
                        cleanup.replication_block_ids.len()
                    );
                }
            }
        }
        if cleanup_result.dropped > 0 {
            info!(
                "Restored the DECOMMISSIONED invariant after promotion: {} stale block \
                 location(s) dropped, {} block(s) now need repair",
                cleanup_result.dropped,
                cleanup_result.under_replicated.len()
            );
        }
        Ok(cleanup_result)
    }

    /// Selector resolution for operator actions: worker id, `host:port`, ip or
    /// hostname. Hosts without any worker entry are accepted, so an operator can
    /// fence a node before it ever registers (design #16).
    pub fn resolve_operator_host(&self, selector: &str) -> FsResult<String> {
        let selector = selector.trim();
        if selector.is_empty() {
            return Err(FsError::common("empty worker selector"));
        }
        {
            let wm = self.worker_manager.read();
            let matches = |worker: &&curvine_model::WorkerInfo| {
                if selector.contains(':') {
                    worker.address.connect_addr() == selector
                } else {
                    worker.worker_id().to_string() == selector
                        || worker.address.hostname == selector
                        || worker.address.ip_addr == selector
                }
            };
            if let Some(worker) = wm
                .worker_map
                .workers
                .values()
                .find(|worker| matches(worker))
            {
                return Ok(worker.address.hostname.clone());
            }
            if let Some(worker) = wm
                .worker_map
                .lost_workers()
                .values()
                .find(|worker| matches(worker))
            {
                return Ok(worker.address.hostname.clone());
            }
        }
        Ok(selector.to_string())
    }

    /// Intent records, for the console and CLI.
    pub fn worker_intents(&self) -> WorkerIntentTable {
        self.worker_manager.read().worker_map.intents.clone()
    }

    /// Admin state an operator action would start from.
    pub fn current_admin(&self, selector: &str) -> FsResult<AdminState> {
        let host = self.resolve_operator_host(selector)?;
        Ok(self
            .state_of_host(&host)
            .map(|state| state.admin())
            .unwrap_or(AdminState::Normal))
    }

    /// Forgets a host entirely: the intent record and any leftover locations.
    ///
    /// This is not a state transition (design #8). It is refused while the node
    /// is reachable, because dropping the intent would let its next block report
    /// re-register data that the operator wanted gone.
    pub fn purge_worker(&self, selector: &str, operator: &str) -> FsResult<WorkerTransition> {
        let host = self.resolve_operator_host(selector)?;
        let worker_ids = {
            let wm = self.worker_manager.read();
            wm.worker_map
                .host_workers(&host)
                .iter()
                .map(|worker| worker.worker_id())
                .collect::<Vec<_>>()
        };
        let online = self
            .worker_manager
            .read()
            .worker_map
            .host_workers(&host)
            .iter()
            .any(|worker| worker.liveness() == Liveness::Running);
        if online {
            return Err(FsError::common(format!(
                "refusing to purge {host}: the node is still heartbeating; cordon or decommission it first"
            )));
        }

        let mut under_replicated = Vec::new();
        for worker_id in &worker_ids {
            let cleanup = self.delete_locations(*worker_id)?;
            under_replicated.extend(cleanup.replication_block_ids.iter().copied());
        }
        self.set_worker_intent(
            &host,
            WorkerIntent::new(AdminState::Normal, operator, "purge", 0),
        )?;
        {
            let mut wm = self.worker_manager.write();
            for worker_id in &worker_ids {
                wm.worker_map.workers.shift_remove(worker_id);
                wm.worker_map.lost_workers.shift_remove(worker_id);
            }
        }
        let keep: Vec<u32> = self
            .worker_manager
            .read()
            .decommissioning_workers()
            .iter()
            .map(|worker| worker.worker_id())
            .collect();
        self.drains().lock().unwrap().retain_workers(&keep);

        info!("Purged worker host {} ({} entries)", host, worker_ids.len());
        Ok(WorkerTransition {
            host,
            worker_ids,
            under_replicated,
            ..Default::default()
        })
    }
}
