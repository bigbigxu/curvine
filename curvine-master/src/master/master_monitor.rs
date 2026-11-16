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

use crate::master::journal::RecoveryGate;
use curvine_raft::raft::RoleState;
use curvine_runtime::common::LocalTime;
use curvine_runtime::sync::StateCtl;
use num_enum::{FromPrimitive, IntoPrimitive};
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

// master state controller
#[repr(i8)]
#[derive(PartialEq, PartialOrd, Debug, Clone, Copy, IntoPrimitive, FromPrimitive)]
pub enum MasterState {
    // Active master node, only this state can provide a metadata lake.
    Active = 1,
    // Slave node, only copy logs.
    #[num_enum(default)]
    Standby = 2,

    // The node is in safe mode.
    SafeMode = 3,

    // The node has exited.
    Exit = 4,
}

/// Coarse phase of a master process, derived from watermarks and role.
///
/// The derivation never depends on the return value of an await, so the phase
/// stays answerable while a recovery is stuck -- which is exactly when an
/// operator needs it. `ufs_lag` is deliberately *not* a phase: the UFS side
/// effect is asynchronous compensation and does not change what the metadata
/// service can do.
#[repr(i8)]
#[derive(PartialEq, Eq, Debug, Clone, Copy, IntoPrimitive, FromPrimitive)]
pub enum MasterPhase {
    /// Process is up, no raft role yet, and nothing left to replay.
    #[num_enum(default)]
    Starting = 1,
    /// Applying committed entries without a role: startup replay, or a follower
    /// catching up on the leader's log.
    Replaying = 2,
    /// Elected leader, still applying the committed tail before it serves.
    CatchingUp = 3,
    /// Leader with `applied == committed`: the metadata service is available.
    Serving = 4,
    /// Follower with `applied == committed`: warm standby.
    StandingBy = 5,
    /// Member recovery quarantine: rebuilding from peers, excluded from voting.
    Quarantined = 6,
    /// Local metadata is known to be damaged: process and ops plane stay up.
    SafeMode = 7,
    /// The raft node stopped.
    Exiting = 8,
}

impl MasterPhase {
    /// Stable name for HTTP payloads, CLI output and log lines.
    pub fn as_str(self) -> &'static str {
        match self {
            MasterPhase::Starting => "Starting",
            MasterPhase::Replaying => "Replaying",
            MasterPhase::CatchingUp => "CatchingUp",
            MasterPhase::Serving => "Serving",
            MasterPhase::StandingBy => "StandingBy",
            MasterPhase::Quarantined => "Quarantined",
            MasterPhase::SafeMode => "SafeMode",
            MasterPhase::Exiting => "Exiting",
        }
    }
}

/// Watermarks published by the journal loader for read-only observers.
///
/// Clone shares the same atomics. Every index is monotone, so readers never need
/// the journal's locks: HTTP probes, metrics and the CLI can all answer while
/// the apply worker is busy replaying.
#[derive(Clone)]
pub struct ProgressView {
    inner: Arc<ProgressInner>,
}

/// Apply rate observed over a sliding window.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ApplyRate {
    pub entries_per_sec: f64,
    pub samples: usize,
    pub window_ms: u64,
}

/// Window length for [`ProgressView::record_sample`].
pub const RATE_WINDOW_MS: u64 = 60_000;
/// Minimum samples before a rate is worth showing to a human.
pub const RATE_MIN_SAMPLES: usize = 3;
/// How long without progress before a recovery is reported as stalled rather
/// than merely slow.
pub const STALL_AFTER_MS: u64 = 10_000;
const RATE_MAX_SAMPLES: usize = 256;

#[derive(Default)]
struct ProgressInner {
    applied: AtomicU64,
    commit: AtomicU64,
    /// Applied index the current replay episode started from: 0, or the index
    /// carried by the snapshot that was installed.
    replay_base: AtomicU64,
    ufs_applied: AtomicU64,
    /// `MasterPhase` discriminant last observed, 0 while never observed.
    last_phase: AtomicI64,
    phase_entered_ms: AtomicU64,
    /// Wall clock of the last `applied` change. Written by the journal loader,
    /// so stall detection does not depend on anyone scraping.
    last_progress_ms: AtomicU64,
    /// `(ms, applied)` observations, oldest first. Only used for the rate.
    samples: Mutex<VecDeque<(u64, u64)>>,
}

impl Default for ProgressView {
    fn default() -> Self {
        Self::new()
    }
}

impl ProgressView {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(ProgressInner::default()),
        }
    }

    pub fn applied(&self) -> u64 {
        self.inner.applied.load(Ordering::Acquire)
    }

    pub fn commit(&self) -> u64 {
        self.inner.commit.load(Ordering::Acquire)
    }

    /// Start of the current replay episode. A progress percentage is only
    /// meaningful relative to this index, never relative to zero.
    pub fn replay_base(&self) -> u64 {
        self.inner.replay_base.load(Ordering::Acquire)
    }

    pub fn ufs_applied(&self) -> u64 {
        self.inner.ufs_applied.load(Ordering::Acquire)
    }

    /// Entries applied but not yet compensated against UFS. Only the leader
    /// advances the UFS watermark; a standby reports zero by construction.
    pub fn ufs_lag(&self) -> u64 {
        self.applied().saturating_sub(self.ufs_applied())
    }

    pub fn set_watermarks(&self, applied: u64, ufs_applied: u64) {
        let previous = self.inner.applied.swap(applied, Ordering::AcqRel);
        self.inner.ufs_applied.store(ufs_applied, Ordering::Release);
        if applied != previous {
            self.inner
                .last_progress_ms
                .store(LocalTime::mills(), Ordering::Release);
        }
    }

    /// Wall clock of the last applied-watermark change, 0 before the first one.
    pub fn last_progress_ms(&self) -> u64 {
        self.inner.last_progress_ms.load(Ordering::Acquire)
    }

    /// No progress for [`STALL_AFTER_MS`]. A stuck recovery must say so: a
    /// progress bar frozen at 42% is worse than no progress bar at all.
    pub fn stalled(&self, now_ms: u64) -> bool {
        let last = self.last_progress_ms();
        last > 0 && now_ms.saturating_sub(last) >= STALL_AFTER_MS
    }

    /// Record one observation of the applied watermark and return the rate over
    /// the sliding window. Observers (a scrape, a CLI call) drive the sampling:
    /// without observations there is no rate to report, which is why callers
    /// must expose the sample count.
    pub fn record_sample(&self, now_ms: u64) -> Option<ApplyRate> {
        let applied = self.applied();
        let mut samples = match self.inner.samples.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };

        match samples.back_mut() {
            // Repeated observations inside the same millisecond must not skew
            // the window, so keep the newest value only.
            Some((ts, value)) if *ts == now_ms => *value = applied,
            _ => samples.push_back((now_ms, applied)),
        }

        // Keep the newest observation that is already outside the window as the
        // baseline, otherwise a scrape interval longer than the window would
        // report "no rate at all" exactly when progress is slowest.
        let cutoff = now_ms.saturating_sub(RATE_WINDOW_MS);
        while samples.len() > 1 && samples.get(1).is_some_and(|(ts, _)| *ts <= cutoff) {
            samples.pop_front();
        }
        while samples.len() > RATE_MAX_SAMPLES {
            samples.pop_front();
        }

        // Nothing has moved for a whole window: report zero instead of letting
        // an old anchor make a finished replay look like it is still running.
        let last_progress = self.last_progress_ms();
        if last_progress > 0 && now_ms.saturating_sub(last_progress) >= RATE_WINDOW_MS {
            return Some(ApplyRate {
                entries_per_sec: 0.0,
                samples: samples.len(),
                window_ms: now_ms.saturating_sub(samples.front()?.0),
            });
        }

        let (first_ts, first_applied) = *samples.front()?;
        let (last_ts, last_applied) = *samples.back()?;
        let elapsed_ms = last_ts.saturating_sub(first_ts);
        if elapsed_ms == 0 || last_applied <= first_applied {
            return Some(ApplyRate {
                entries_per_sec: 0.0,
                samples: samples.len(),
                window_ms: elapsed_ms,
            });
        }

        Some(ApplyRate {
            entries_per_sec: (last_applied - first_applied) as f64 / (elapsed_ms as f64 / 1000.0),
            samples: samples.len(),
            window_ms: elapsed_ms,
        })
    }

    pub fn set_commit(&self, commit: u64) {
        self.inner.commit.store(commit, Ordering::Release);
    }

    pub fn set_replay_base(&self, replay_base: u64) {
        self.inner.replay_base.store(replay_base, Ordering::Release);
    }

    pub fn phase_entered_ms(&self) -> u64 {
        self.inner.phase_entered_ms.load(Ordering::Acquire)
    }

    /// Record the phase a reader just observed and return when it was entered.
    ///
    /// The phase is derived on read, so the transition timestamp can only be
    /// recorded by an observer. Both `/phase` and `/metrics` call this, which is
    /// enough for the timestamp to advance even when nobody is scraping.
    pub fn observe_phase(&self, phase: MasterPhase, now_ms: u64) -> u64 {
        let next = i64::from(i8::from(phase));
        let previous = self.inner.last_phase.load(Ordering::Acquire);
        if previous == next {
            return self.phase_entered_ms();
        }

        if self
            .inner
            .last_phase
            .compare_exchange(previous, next, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            self.inner.phase_entered_ms.store(now_ms, Ordering::Release);
        }

        self.phase_entered_ms()
    }
}

#[derive(Clone)]
pub struct MasterMonitor {
    pub(crate) journal_ctl: StateCtl,
    pub fs_ctl: StateCtl,
    recovery: RecoveryGate,
    /// Raft term of the last persisted hard state, published by the journal
    /// loader. Client-session lease
    /// epochs are keyed on it: a promotion always carries a higher term than the
    /// previous leadership, while a standby can still observe it (#1883 review).
    term: Arc<AtomicU64>,
    /// Replay watermarks published by the journal loader (#CV-94).
    progress: ProgressView,
    /// Leader announced by the last raft `SoftState`, 0 while none is known.
    leader_id: Arc<AtomicU64>,
    /// Term whose promotion gate has been latched, see [`Self::serving_gate`].
    latch_term: Arc<AtomicU64>,
    /// Whether that term already reached `applied == committed` once.
    latch_caught_up: Arc<AtomicBool>,
}

impl MasterMonitor {
    pub fn new(journal_ctl: StateCtl, fs_ctl: StateCtl) -> Self {
        Self::with_term(journal_ctl, fs_ctl, Arc::new(AtomicU64::new(0)))
    }

    /// Build a monitor that shares its term handle with the journal loader.
    pub fn with_term(journal_ctl: StateCtl, fs_ctl: StateCtl, term: Arc<AtomicU64>) -> Self {
        Self {
            journal_ctl,
            fs_ctl,
            recovery: RecoveryGate::new(),
            term,
            progress: ProgressView::new(),
            leader_id: Arc::new(AtomicU64::new(0)),
            latch_term: Arc::new(AtomicU64::new(u64::MAX)),
            latch_caught_up: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Build a monitor that shares the term and leader handles with the raft
    /// node, so `/phase` can name the leader without touching raft state.
    pub fn with_raft_handles(
        journal_ctl: StateCtl,
        fs_ctl: StateCtl,
        term: Arc<AtomicU64>,
        leader_id: Arc<AtomicU64>,
    ) -> Self {
        Self {
            journal_ctl,
            fs_ctl,
            recovery: RecoveryGate::new(),
            term,
            progress: ProgressView::new(),
            leader_id,
            latch_term: Arc::new(AtomicU64::new(u64::MAX)),
            latch_caught_up: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Raft id of the announced leader, 0 while none is known.
    pub fn leader_id(&self) -> u64 {
        self.leader_id.load(Ordering::Acquire)
    }

    /// Shared replay-watermark view, also handed to the journal loader.
    pub fn progress(&self) -> ProgressView {
        self.progress.clone()
    }

    /// Whether the promotion catch-up gate is open for the current term.
    ///
    /// A serving leader is never exactly caught up: there is always one entry
    /// in flight between commit and apply. Latching "caught up" once per term
    /// separates the promotion gate (which must be waited for) from steady-state
    /// lag (which must not flap `/readyz`), and a new term re-arms it.
    fn serving_gate(&self, caught_up: bool) -> bool {
        let term = self.term();
        let latched_term = self.latch_term.load(Ordering::Acquire);

        if latched_term != term {
            if self
                .latch_term
                .compare_exchange(latched_term, term, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                self.latch_caught_up.store(caught_up, Ordering::Release);
            }
            return self.latch_caught_up.load(Ordering::Acquire);
        }

        if caught_up && !self.latch_caught_up.load(Ordering::Acquire) {
            self.latch_caught_up.store(true, Ordering::Release);
        }
        self.latch_caught_up.load(Ordering::Acquire)
    }

    /// Derive the externally visible phase.
    ///
    /// `quarantined` comes from the durable member-recovery marker, so the
    /// caller decides how expensive that lookup is allowed to be: the HTTP plane
    /// reads it from the journal store, tests pass it directly.
    pub fn phase(&self, quarantined: bool) -> MasterPhase {
        let role = self.journal_role();
        if role == RoleState::Exit {
            return MasterPhase::Exiting;
        }
        if self.is_safe_mode() {
            return MasterPhase::SafeMode;
        }
        if quarantined {
            return MasterPhase::Quarantined;
        }

        let caught_up = self.progress.applied() >= self.progress.commit();
        match role {
            RoleState::Leader if self.serving_gate(caught_up) => MasterPhase::Serving,
            RoleState::Leader => MasterPhase::CatchingUp,
            RoleState::Follower if caught_up => MasterPhase::StandingBy,
            RoleState::Follower => MasterPhase::Replaying,
            _ if caught_up => MasterPhase::Starting,
            _ => MasterPhase::Replaying,
        }
    }

    pub(crate) fn recovery(&self) -> RecoveryGate {
        self.recovery.clone()
    }

    pub fn is_safe_mode(&self) -> bool {
        self.recovery.is_safe_mode()
    }

    /// Why safe mode was entered, if it was.
    pub fn safe_mode_reason(&self) -> Option<String> {
        self.recovery.last_error()
    }

    pub fn ufs_caught_up(&self) -> bool {
        self.recovery.ufs_caught_up()
    }

    pub fn ufs_pending(&self) -> u64 {
        self.recovery.pending()
    }

    /// Raft term of the last persisted hard state; 0 before the first one.
    pub fn term(&self) -> u64 {
        self.term.load(Ordering::Acquire)
    }

    /// Test-only term bump, mirroring [`MasterMonitor::set_role_for_test`].
    #[doc(hidden)]
    pub fn set_term_for_test(&self, term: u64) {
        self.term.store(term, Ordering::Release);
    }

    // Determine whether the current node is an active node.
    // The journal is at the active node of the leader, which is the master.
    pub fn is_active(&self) -> bool {
        let cur: RoleState = self.journal_ctl.state();
        cur == RoleState::Leader
    }

    /// Raw raft role, without the `Init -> Exit` collapse of [`Self::journal_state`].
    /// Liveness needs to tell "role not known yet" (replaying, still alive)
    /// apart from "the raft node stopped".
    pub fn journal_role(&self) -> RoleState {
        self.journal_ctl.state()
    }

    pub fn journal_state(&self) -> MasterState {
        let s: RoleState = self.journal_ctl.state();
        match s {
            RoleState::Leader => MasterState::Active,
            RoleState::Follower => MasterState::Standby,
            _ => MasterState::Exit,
        }
    }

    /// Test-only role switch, mirroring `JournalSystem::fs_only_for_test`.
    #[doc(hidden)]
    pub fn set_role_for_test(&self, state: curvine_raft::raft::RoleState) {
        self.journal_ctl.set_state(state);
    }

    pub fn is_stop(&self) -> bool {
        let cur: RoleState = self.journal_ctl.state();
        cur == RoleState::Exit
    }

    /// Integration tests without a live journal use this to simulate leader /
    /// follower transitions for dir-space scheduling.
    #[doc(hidden)]
    pub fn set_journal_role_for_test(&self, role: RoleState) {
        self.journal_ctl.set_state(role);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn monitor(role: RoleState) -> MasterMonitor {
        MasterMonitor::new(StateCtl::new(role.into()), StateCtl::new(0))
    }

    fn with_watermarks(monitor: &MasterMonitor, applied: u64, commit: u64) {
        monitor.progress().set_watermarks(applied, applied);
        monitor.progress().set_commit(commit);
    }

    #[test]
    fn phase_reports_the_stopped_process_last() {
        let monitor = monitor(RoleState::Exit);
        monitor.recovery.enter_safe_mode("damaged");
        assert_eq!(monitor.phase(true), MasterPhase::Exiting);
    }

    #[test]
    fn phase_prefers_safe_mode_and_quarantine_over_role() {
        let monitor = monitor(RoleState::Leader);
        with_watermarks(&monitor, 10, 10);
        assert_eq!(monitor.phase(true), MasterPhase::Quarantined);

        monitor.recovery.enter_safe_mode("journal entry corrupt");
        assert_eq!(monitor.phase(true), MasterPhase::SafeMode);
        assert_eq!(monitor.phase(false), MasterPhase::SafeMode);
    }

    /// A serving leader always has one entry in flight between commit and
    /// apply; that must not flip it back to CatchingUp and flap /readyz.
    #[test]
    fn a_serving_leader_keeps_serving_through_in_flight_lag() {
        let monitor = monitor(RoleState::Leader);
        monitor.progress().set_commit(100);

        monitor.progress().set_watermarks(99, 99);
        assert_eq!(monitor.phase(false), MasterPhase::CatchingUp);

        monitor.progress().set_watermarks(100, 100);
        assert_eq!(monitor.phase(false), MasterPhase::Serving);

        // The next proposal is committed before it is applied.
        monitor.progress().set_commit(101);
        monitor.progress().set_watermarks(100, 100);
        assert_eq!(monitor.phase(false), MasterPhase::Serving);

        monitor.progress().set_watermarks(101, 101);
        assert_eq!(monitor.phase(false), MasterPhase::Serving);
    }

    #[test]
    fn a_new_term_re_arms_the_promotion_gate() {
        let monitor = monitor(RoleState::Leader);
        monitor.progress().set_commit(100);

        monitor.progress().set_watermarks(100, 100);
        assert_eq!(monitor.phase(false), MasterPhase::Serving);

        // Lost the election and won a new one: the gate must close again.
        monitor.set_term_for_test(2);
        monitor.progress().set_commit(150);
        monitor.progress().set_watermarks(100, 100);
        assert_eq!(monitor.phase(false), MasterPhase::CatchingUp);

        monitor.progress().set_watermarks(150, 150);
        assert_eq!(monitor.phase(false), MasterPhase::Serving);
    }

    #[test]
    fn leader_splits_on_the_committed_watermark() {
        let monitor = monitor(RoleState::Leader);

        with_watermarks(&monitor, 90, 100);
        assert_eq!(monitor.phase(false), MasterPhase::CatchingUp);

        with_watermarks(&monitor, 100, 100);
        assert_eq!(monitor.phase(false), MasterPhase::Serving);
    }

    #[test]
    fn follower_replays_until_it_reaches_the_commit() {
        let monitor = monitor(RoleState::Follower);

        with_watermarks(&monitor, 0, 100);
        assert_eq!(monitor.phase(false), MasterPhase::Replaying);

        with_watermarks(&monitor, 100, 100);
        assert_eq!(monitor.phase(false), MasterPhase::StandingBy);
    }

    #[test]
    fn no_role_yet_is_starting_or_replaying() {
        let monitor = monitor(RoleState::Init);

        with_watermarks(&monitor, 0, 0);
        assert_eq!(monitor.phase(false), MasterPhase::Starting);

        with_watermarks(&monitor, 0, 7);
        assert_eq!(monitor.phase(false), MasterPhase::Replaying);
    }

    #[test]
    fn phase_names_are_stable() {
        // HTTP payloads, CLI output and dashboards key on these strings.
        let all = [
            (MasterPhase::Starting, "Starting"),
            (MasterPhase::Replaying, "Replaying"),
            (MasterPhase::CatchingUp, "CatchingUp"),
            (MasterPhase::Serving, "Serving"),
            (MasterPhase::StandingBy, "StandingBy"),
            (MasterPhase::Quarantined, "Quarantined"),
            (MasterPhase::SafeMode, "SafeMode"),
            (MasterPhase::Exiting, "Exiting"),
        ];
        for (phase, name) in all {
            assert_eq!(phase.as_str(), name);
            assert!(i8::from(phase) >= 1);
        }
    }

    #[test]
    fn observe_phase_stamps_transitions_only() {
        let view = ProgressView::new();
        assert_eq!(view.phase_entered_ms(), 0);

        assert_eq!(view.observe_phase(MasterPhase::Replaying, 1_000), 1_000);
        // The same phase keeps the original stamp.
        assert_eq!(view.observe_phase(MasterPhase::Replaying, 2_000), 1_000);

        assert_eq!(view.observe_phase(MasterPhase::Serving, 3_000), 3_000);
        assert_eq!(view.observe_phase(MasterPhase::Serving, 4_000), 3_000);
    }

    #[test]
    fn rate_needs_two_observations_of_progress() {
        let view = ProgressView::new();
        assert!(view.record_sample(1_000).is_some(), "first sample exists");

        view.set_watermarks(100, 0);
        let rate = view.record_sample(2_000).expect("second sample");
        assert_eq!(rate.entries_per_sec, 100.0);
        assert_eq!(rate.samples, 2);

        view.set_watermarks(300, 0);
        let rate = view.record_sample(3_000).expect("third sample");
        // 0 -> 300 entries over the 2s window.
        assert_eq!(rate.entries_per_sec, 150.0);
        assert_eq!(rate.samples, 3);
    }

    /// A finished replay must not keep reporting its old rate: the window
    /// anchor is not a reason to claim progress that stopped happening.
    #[test]
    fn rate_falls_to_zero_when_progress_stops() {
        let view = ProgressView::new();
        view.set_watermarks(100, 100);
        view.record_sample(1_000);
        view.set_watermarks(300, 300);
        let rate = view.record_sample(2_000).expect("sample");
        assert!(rate.entries_per_sec > 0.0);

        let idle = view.record_sample(2_000 + RATE_WINDOW_MS).expect("sample");
        assert_eq!(idle.entries_per_sec, 0.0);
    }

    #[test]
    fn rate_drops_observations_older_than_the_window() {
        let view = ProgressView::new();
        view.set_watermarks(0, 0);
        view.record_sample(0);
        view.set_watermarks(10, 0);
        view.record_sample(10_000);
        view.set_watermarks(20, 0);

        // Everything before 70s is evicted once we observe at 80s.
        let rate = view.record_sample(80_000).expect("sample inside window");
        assert_eq!(rate.samples, 2);
        assert_eq!(rate.entries_per_sec, 10.0 / 70.0);
    }

    #[test]
    fn repeated_observations_in_one_millisecond_do_not_skew_the_window() {
        let view = ProgressView::new();
        view.record_sample(1_000);
        view.set_watermarks(50, 0);
        view.record_sample(1_000);
        let rate = view.record_sample(1_000).expect("same-millisecond sample");
        assert_eq!(rate.samples, 1, "one timestamp, one sample");
        assert_eq!(rate.entries_per_sec, 0.0);
    }

    #[test]
    fn stall_is_reported_from_the_loader_clock_not_from_scrapes() {
        let view = ProgressView::new();
        assert!(!view.stalled(10_000), "no progress yet is not a stall");

        view.set_watermarks(10, 0);
        let last = view.last_progress_ms();
        assert!(last > 0);
        assert!(!view.stalled(last + STALL_AFTER_MS - 1));
        assert!(view.stalled(last + STALL_AFTER_MS));
    }

    #[test]
    fn progress_view_reports_ufs_lag_from_watermarks() {
        let view = ProgressView::new();
        view.set_watermarks(100, 60);
        assert_eq!(view.ufs_lag(), 40);

        // A standby never advances the UFS watermark.
        view.set_watermarks(100, 0);
        assert_eq!(view.ufs_lag(), 100);

        // Applied behind UFS must not underflow.
        view.set_watermarks(10, 100);
        assert_eq!(view.ufs_lag(), 0);
    }
}
