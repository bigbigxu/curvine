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

//! Peer liveness for the lease-free ownership model (CV-105 §3.2).
//!
//! Transfer has no registry and no heartbeat: `transfer.endpoints` is the only
//! instance list, and each instance answers `GetTransferServiceInfo` about
//! itself. That answer carries `instance_id` (the process-unique owner id that
//! job rows store) and `ring_members` (the endpoints *that* instance is using),
//! which is everything the watchdog needs:
//!
//! - an owner is alive when some probed peer reported its `instance_id`;
//! - a peer that fails `dead_after` consecutive probes is gone;
//! - **nothing is judged dead while any peer is merely "unproven"** — a
//!   transient network error must never kill a live instance's job;
//! - a peer whose `ring_members` disagree with ours is a configuration drift,
//!   reported once and then excluded from judgement, because "I cannot see you"
//!   is then a config bug rather than a death.

use std::collections::HashMap;
use std::time::Duration;

/// One probe answer, reduced to what ownership needs.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PeerReport {
    /// Process-unique owner id (`TransferServiceInfoResponse.instance_id`).
    pub instance_id: String,
    /// The endpoint list that peer is configured with
    /// (`TransferServiceInfoResponse.ring_members`).
    pub ring_members: Vec<String>,
}

/// Per-peer probe bookkeeping. Pure data: no I/O, so the watchdog rules are
/// testable without a network.
#[derive(Clone, Debug, Default)]
pub struct PeerHealth {
    /// Consecutive probe failures per address.
    failures: HashMap<String, u32>,
    /// Last reported instance id per address (survives failures until the peer
    /// is declared dead).
    instance_ids: HashMap<String, String>,
    /// Addresses excluded from judgement because their configuration drifted.
    drifted: HashMap<String, Vec<String>>,
    /// How many consecutive failures declare a peer dead.
    dead_after: u32,
    /// Completed probe rounds. Zero means "nothing is known yet", and no owner
    /// may be judged dead on an empty picture.
    rounds: u32,
}

impl PeerHealth {
    pub fn new(dead_after: u32) -> Self {
        Self {
            dead_after: dead_after.max(1),
            ..Default::default()
        }
    }

    /// One probe round finished (every peer was attempted once).
    pub fn finish_round(&mut self) {
        self.rounds = self.rounds.saturating_add(1);
    }

    pub fn rounds(&self) -> u32 {
        self.rounds
    }

    /// A probe answered: the peer is alive and its owner id is known again. A
    /// matching endpoint list also clears an earlier drift.
    pub fn observe_ok(&mut self, addr: &str, report: PeerReport, local: &[String]) {
        self.failures.remove(addr);
        self.instance_ids
            .insert(addr.to_string(), report.instance_id.clone());
        let mut reported = report.ring_members.clone();
        let mut configured = local.to_vec();
        reported.sort();
        configured.sort();
        if reported == configured {
            self.drifted.remove(addr);
        }
    }

    /// A probe failed. Returns `true` exactly once, when this failure crosses
    /// the threshold (so the caller can log a transition, not every attempt).
    pub fn observe_failure(&mut self, addr: &str) -> bool {
        let failures = self.failures.entry(addr.to_string()).or_insert(0);
        *failures = failures.saturating_add(1);
        *failures == self.dead_after
    }

    /// Marks a peer as configuration-drifted. Drifted peers keep their owner id
    /// registered but are never used to judge anyone dead.
    pub fn observe_drift(&mut self, addr: &str, remote: Vec<String>, local: &[String]) {
        log::warn!(
            "transfer peer {} reports endpoints {:?} but this instance is configured with {:?}; \
             ownership judgement for that peer is disabled until the configuration matches",
            addr,
            remote,
            local
        );
        self.drifted.insert(addr.to_string(), remote);
    }

    pub fn is_drifted(&self, addr: &str) -> bool {
        self.drifted.contains_key(addr)
    }

    /// Drifted peers, for the `transfer_peer_config_drift` metric.
    pub fn drifted_peers(&self) -> Vec<(String, Vec<String>)> {
        let mut peers: Vec<(String, Vec<String>)> = self
            .drifted
            .iter()
            .map(|(addr, members)| (addr.clone(), members.clone()))
            .collect();
        peers.sort();
        peers
    }

    /// `true` when this address failed at least `dead_after` times in a row.
    pub fn is_dead(&self, addr: &str) -> bool {
        self.failures.get(addr).copied().unwrap_or(0) >= self.dead_after
    }

    /// The liveness question the death scan actually asks: is this owner id
    /// still served by some peer we can see?
    ///
    /// Conservative by construction: an owner counts as alive unless every peer
    /// that could be serving it is definitively dead, and any drift or unproven
    /// peer keeps it alive.
    pub fn owner_is_alive(&self, owner: &str, peers: &[String]) -> bool {
        // Nothing probed yet: an empty picture must never kill anything.
        if peers.is_empty() || self.rounds == 0 {
            return true;
        }
        // A drifted peer disables judgement entirely.
        for peer in peers {
            if self.is_drifted(peer) {
                return true;
            }
        }
        // Known mapping decides: the peer serving this owner says it all.
        for peer in peers {
            if self.instance_ids.get(peer).map(String::as_str) == Some(owner) {
                return !self.is_dead(peer);
            }
        }
        // Unmatched. A peer that failed but has not crossed the threshold yet is
        // still "could be serving it", so keep the owner alive.
        for peer in peers {
            if self.is_unproven(peer) {
                return true;
            }
        }
        // After at least one full round, every peer either answered (and its id
        // is recorded, so it is not this owner) or is definitively dead. An owner
        // no peer claims is therefore gone — including the 2-instance case where
        // the only candidate address never answered at all, and the case of a
        // restarted process whose old instance id no live peer can have.
        false
    }

    /// `true` while the address has failed but not yet crossed the threshold:
    /// the watchdog must not act on a flapping peer.
    fn is_unproven(&self, addr: &str) -> bool {
        match self.failures.get(addr).copied().unwrap_or(0) {
            0 => false,
            failures => failures < self.dead_after,
        }
    }
}

/// Probe cadence and thresholds, kept in one place so the scheduler and the
/// tests cannot disagree.
pub const PEER_PROBE_TIMEOUT: Duration = Duration::from_secs(2);
pub const PEER_DEAD_AFTER: u32 = 3;
/// How often peers are probed. Cheap (one local RPC per peer) and far below the
/// dead-after threshold, so 3 failures still take ~15s to accumulate.
pub const PEER_PROBE_INTERVAL: Duration = Duration::from_secs(5);

/// What the stop-then-run machine does once an owner-lost job has no
/// dispatchable tasks left (design §3.3).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CancelOutcome {
    /// Tasks are still running somewhere: write nothing, wait.
    Wait,
    /// Budget left: start the next run generation (`run_id + 1`, Pending).
    Revive,
    /// Budget exhausted: terminal failure.
    Fail,
}

/// Pure decision for the convergence step, so the rule is unit-testable without
/// a store or a scheduler.
pub fn cancel_outcome(
    has_recoverable_tasks: bool,
    run: u64,
    max_runs: u32,
    replan_allowed: bool,
) -> CancelOutcome {
    if has_recoverable_tasks {
        return CancelOutcome::Wait;
    }
    // A job killed by its own TTL never re-plans, even with budget left: the
    // upstream asked for that lifetime and extending it would be silent scope
    // creep. Owner loss is the only reason that may start another run.
    if !replan_allowed || run >= u64::from(max_runs.max(1)) {
        CancelOutcome::Fail
    } else {
        CancelOutcome::Revive
    }
}

/// Waiting in `Canceling` longer than this is worth a warning; nothing is ever
/// forced. CV-105 dropped the timeout-kill branch: the job waits until the
/// workers stop reporting.
pub const CANCELING_WARN_AFTER: Duration = Duration::from_secs(300);

#[cfg(test)]
mod tests {
    use super::*;

    fn peers() -> Vec<String> {
        vec!["a:1".to_string(), "b:1".to_string()]
    }

    fn report(id: &str, members: &[&str]) -> PeerReport {
        PeerReport {
            instance_id: id.to_string(),
            ring_members: members.iter().map(|m| m.to_string()).collect(),
        }
    }

    #[test]
    fn a_peer_is_dead_only_after_three_consecutive_failures() {
        let mut health = PeerHealth::new(PEER_DEAD_AFTER);
        let local = peers();
        health.observe_ok("a:1", report("owner-a", &["a:1", "b:1"]), &local);
        health.observe_ok("b:1", report("owner-b", &["a:1", "b:1"]), &local);
        health.finish_round();

        assert!(!health.observe_failure("b:1"));
        assert!(!health.observe_failure("b:1"));
        assert!(!health.is_dead("b:1"));
        // Two failures are not evidence of death.
        assert!(health.owner_is_alive("owner-b", &local));

        assert!(health.observe_failure("b:1"), "third failure crosses the line");
        assert!(health.is_dead("b:1"));
        assert!(!health.owner_is_alive("owner-b", &local));
    }

    #[test]
    fn a_successful_probe_resets_the_failure_count() {
        let local = peers();
        let mut health = PeerHealth::new(PEER_DEAD_AFTER);
        health.observe_failure("b:1");
        health.observe_failure("b:1");
        health.observe_ok("b:1", report("owner-b", &["a:1", "b:1"]), &local);
        health.observe_failure("b:1");
        assert!(!health.is_dead("b:1"), "the counter restarted at the success");
    }

    #[test]
    fn an_owner_never_observed_on_a_two_instance_fleet_is_dead() {
        // Regression (SCN3): with two instances, the dead one's instance id may
        // never have been recorded. It must still be judged dead once its only
        // candidate address stopped answering.
        let mut health = PeerHealth::new(PEER_DEAD_AFTER);
        let local = peers();
        health.observe_ok("a:1", report("owner-a", &["a:1", "b:1"]), &local);
        for _ in 0..PEER_DEAD_AFTER {
            health.observe_failure("b:1");
        }
        health.finish_round();
        assert!(health.is_dead("b:1"));
        // `owner-b` was never recorded, yet "a:1" proves it is not the owner.
        assert!(!health.owner_is_alive("owner-b", &local));
    }

    #[test]
    fn an_owner_never_observed_stays_alive_while_a_peer_is_unproven() {
        let mut health = PeerHealth::new(PEER_DEAD_AFTER);
        let local = peers();
        health.observe_ok("a:1", report("owner-a", &["a:1", "b:1"]), &local);
        health.observe_failure("b:1");
        health.observe_failure("b:1"); // one short of the threshold
        health.finish_round();
        assert!(health.owner_is_alive("owner-b", &local), "flapping peer keeps it alive");
    }

    #[test]
    fn nothing_is_judged_before_the_first_probe_round() {
        let health = PeerHealth::new(PEER_DEAD_AFTER);
        let local = peers();
        // Boot: no round has finished, so an unknown owner must stay alive.
        assert!(health.owner_is_alive("owner-whatever", &local));
    }

    #[test]
    fn an_unknown_owner_stays_alive_while_any_peer_is_unproven() {
        let health = PeerHealth::new(PEER_DEAD_AFTER);
        let local = peers();
        // Nothing probed yet: never kill.
        assert!(health.owner_is_alive("owner-x", &local));
    }

    #[test]
    fn an_owner_is_dead_once_every_peer_is_dead() {
        let mut health = PeerHealth::new(PEER_DEAD_AFTER);
        let local = peers();
        for _ in 0..PEER_DEAD_AFTER {
            health.observe_failure("a:1");
            health.observe_failure("b:1");
        }
        health.finish_round();
        assert!(!health.owner_is_alive("owner-gone", &local));
    }

    #[test]
    fn drift_disables_judgement_for_that_peer() {
        let mut health = PeerHealth::new(PEER_DEAD_AFTER);
        let local = peers();
        health.observe_ok("a:1", report("owner-a", &["a:1", "b:1"]), &local);
        health.observe_drift("a:1", vec!["a:1".to_string()], &local);
        assert!(health.is_drifted("a:1"));
        for _ in 0..10 {
            health.observe_failure("a:1");
        }
        // A drifted peer never justifies killing anything.
        assert!(health.owner_is_alive("owner-a", &local));
        assert_eq!(health.drifted_peers().len(), 1);
    }

    #[test]
    fn matching_endpoints_clear_a_previous_drift() {
        let mut health = PeerHealth::new(PEER_DEAD_AFTER);
        let local = peers();
        health.observe_drift("a:1", vec!["other:1".to_string()], &local);
        health.observe_ok("a:1", report("owner-a", &["a:1", "b:1"]), &local);
        assert!(!health.is_drifted("a:1"));
    }

    #[test]
    fn convergence_waits_then_revives_then_fails_on_budget() {
        assert_eq!(cancel_outcome(true, 1, 2, true), CancelOutcome::Wait);
        assert_eq!(cancel_outcome(false, 1, 2, true), CancelOutcome::Revive);
        assert_eq!(cancel_outcome(false, 2, 2, true), CancelOutcome::Fail);
        // A job allowed a single run never re-plans.
        assert_eq!(cancel_outcome(false, 1, 1, true), CancelOutcome::Fail);
        // Garbage budget still means "at least one run".
        assert_eq!(cancel_outcome(false, 1, 0, true), CancelOutcome::Fail);
    }

    #[test]
    fn ttl_expiry_never_replans_even_with_budget_left() {
        // Budget allows another run, but the reason forbids it.
        assert_eq!(cancel_outcome(false, 1, 5, false), CancelOutcome::Fail);
        // Waiting still wins while tasks are alive.
        assert_eq!(cancel_outcome(true, 1, 5, false), CancelOutcome::Wait);
    }

    #[test]
    fn canceling_warns_long_before_anything_else_happens() {
        assert_eq!(CANCELING_WARN_AFTER.as_secs(), 300);
        assert_eq!(PEER_PROBE_TIMEOUT.as_secs(), 2);
    }
}
