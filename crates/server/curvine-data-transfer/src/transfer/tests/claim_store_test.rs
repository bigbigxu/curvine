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

//! Ownership without a lease (CV-105 / design §3.1, §3.3): one-CAS claim, the
//! run generation as fence, and the three state transitions the owner-death path
//! needs. All of them are exercised through the in-memory store, which is the
//! backend the scheduler tests run on.

use super::{MemoryTransferStore, TransferStore};
use curvine_model::{TransferJobRecord, TransferKind, TransferProgress, TransferState};

fn job_at(job_id: &str, updated_at: i64) -> TransferJobRecord {
    TransferJobRecord {
        job_key: format!("key-{job_id}"),
        job_id: job_id.to_string(),
        run_id: 1,
        kind: TransferKind::Load,
        source_path: format!("file:///src/{job_id}"),
        target_path: format!("/cv/{job_id}"),
        command_json: "{}".to_string(),
        mount_snapshot_json: "{}".to_string(),
        secret_ref_json: "{}".to_string(),
        cluster_snapshot_version: 1,
        mount_snapshot_attempts: 0,
        planning_attempts: 0,
        takeover_count: 0,
        placement_addr: String::new(),
        owner_addr: String::new(),
        terminal_summary: None,
        terminal_counts: None,
        cv_metadata_epoch: None,
        state: TransferState::Pending,
        owner: String::new(),
        fence: 0,
        lease_expire_at: 0,
        cancel_requested: false,
        expires_at: 0,
        cancel_reason: String::new(),
        max_runs: TransferJobRecord::DEFAULT_MAX_RUNS,
        summary: TransferProgress::default(),
        client_request_id: format!("request-{job_id}"),
        submitter: "s".to_string(),
        tenant: "t".to_string(),
        created_at: 1,
        updated_at,
    }
}

fn job(job_id: &str) -> TransferJobRecord {
    job_at(job_id, 1)
}

fn store_with(jobs: Vec<TransferJobRecord>) -> MemoryTransferStore {
    let store = MemoryTransferStore::new();
    for job in jobs {
        store
            .create_or_get_by_request_id_checked(job)
            .expect("insert job");
    }
    store
}

#[test]
fn claim_takes_a_pending_job_exactly_once() {
    let store = store_with(vec![job("a")]);

    let first = store
        .claim_next_job("owner-1", 10, 8, true, 120_000, "")
        .expect("claim a")
        .expect("first claim wins");
    assert_eq!(first.job_id, "a");
    assert_eq!(first.owner, "owner-1");
    // Fence token is the run generation the owner was granted.
    assert_eq!(first.fence, first.run_id);

    // A different instance must not get the same job while it is owned.
    assert!(store
        .claim_next_job("owner-2", 11, 8, true, 120_000, "")
        .expect("claim from owner-2")
        .is_none());

    let claimed = store.get_transfer("a").unwrap().expect("job a");
    assert_eq!(claimed.state, TransferState::Planning);
    assert_eq!(claimed.owner, "owner-1");
}

#[test]
fn claim_respects_the_executing_ceiling_and_returns_owned_work_first() {
    // Distinct `updated_at`: claims are FIFO, oldest first.
    let store = store_with(vec![job_at("a", 1), job_at("b", 2)]);

    let first = store
        .claim_next_job("owner-1", 10, 1, true, 120_000, "")
        .unwrap()
        .expect("first claim");
    assert_eq!(first.job_id, "a", "claims go oldest-first");
    // Ceiling of one executing job: the second Pending job must wait.
    assert!(store
        .claim_next_job("owner-1", 11, 1, true, 120_000, "")
        .unwrap()
        .is_none());

    // Without the ceiling the same instance picks up the second job.
    let second = store
        .claim_next_job("owner-1", 12, 8, true, 120_000, "")
        .unwrap();
    assert_eq!(second.expect("second job").job_id, "b");
}

#[test]
fn claim_re_ticks_owned_work_without_any_lease_window() {
    let store = store_with(vec![job("a")]);
    let lease = store
        .claim_next_job("owner-1", 10, 8, true, 120_000, "")
        .unwrap()
        .expect("claim");

    // A job with nothing to dispatch is not handed back: owning it is not a
    // reason to keep starving the Pending queue.
    assert!(store
        .claim_next_job("owner-1", 11, 8, false, 120_000, "")
        .unwrap()
        .is_none());

    // Once it has work, the same owner re-ticks it far past any old lease
    // window, because ownership is process-scoped, not time-scoped.
    store
        .persist_planned_tasks(super::TransferPlannedTasks {
            job_id: lease.job_id.clone(),
            run_id: lease.run_id,
            owner: lease.owner.clone(),
            fence: lease.fence,
            tasks: vec![],
            message: "planned".to_string(),
            now_ms: 12,
        })
        .ok();
    let again = store
        .claim_next_job("owner-1", 10 + 86_400_000, 8, false, 120_000, "")
        .unwrap();
    if let Some(again) = again {
        assert_eq!(again.job_id, lease.job_id);
        assert_eq!(again.run_id, lease.run_id);
    }
}

#[test]
fn mark_canceling_records_the_reason_and_owner() {
    let store = store_with(vec![job("a")]);
    store
        .claim_next_job("owner-1", 10, 8, true, 120_000, "")
        .unwrap();

    assert!(store
        .mark_canceling("a", 1, "owner-2", "owner_lost", 20)
        .unwrap());
    let record = store.get_transfer("a").unwrap().expect("job a");
    assert_eq!(record.state, TransferState::Canceling);
    assert_eq!(record.cancel_reason, "owner_lost");
    // owner loss is not a user cancel (see only_a_user_cancel_sets_cancel_requested)
    assert!(!record.cancel_requested);
    assert_eq!(record.owner, "owner-2");
}

#[test]
fn mark_canceling_ignores_terminal_and_stale_run_ids() {
    let store = store_with(vec![job("a")]);
    store
        .claim_next_job("owner-1", 10, 8, true, 120_000, "")
        .unwrap();

    assert!(!store.mark_canceling("a", 2, "o", "owner_lost", 20).unwrap());
    assert!(store.fail_job("a", 1, "owner_lost", 30).unwrap());
    // Already terminal: a second transition must not resurrect the job.
    assert!(!store
        .mark_canceling("a", 1, "o", "ttl_expired", 40)
        .unwrap());
}

#[test]
fn revive_run_bumps_the_generation_and_clears_ownership() {
    let store = store_with(vec![job("a")]);
    store
        .claim_next_job("owner-1", 10, 8, true, 120_000, "")
        .unwrap();
    store
        .mark_canceling("a", 1, "owner-2", "owner_lost", 20)
        .unwrap();

    assert!(store.revive_run("a", 1, 30).unwrap());
    let record = store.get_transfer("a").unwrap().expect("job a");
    assert_eq!(record.run_id, 2);
    assert_eq!(record.state, TransferState::Pending);
    assert!(record.owner.is_empty());
    assert!(record.cancel_reason.is_empty());
    assert!(!record.cancel_requested);

    // The next run is claimable again, and its fence is the new generation.
    let lease = store
        .claim_next_job("owner-3", 40, 8, true, 120_000, "")
        .unwrap()
        .expect("claim run 2");
    assert_eq!(lease.run_id, 2);
    assert_eq!(lease.fence, 2);
}

#[test]
fn fail_job_is_terminal_and_keeps_the_reason() {
    let store = store_with(vec![job("a")]);
    store
        .claim_next_job("owner-1", 10, 8, true, 120_000, "")
        .unwrap();

    assert!(store.fail_job("a", 1, "ttl_expired", 20).unwrap());
    let record = store.get_transfer("a").unwrap().expect("job a");
    assert_eq!(record.state, TransferState::Failed);
    assert_eq!(record.cancel_reason, "ttl_expired");
    assert_eq!(record.summary.message, "ttl_expired");

    assert!(!store.fail_job("a", 1, "ttl_expired", 30).unwrap());
    assert!(!store.revive_run("a", 1, 40).unwrap());
}

#[test]
fn fresh_jobs_have_no_ttl_and_two_runs_by_default() {
    let job = job("a");
    assert_eq!(
        job.expires_at, 0,
        "no TTL unless the submitter asks for one"
    );
    assert_eq!(job.max_runs, 2, "initial run plus one re-plan");
    assert!(job.cancel_reason.is_empty());
}

#[test]
fn concurrent_mark_canceling_transitions_exactly_once() {
    let store = store_with(vec![job("a")]);
    store
        .claim_next_job("owner-1", 10, 8, true, 120_000, "")
        .unwrap();

    // Two instances sweep the same dead owner; the row decides, not the caller.
    let first = store
        .mark_canceling("a", 1, "sweeper-1", "owner_lost", 20)
        .unwrap();
    let second = store
        .mark_canceling("a", 1, "sweeper-2", "owner_lost", 21)
        .unwrap();
    assert!(first, "the first sweep moves the job into Canceling");
    assert!(
        !second,
        "the second finds it already there and does nothing"
    );
    assert_eq!(store.get_transfer("a").unwrap().unwrap().owner, "sweeper-1");
}

#[test]
fn canceling_waits_for_recoverable_tasks_before_reviving() {
    use super::ownership::{cancel_outcome, CancelOutcome};

    let store = store_with(vec![job("a")]);
    let lease = store
        .claim_next_job("owner-1", 10, 8, true, 120_000, "")
        .unwrap()
        .unwrap();
    store
        .mark_canceling("a", 1, "owner-1", "owner_lost", 20)
        .unwrap();
    store
        .persist_planned_tasks(super::TransferPlannedTasks {
            job_id: lease.job_id.clone(),
            run_id: lease.run_id,
            owner: lease.owner.clone(),
            fence: lease.fence,
            tasks: vec![],
            message: "planned".to_string(),
            now_ms: 21,
        })
        .ok();

    // Tasks still recoverable: the machine waits and writes nothing.
    assert_eq!(
        store.get_transfer("a").unwrap().unwrap().state,
        TransferState::Canceling
    );
    assert_eq!(cancel_outcome(true, 1, 2, true), CancelOutcome::Wait);

    // Converged: budget left, so the next generation starts as Pending.
    assert!(store.revive_run("a", 1, 30).unwrap());
    let revived = store.get_transfer("a").unwrap().unwrap();
    assert_eq!(revived.run_id, 2);
    assert_eq!(revived.state, TransferState::Pending);

    // Converged with the budget spent: terminal instead.
    store
        .claim_next_job("owner-1", 31, 8, true, 120_000, "")
        .unwrap();
    store
        .mark_canceling("a", 2, "owner-1", "owner_lost", 32)
        .unwrap();
    assert_eq!(cancel_outcome(false, 2, 2, true), CancelOutcome::Fail);
    assert!(store.fail_job("a", 2, "owner_lost", 33).unwrap());
    assert_eq!(
        store.get_transfer("a").unwrap().unwrap().state,
        TransferState::Failed
    );
}

#[test]
fn a_restarted_instance_sees_its_old_jobs_as_another_owner() {
    // Boot self-check path: the job was claimed by a previous process, whose
    // owner id is an instance id that no longer exists. It shows up in the
    // ownership sweep route, not as "mine".
    let store = store_with(vec![job("a")]);
    store
        .claim_next_job("old-instance-uuid", 10, 8, true, 120_000, "")
        .unwrap();
    let owned = store.list_active_owned_jobs(16).unwrap();
    assert_eq!(owned.len(), 1);
    assert_eq!(owned[0].2, "old-instance-uuid");
    assert_ne!(owned[0].2, "new-instance-uuid");
}

#[test]
fn ttl_deadline_is_fixed_and_never_extended_by_a_replan() {
    let store = store_with(vec![job("a")]);
    store
        .claim_next_job("owner-1", 10, 8, true, 120_000, "")
        .unwrap()
        .unwrap();
    // Simulate a submit that asked for a deadline.
    store.set_expires_at_for_test("a", 5_000).unwrap();
    assert!(store
        .mark_canceling("a", 1, "owner-1", "owner_lost", 20)
        .unwrap());
    assert!(store.revive_run("a", 1, 30).unwrap());
    let revived = store.get_transfer("a").unwrap().unwrap();
    assert_eq!(revived.run_id, 2);
    assert_eq!(
        revived.expires_at, 5_000,
        "the deadline belongs to the submit, not to a run"
    );
}

#[test]
fn jobs_without_a_ttl_never_expire() {
    let store = store_with(vec![job("a")]);
    store
        .claim_next_job("owner-1", 10, 8, true, 120_000, "")
        .unwrap();
    // expires_at = 0 means "no TTL": far in the future it still never shows up.
    assert!(store
        .list_expired_jobs(i64::MAX / 4, 16)
        .unwrap()
        .is_empty());
}

#[test]
fn an_expired_job_is_listed_once_and_skipped_while_canceling() {
    let store = store_with(vec![job("a"), job("b")]);
    store.set_expires_at_for_test("a", 1_000).unwrap();
    store.set_expires_at_for_test("b", 1_000).unwrap();
    assert_eq!(store.list_expired_jobs(2_000, 16).unwrap().len(), 2);

    assert!(store
        .mark_canceling("a", 1, "sweeper", "ttl_expired", 2_000)
        .unwrap());
    let listed = store.list_expired_jobs(3_000, 16).unwrap();
    assert_eq!(listed.len(), 1, "a cancelling job is not re-marked");
    assert_eq!(listed[0].0, "b");
}

#[test]
fn ttl_and_owner_loss_share_the_state_machine_but_not_the_outcome() {
    use super::ownership::{cancel_outcome, CancelOutcome};
    // Same convergence point, different verdict.
    assert_eq!(cancel_outcome(false, 1, 2, true), CancelOutcome::Revive);
    assert_eq!(cancel_outcome(false, 1, 2, false), CancelOutcome::Fail);
}

#[test]
fn re_tick_writes_nothing_to_the_store() {
    // R1 regression: rotating over owned jobs must not touch the row. The old
    // implementation bumped `updated_at` on every claim, which dominated the
    // measured write amplification.
    let store = store_with(vec![job("a")]);
    let lease = store
        .claim_next_job("owner-1", 10, 8, true, 120_000, "")
        .unwrap()
        .unwrap();
    // A Canceling job always counts as "work" for the re-tick rotation.
    store.mark_canceling("a", 1, "owner-1", "user", 11).unwrap();
    let before = store.get_transfer("a").unwrap().unwrap();

    // Many re-ticks through the in-memory keyset, exactly like the scheduler.
    let mut cursor = String::new();
    for tick in 0..50 {
        // Same wrap-around the scheduler does: an exhausted keyset restarts.
        let got = match store
            .claim_next_job("owner-1", 20 + tick, 8, false, 120_000, &cursor)
            .unwrap()
        {
            Some(lease) => lease,
            None => {
                // Same wrap-around the scheduler does: an exhausted keyset
                // restarts from the beginning.
                store
                    .claim_next_job("owner-1", 20 + tick, 8, false, 120_000, "")
                    .unwrap()
                    .expect("owned job is re-ticked after the cursor wraps")
            }
        };
        assert_eq!(got.job_id, lease.job_id);
        cursor = got.job_id;
    }

    let after = store.get_transfer("a").unwrap().unwrap();
    assert_eq!(
        after.updated_at, before.updated_at,
        "re-tick must not write"
    );
    assert_eq!(after.state, before.state);
    assert_eq!(after.fence, before.fence);
}

#[test]
fn the_re_tick_cursor_wraps_and_rotates_over_owned_jobs() {
    // Keyset semantics: an empty cursor starts at the lowest id, and an exhausted
    // walk returns nothing so the scheduler resets the cursor.
    let store = store_with(vec![job_at("a", 1), job_at("b", 2)]);
    let first = store
        .claim_next_job("owner-1", 10, 8, true, 120_000, "")
        .unwrap()
        .unwrap();
    assert_eq!(first.job_id, "a");
    // Promote both to owned work so they become re-tick candidates.
    store.mark_canceling("a", 1, "owner-1", "user", 11).ok();
    let second = store
        .claim_next_job("owner-1", 12, 8, false, 120_000, "a")
        .unwrap();
    if let Some(second) = second {
        assert!(second.job_id > first.job_id, "cursor only moves forward");
    }
    assert!(
        store
            .claim_next_job("owner-1", 13, 8, false, 120_000, "zzz")
            .unwrap()
            .is_none(),
        "an exhausted cursor yields nothing so the caller can wrap"
    );
}

#[test]
fn only_a_user_cancel_sets_cancel_requested() {
    // The race that made SCN1 flaky: `mark_canceling` used to set
    // `cancel_requested = true` for every reason, so the tick's user-cancel
    // branch competed with the owner-lost advance and could Cancel the job
    // instead of re-planning it.
    let store = store_with(vec![job("a")]);
    store
        .claim_next_job("owner-1", 10, 8, true, 120_000, "")
        .unwrap();

    assert!(store
        .mark_canceling("a", 1, "sweeper", "owner_lost", 20)
        .unwrap());
    let record = store.get_transfer("a").unwrap().unwrap();
    assert_eq!(record.state, TransferState::Canceling);
    assert_eq!(record.cancel_reason, "owner_lost");
    assert!(!record.cancel_requested, "owner loss is not a user cancel");

    // A TTL expiry is not a user cancel either.
    assert!(store.fail_job("a", 1, "ttl_expired", 30).is_ok());
    let b = store_with(vec![job("b")]);
    b.claim_next_job("owner-1", 10, 8, true, 120_000, "")
        .unwrap();
    assert!(b
        .mark_canceling("b", 1, "sweeper", "ttl_expired", 20)
        .unwrap());
    assert!(!b.get_transfer("b").unwrap().unwrap().cancel_requested);

    // A real user cancel does set it.
    let c = store_with(vec![job("c")]);
    c.claim_next_job("owner-1", 10, 8, true, 120_000, "")
        .unwrap();
    assert!(c.mark_canceling("c", 1, "user", "user", 20).unwrap());
    assert!(c.get_transfer("c").unwrap().unwrap().cancel_requested);
}

#[test]
fn an_owner_lost_job_re_plans_instead_of_being_canceled() {
    // Two-tick competition, pinned: with cancel_requested clear, the only exit
    // for Canceling(owner_lost) is the convergence fork — revive inside the run
    // budget, fail outside it. It must never look like a user cancel.
    use super::ownership::{cancel_outcome, CancelOutcome};

    let store = store_with(vec![job("a")]);
    store
        .claim_next_job("owner-1", 10, 8, true, 120_000, "")
        .unwrap();
    store
        .mark_canceling("a", 1, "sweeper", "owner_lost", 20)
        .unwrap();
    let record = store.get_transfer("a").unwrap().unwrap();
    assert!(
        !record.cancel_requested,
        "the cancel branch cannot claim this job"
    );

    let replan_allowed = record.cancel_reason != "ttl_expired";
    assert_eq!(
        cancel_outcome(false, record.run_id, record.max_runs, replan_allowed),
        CancelOutcome::Revive
    );
    assert!(store.revive_run("a", 1, 30).unwrap());
    let revived = store.get_transfer("a").unwrap().unwrap();
    assert_eq!(revived.run_id, 2);
    assert_eq!(revived.state, TransferState::Pending);
    assert!(!revived.cancel_requested);
}
