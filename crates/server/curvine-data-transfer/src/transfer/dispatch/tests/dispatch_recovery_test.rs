use super::super::{now_ms, TransferScheduler, MAX_PLACEMENT_HAND_BACKS};
use crate::common::UfsFactory;
use crate::transfer::memory_store::MemoryTransferStore;
use crate::transfer::plan::TransferPlanner;
use crate::transfer::store::TransferStore;
use crate::transfer::ClusterMetadataCache;
use crate::transfer::{TransferMetrics, TransferRuntimeConfig};
use curvine_client_core::file::CurvineFileSystem;
use curvine_config::{ClusterConf, TransferConf};
use curvine_model::{
    TransferJobRecord, TransferKind, TransferLease, TransferProgress, TransferState,
    TransferTaskRecord, TransferTaskState,
};
use std::sync::Arc;

fn scheduler(store: Arc<MemoryTransferStore>) -> TransferScheduler<MemoryTransferStore> {
    scheduler_with_state(store, Arc::new(TransferRuntimeConfig::with_limit(1)))
}

fn scheduler_with_state(
    store: Arc<MemoryTransferStore>,
    runtime_config: Arc<TransferRuntimeConfig>,
) -> TransferScheduler<MemoryTransferStore> {
    let conf = ClusterConf::default();
    let rt = Arc::new(conf.transfer_server_conf().create_runtime());
    let fs = CurvineFileSystem::with_rt(ClusterConf::default(), rt.clone()).unwrap();
    let cache = ClusterMetadataCache::new(fs.clone());
    let factory = Arc::new(UfsFactory::with_rt(&conf.client, rt));
    let planner = TransferPlanner::new(
        fs.clone(),
        factory.clone(),
        cache.clone(),
        conf.client.clone(),
        runtime_config.clone(),
        1,
        curvine_config::LoadTransferDefaults::from_job(&conf.job),
    );
    TransferScheduler::new(
        store,
        planner,
        cache,
        factory,
        fs,
        "test-owner".to_string(),
        "127.0.0.1:9012".to_string(),
        TransferConf::default(),
        runtime_config,
        std::sync::Arc::new(crate::transfer::TaskCache::new()),
        None,
    )
}

fn running_job(job_id: &str) -> TransferJobRecord {
    TransferJobRecord {
        job_key: format!("key-{job_id}"),
        job_id: job_id.to_string(),
        run_id: 1,
        kind: TransferKind::Load,
        source_path: "file:///source".to_string(),
        target_path: format!("/target/{job_id}"),
        command_json: "{}".to_string(),
        mount_snapshot_json: "{}".to_string(),
        secret_ref_json: String::new(),
        cluster_snapshot_version: 1,
        mount_snapshot_attempts: 0,
        planning_attempts: 0,
        takeover_count: 0,
        placement_addr: String::new(),
        owner_addr: String::new(),
        terminal_summary: None,
        terminal_counts: None,
        cv_metadata_epoch: None,
        state: TransferState::Running,
        owner: "test-owner".to_string(),
        fence: 1,
        lease_expire_at: now_ms().saturating_add(60_000),
        cancel_requested: false,
        expires_at: 0,
        cancel_reason: String::new(),
        max_runs: TransferJobRecord::DEFAULT_MAX_RUNS,
        summary: TransferProgress::default(),
        client_request_id: format!("request-{job_id}"),
        submitter: "test".to_string(),
        tenant: "test".to_string(),
        created_at: 1,
        updated_at: 1,
    }
}

fn task(job_id: &str, task_id: &str, state: TransferTaskState) -> TransferTaskRecord {
    TransferTaskRecord {
        job_id: job_id.to_string(),
        run_id: 1,
        task_id: task_id.to_string(),
        attempt_id: 0,
        source_path: "file:///source".to_string(),
        target_path: format!("/target/{job_id}/{task_id}"),
        worker_id: 0,
        worker_session_id: String::new(),
        source_read_plan_json: String::new(),
        report_target_json: String::new(),
        state,
        progress: TransferProgress::default(),
        estimated_size: 0,
        retry_count: 0,
        attempt_started_at: 0,
        last_report_at: 0,
        stale_deadline_at: 0,
        updated_at: 0,
        accepted_at_ms: 0,
        first_byte_at_ms: 0,
        block_locations: Vec::new(),
        replace_target: false,
    }
}

fn lease(job: &TransferJobRecord) -> TransferLease {
    TransferLease {
        job_id: job.job_id.clone(),
        run_id: job.run_id,
        owner: "test-owner".to_string(),
        fence: 1,
    }
}

#[tokio::test]
async fn pending_redispatch_is_not_starved_by_backoff_failure() {
    let store = Arc::new(MemoryTransferStore::new());
    let job = running_job("pending-redispatch");
    store.create_or_get_by_request_id(job.clone()).unwrap();

    let mut failed = task(&job.job_id, "failed", TransferTaskState::Failed);
    failed.retry_count = 1;
    failed.progress.update_time = now_ms();
    failed.progress.message = "RateLimited by source service".to_string();
    store
        .insert_tasks(vec![
            failed,
            task(&job.job_id, "pending", TransferTaskState::Pending),
        ])
        .unwrap();

    scheduler(store.clone())
        .check_running_transfer(job.clone(), lease(&job))
        .await
        .unwrap();

    let stored = store.get_transfer(&job.job_id).unwrap().unwrap();
    assert!(
        stored
            .summary
            .message
            .contains("waiting for worker capacity"),
        "backoff Failed task must not starve Pending-task redispatch"
    );
}

#[tokio::test]
async fn permanent_failure_is_not_masked_by_backoff_failure() {
    let store = Arc::new(MemoryTransferStore::new());
    let job = running_job("permanent-and-retryable");
    store.create_or_get_by_request_id(job.clone()).unwrap();

    let mut retryable = task(&job.job_id, "retryable", TransferTaskState::Failed);
    retryable.retry_count = 1;
    retryable.progress.update_time = now_ms();
    retryable.progress.message = "RateLimited by source service".to_string();

    let mut permanent = task(&job.job_id, "permanent", TransferTaskState::Failed);
    permanent.progress.message = "transfer task failed".to_string();
    store.insert_tasks(vec![retryable, permanent]).unwrap();

    scheduler(store.clone())
        .check_running_transfer(job.clone(), lease(&job))
        .await
        .unwrap();

    let stored = store.get_transfer(&job.job_id).unwrap().unwrap();
    assert_eq!(stored.state, TransferState::Failed);
}

#[tokio::test]
async fn permanent_failure_beyond_recovery_batch_is_not_masked() {
    let store = Arc::new(MemoryTransferStore::new());
    let job = running_job("terminal-after-batch");
    store.create_or_get_by_request_id(job.clone()).unwrap();

    let now = now_ms();
    let mut tasks = Vec::with_capacity(65);
    for index in 0..TransferConf::DEFAULT_TASK_PROBE_CONCURRENCY {
        let mut retryable = task(
            &job.job_id,
            &format!("retryable-{index:03}"),
            TransferTaskState::Failed,
        );
        retryable.retry_count = 1;
        retryable.updated_at = now.saturating_sub(64 - index as i64);
        retryable.progress.message = "RateLimited by source service".to_string();
        tasks.push(retryable);
    }
    let mut permanent = task(&job.job_id, "terminal", TransferTaskState::Failed);
    permanent.updated_at = now;
    permanent.progress.message = "transfer task failed".to_string();
    tasks.push(permanent);
    store.insert_tasks(tasks).unwrap();

    scheduler(store.clone())
        .check_running_transfer(job.clone(), lease(&job))
        .await
        .unwrap();

    let stored = store.get_transfer(&job.job_id).unwrap().unwrap();
    assert_eq!(stored.state, TransferState::Failed);
}

#[tokio::test]
async fn due_failure_beyond_backoff_batch_is_requeued() {
    let store = Arc::new(MemoryTransferStore::new());
    let job = running_job("due-after-backoff-batch");
    store.create_or_get_by_request_id(job.clone()).unwrap();

    let now = now_ms();
    let mut tasks = Vec::with_capacity(65);
    for index in 0..TransferConf::DEFAULT_TASK_PROBE_CONCURRENCY {
        let mut backoff = task(
            &job.job_id,
            &format!("backoff-{index:03}"),
            TransferTaskState::Failed,
        );
        backoff.retry_count = 1;
        backoff.progress.update_time = now;
        backoff.progress.message = "RateLimited by source service".to_string();
        tasks.push(backoff);
    }
    let mut due = task(&job.job_id, "due", TransferTaskState::Failed);
    due.retry_count = 1;
    due.progress.update_time = now.saturating_sub(60_000);
    due.progress.message = "RateLimited by source service".to_string();
    tasks.push(due);
    store.insert_tasks(tasks).unwrap();

    assert!(scheduler(store.clone())
        .recover_transient_failed_tasks(&job, &lease(&job), now)
        .await
        .unwrap());
    let due = store
        .list_transfer_tasks(&job.job_id, job.run_id)
        .unwrap()
        .into_iter()
        .find(|task| task.task_id == "due")
        .unwrap();
    assert_eq!(due.state, TransferTaskState::Pending);
}

/// The per-job task cap must be read at planning time, not snapshotted at
/// construction: a runtime-config push has to change the very next plan of an
/// already-running transfer role. Mirrors `dispatch_reads_the_live_worker_task_limit`
/// for the planner half of the same state object.
#[test]
fn planner_reads_the_live_task_cap() {
    use crate::transfer::{build_transfer_runtime_registry, MAX_TASKS_PER_TRANSFER_CONFIG};

    let conf = ClusterConf::default();
    // `with_limits(1, 10_000_000)` makes the cap distinguishable from the
    // default, so a snapshot would be visible as a wrong value.
    let state = Arc::new(TransferRuntimeConfig::with_limits(1, 10_000_000, 500_000));
    let scheduler = scheduler_with_state(Arc::new(MemoryTransferStore::new()), state.clone());
    let registry = build_transfer_runtime_registry(state.clone(), &conf);

    assert_eq!(state.max_tasks_per_transfer(), 10_000_000);

    // Lowering the live value must be visible immediately through the same
    // state the planner was handed.
    registry
        .set(MAX_TASKS_PER_TRANSFER_CONFIG, "250000", None)
        .unwrap();
    assert_eq!(state.max_tasks_per_transfer(), 250_000);

    // Resetting restores the file value rather than leaving the pushed one.
    registry.reset(MAX_TASKS_PER_TRANSFER_CONFIG, None).unwrap();
    assert_eq!(state.max_tasks_per_transfer(), 10_000_000);

    drop(scheduler);
}

/// The concurrency ceiling must be read at dispatch time, not snapshotted at
/// construction: a runtime-config push has to change the very next decision of
/// an already-running scheduler. Both the state setter and the registry (the
/// only production write path) are exercised so the test fails if either the
/// field goes back to a `usize` snapshot or the read point caches a local.
#[test]
fn dispatch_reads_the_live_worker_task_limit() {
    use crate::transfer::{build_transfer_runtime_registry, WORKER_MAX_CONCURRENT_TASKS_CONFIG};
    use curvine_model::WorkerInfo;

    let conf = ClusterConf::default();
    let state = Arc::new(TransferRuntimeConfig::with_limit(1));
    let scheduler = scheduler_with_state(Arc::new(MemoryTransferStore::new()), state.clone());
    let workers = [WorkerInfo::new(Default::default(), 1)];
    // The admission gate itself, which is what the live limit has to move. The
    // decision below falls back to a probe when the gate excludes everything,
    // so the ceiling is only observable here.
    let admitted = |scheduler: &TransferScheduler<MemoryTransferStore>, state: &TransferRuntimeConfig| {
        super::super::choose_worker_admitted(
            &workers,
            10,
            state.worker_task_limit(),
            TransferConf::DEFAULT_WORKER_INFLIGHT_BUDGET_BYTES,
            |id| scheduler.inflight.get(id),
        )
        .len()
    };

    // Ceiling 1: the first decision admits the idle worker and debits the
    // ledger; the same 1-slot ceiling then excludes it from the gate.
    assert_eq!(admitted(&scheduler, &state), 1);
    assert!(scheduler.choose_worker(&workers, 10).is_some());
    assert_eq!(admitted(&scheduler, &state), 0);
    // A saturated ledger dispatches nothing: capacity comes from the heartbeat
    // snapshot, not from aiming at a worker we already estimated as full.
    assert!(
        scheduler.choose_worker(&workers, 10).is_none(),
        "a saturated ledger must stop dispatching, not probe"
    );

    // Raising the live value alone (no new scheduler) admits the next decision.
    assert_eq!(state.update_worker_task_limit(3).unwrap(), 1);
    assert_eq!(admitted(&scheduler, &state), 1);

    // The registry is the production write path and must move the same atomic.
    let registry = build_transfer_runtime_registry(state.clone(), &conf);
    registry
        .set(WORKER_MAX_CONCURRENT_TASKS_CONFIG, "1", None)
        .unwrap();
    assert_eq!(state.worker_task_limit(), 1);
    assert_eq!(admitted(&scheduler, &state), 0);
    registry
        .set(WORKER_MAX_CONCURRENT_TASKS_CONFIG, "9", None)
        .unwrap();
    assert_eq!(admitted(&scheduler, &state), 1);
}

fn scheduler_with_placement(
    store: Arc<MemoryTransferStore>,
    endpoints: &[&str],
    self_addr: &str,
) -> TransferScheduler<MemoryTransferStore> {
    let conf = ClusterConf::default();
    let rt = Arc::new(conf.transfer_server_conf().create_runtime());
    let fs = CurvineFileSystem::with_rt(ClusterConf::default(), rt.clone()).unwrap();
    let cache = ClusterMetadataCache::new(fs.clone());
    let runtime_config = Arc::new(TransferRuntimeConfig::with_limit(1));
    let factory = Arc::new(UfsFactory::with_rt(&conf.client, rt.clone()));
    let planner = TransferPlanner::new(
        fs.clone(),
        factory.clone(),
        cache.clone(),
        conf.client.clone(),
        runtime_config.clone(),
        1,
        curvine_config::LoadTransferDefaults::from_job(&conf.job),
    );
    let clients = Arc::new(curvine_rpc::client::ClientFactory::with_rt(
        conf.client_rpc_conf(),
        rt,
    ));
    let forwarder = Arc::new(crate::transfer::Forwarder::new(
        endpoints.iter().map(|addr| (*addr).to_string()).collect(),
        self_addr,
        clients,
    ));
    TransferScheduler::new(
        store,
        planner,
        cache,
        factory,
        fs,
        "test-owner".to_string(),
        self_addr.to_string(),
        TransferConf::default(),
        runtime_config,
        std::sync::Arc::new(crate::transfer::TaskCache::new()),
        Some(forwarder),
    )
}

/// A job that a scheduler has just leased for planning, with the placement the
/// submitter recorded for it.
fn planning_job(job_id: &str, placement_addr: &str) -> TransferJobRecord {
    let mut job = running_job(job_id);
    job.state = TransferState::Planning;
    job.placement_addr = placement_addr.to_string();
    job.summary.message = "acquired for planning".to_string();
    job.lease_expire_at = now_ms().saturating_add(60_000);
    job
}

/// The acting owner has to reach the store, not just this tick's copy.
///
/// `owner_addr` is what status/watch relay on and what the console shows as
/// "running on". Mutating the in-memory record only meant every SQL-backed job
/// kept an empty owner, so a job looked unowned for its whole life.
#[tokio::test]
async fn acquiring_a_job_persists_the_acting_owner_address() {
    let store = Arc::new(MemoryTransferStore::new());
    // Placement names this instance, so the tick keeps the lease and records
    // itself as the owner. Planning then fails (no mount table in this test),
    // which is irrelevant: the owner write happens first.
    let mut job = running_job("owner-addr-persisted");
    job.state = TransferState::Pending;
    job.placement_addr = "10.9.9.10:8010".to_string();
    job.lease_expire_at = 0;
    store.create_or_get_by_request_id(job.clone()).unwrap();
    let scheduler = scheduler_with_placement(
        store.clone(),
        &["10.9.9.9:8010", "10.9.9.10:8010"],
        "10.9.9.10:8010",
    );

    // A Pending job is only preferred every Nth tick, so run a few ticks: the
    // owner write has to happen the first time the lease is actually taken.
    for _ in 0..40 {
        let _ = scheduler.run_once().await;
        if !store
            .get_transfer(&job.job_id)
            .unwrap()
            .unwrap()
            .owner_addr
            .is_empty()
        {
            break;
        }
    }

    let stored = store.get_transfer(&job.job_id).unwrap().unwrap();
    assert_eq!(
        stored.owner_addr, "10.9.9.10:8010",
        "the owner address must be persisted, not only held in memory"
    );
    // The relay path reads it back through the service.
    let service = crate::transfer::TransferService::new(store.clone());
    assert_eq!(
        service.owner_addr(&job.job_id).as_deref(),
        Some("10.9.9.10:8010")
    );
}

/// Handing a lease to a sibling must keep working: the fix must not disable
/// placement, only make "which address is me" resolvable.
#[tokio::test]
async fn placement_naming_a_sibling_in_the_ring_hands_the_lease_back() {
    let store = Arc::new(MemoryTransferStore::new());
    let job = planning_job("placed-on-sibling", "10.9.9.9:8010");
    store.create_or_get_by_request_id(job.clone()).unwrap();
    let scheduler = scheduler_with_placement(
        store.clone(),
        &["10.9.9.9:8010", "10.9.9.10:8010"],
        "10.9.9.10:8010",
    );

    assert!(scheduler
        .hand_back_to_placement(&job, &lease(&job), now_ms())
        .unwrap());

    let stored = store.get_transfer(&job.job_id).unwrap().unwrap();
    assert_eq!(stored.state, TransferState::Pending);
    assert!(stored.owner.is_empty(), "the lease must be released");
}

/// The livelock: when `transfer.endpoints` never names this instance, a
/// hand-back can never land anywhere and the same instance re-acquires the job
/// on the next tick, forever (observed in production: 4.1M hand-backs, zero
/// planning). Such a job has to be planned locally instead.
#[tokio::test]
async fn placement_to_an_instance_outside_the_ring_plans_locally_instead_of_looping() {
    let store = Arc::new(MemoryTransferStore::new());
    let job = planning_job("placement-outside-ring", "10.9.9.9:8010");
    store.create_or_get_by_request_id(job.clone()).unwrap();
    // This instance is not part of the configured instance list at all.
    let scheduler = scheduler_with_placement(store.clone(), &["10.9.9.9:8010"], "10.9.9.10:8010");

    assert!(
        !scheduler
            .hand_back_to_placement(&job, &lease(&job), now_ms())
            .unwrap(),
        "an unlisted instance must keep the lease instead of returning it forever"
    );

    let stored = store.get_transfer(&job.job_id).unwrap().unwrap();
    assert_eq!(stored.state, TransferState::Planning);
    assert_eq!(stored.owner, "test-owner");
}

/// The other livelock: placement names a live sibling in the ring, but that
/// sibling never claims the lease (dead endpoint behind a static list, a drifted
/// endpoint list, or a rerun whose fresh request id hashes outside the ring).
/// Retrying the hand-back forever keeps the job Pending, so this instance has to
/// keep the lease after a bounded number of attempts.
#[tokio::test]
async fn placement_that_never_claims_the_lease_is_planned_locally() {
    let store = Arc::new(MemoryTransferStore::new());
    let job = planning_job("placement-never-claimed", "10.9.9.9:8010");
    store.create_or_get_by_request_id(job.clone()).unwrap();
    let scheduler = scheduler_with_placement(
        store.clone(),
        &["10.9.9.9:8010", "10.9.9.10:8010"],
        "10.9.9.10:8010",
    );

    for attempt in 1..=MAX_PLACEMENT_HAND_BACKS {
        assert!(
            scheduler
                .hand_back_to_placement(&job, &lease(&job), now_ms())
                .unwrap(),
            "hand-back {} must still give the placement owner a chance",
            attempt
        );
    }
    // The hand-backs above really requeued the row, so it is Pending now. A
    // real tick re-acquires it as Planning first; what matters here is that the
    // takeover does not write the row again.
    let before = store.get_transfer(&job.job_id).unwrap().unwrap();
    assert_eq!(before.state, TransferState::Pending);
    assert!(
        !scheduler
            .hand_back_to_placement(&job, &lease(&job), now_ms())
            .unwrap(),
        "an unclaimed placement must not keep the job Pending forever"
    );
    let after = store.get_transfer(&job.job_id).unwrap().unwrap();
    assert_eq!(
        after.state, before.state,
        "a takeover must keep the lease, not requeue the row"
    );
    assert_eq!(after.fence, before.fence);
    // The takeover has to stick: a job that failed to plan and came back must
    // not spend another five ticks handing the lease to the same dead address.
    assert!(
        !scheduler
            .hand_back_to_placement(&job, &lease(&job), now_ms())
            .unwrap(),
        "the takeover must survive the job being re-acquired"
    );
}

/// Cancelling must cost one store operation for the whole plan and must leave a
/// task alone while its worker is still reporting it (that worker gets a cancel
/// RPC instead). Regression guard for the 114k-transaction cancel of CV-86.
#[tokio::test]
async fn cancel_stops_recoverable_tasks_and_keeps_live_runs() {
    let store = Arc::new(MemoryTransferStore::new());
    let job = running_job("cancel-batch");
    store.create_or_get_by_request_id(job.clone()).unwrap();

    let now = now_ms();
    let pending = task(&job.job_id, "pending", TransferTaskState::Pending);
    let stale = task(&job.job_id, "stale", TransferTaskState::Stale);

    let mut stale_running = task(&job.job_id, "stale-running", TransferTaskState::Running);
    stale_running.stale_deadline_at = now.saturating_sub(1_000);
    stale_running.worker_id = 11;
    stale_running.worker_session_id = "s11".to_string();

    let mut live = task(&job.job_id, "live", TransferTaskState::Running);
    live.stale_deadline_at = now.saturating_add(60_000);
    live.worker_id = 22;
    live.worker_session_id = "s22".to_string();

    let completed = task(&job.job_id, "done", TransferTaskState::Completed);
    store
        .insert_tasks(vec![pending, stale, stale_running, live, completed])
        .unwrap();

    scheduler(store.clone())
        .stop_recoverable_tasks(&job, &lease(&job), "transfer canceled")
        .await
        .unwrap();

    let tasks = store.list_transfer_tasks(&job.job_id, job.run_id).unwrap();
    let state_of = |task_id: &str| {
        tasks
            .iter()
            .find(|task| task.task_id == task_id)
            .map(|task| task.state)
            .unwrap()
    };
    assert_eq!(state_of("pending"), TransferTaskState::Canceled);
    assert_eq!(state_of("stale"), TransferTaskState::Canceled);
    assert_eq!(state_of("stale-running"), TransferTaskState::Canceled);
    assert_eq!(
        state_of("live"),
        TransferTaskState::Running,
        "a task whose worker is still reporting must not be canceled behind its back"
    );
    assert_eq!(state_of("done"), TransferTaskState::Completed);
    assert_eq!(
        store.running_task_workers(&job.job_id, job.run_id).unwrap(),
        vec![(22u32, "s22".to_string())]
    );
}

/// The task-state gauges are process-global, so the two tests that assert on
/// them must not interleave (other tests may still debit a gauge concurrently,
/// which is why the assertions are one-sided).
static GAUGE_TEST_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

/// Reads a registered gauge straight out of the Prometheus registry.
fn gauge_value(name: &str) -> i64 {
    curvine_metrics::Metrics::registry()
        .gather()
        .iter()
        .find(|family| family.get_name() == name)
        .and_then(|family| family.get_metric().first())
        .map(|metric| metric.get_gauge().get_value() as i64)
        .unwrap_or(0)
}

/// `transfer_tasks_pending` must reflect file-level tasks, which have no
/// `transfer_tasks` rows: the store-side scan alone reports 0 and the dashboard
/// then drifts to a negative count as dispatches debit it.
#[test]
fn task_count_calibration_includes_in_memory_tasks() {
    let scheduler = scheduler(Arc::new(MemoryTransferStore::new()));
    let job = running_job("calibrated");
    scheduler.task_cache.seed(
        &job.job_id,
        job.run_id,
        vec![
            task(&job.job_id, "p1", TransferTaskState::Pending),
            task(&job.job_id, "p2", TransferTaskState::Pending),
            task(&job.job_id, "r1", TransferTaskState::Running),
        ],
    );

    // The store holds no rows at all, so everything reported here comes from
    // the in-memory task cache.
    let counts = scheduler.calibrated_task_counts(now_ms()).unwrap();
    assert_eq!((counts.pending, counts.running), (2, 1));
}

/// A dispatch that no process ever planned (takeover, restart, redelivery) debits
/// `pending` without a matching credit. The gauge must drift towards zero, never
/// below it: `-302` is what an operator saw on a live dashboard.
#[test]
fn task_gauges_never_go_negative() {
    let _guard = GAUGE_TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let metrics = TransferMetrics::get().unwrap();
    let before_pending = gauge_value("transfer_tasks_pending");
    let before_running = gauge_value("transfer_tasks_running");
    for _ in 0..(before_pending + 5) {
        metrics.task_assigned();
    }
    for _ in 0..(before_running + 5) {
        metrics.task_terminal(TransferTaskState::Completed);
    }
    // Another test may have credited these gauges concurrently, so only the
    // invariant is asserted: an over-decremented gauge floors at zero.
    assert!(gauge_value("transfer_tasks_pending") >= 0);
    assert!(gauge_value("transfer_tasks_running") >= 0);
}

/// A worker that refuses a file-level task must not cost a whole
/// `task_stale_timeout`: file-level tasks have no `transfer_tasks` row, so the
/// store write answers "not found" and the task used to sit in Running until a
/// stale deadline expired. The rejection has to release the cache attempt.
#[test]
fn rejected_file_level_task_returns_to_pending_immediately() {
    use crate::transfer::dispatch::requeue_rejected_task;

    let job = running_job("rejected-cache-task");
    let task_cache = Arc::new(crate::transfer::TaskCache::new());
    let mut task = task(&job.job_id, "t1", TransferTaskState::Running);
    task.worker_id = 7;
    task.stale_deadline_at = now_ms() + 60_000;
    task_cache.seed(&job.job_id, job.run_id, vec![task.clone()]);
    let store = Arc::new(MemoryTransferStore::new());

    let accepted = requeue_rejected_task(
        store.as_ref(),
        &task_cache,
        &task,
        "test-owner",
        1,
        "worker_at_capacity: inflight bytes",
        false,
        now_ms(),
    )
    .unwrap();
    assert!(accepted, "the cache release must count as requeued");

    let cached = task_cache
        .tasks_by_state(&job.job_id, job.run_id, TransferTaskState::Pending)
        .into_iter()
        .map(|task| task.task_id)
        .collect::<Vec<_>>();
    assert_eq!(cached, vec!["t1".to_string()]);
    assert!(
        task_cache
            .tasks_by_state(&job.job_id, job.run_id, TransferTaskState::Running)
            .is_empty(),
        "the rejected attempt must not stay Running in the cache"
    );
}

/// The same rejection for a row-backed (segment) task goes through the store.
#[test]
fn rejected_row_backed_task_is_requeued_in_the_store() {
    use crate::transfer::dispatch::requeue_rejected_task;

    let job = running_job("rejected-row-task");
    let mut task = task(&job.job_id, "t1", TransferTaskState::Running);
    task.worker_id = 7;
    let store = Arc::new(MemoryTransferStore::new());
    // The store validates owner/lease against the job row, so the row has to
    // exist exactly like it would in production.
    store.create_or_get_by_request_id(job.clone()).unwrap();
    store.insert_tasks(vec![task.clone()]).unwrap();
    let task_cache = Arc::new(crate::transfer::TaskCache::new());

    let accepted = requeue_rejected_task(
        store.as_ref(),
        &task_cache,
        &task,
        &job.owner,
        job.fence,
        "worker_at_capacity: inflight bytes",
        false,
        now_ms(),
    )
    .unwrap();
    assert!(accepted);
    let pending = store
        .list_tasks_by_state(&job.job_id, job.run_id, TransferTaskState::Pending, 10)
        .unwrap();
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].task_id, "t1");
}

/// Capacity can never wedge dispatch permanently: a ledger that overshot (a
/// burst of dispatches, or refusals counted as in-flight) is replaced by the
/// next heartbeat snapshot, and the very next decision sees the freed room
/// again. This is the invariant the old "rebase on submit answers" design broke:
/// its only correction path was the submit it had just stopped making.
#[test]
fn oversized_ledger_recovers_on_the_next_snapshot() {
    let scheduler = scheduler(Arc::new(MemoryTransferStore::new()));
    let worker = {
        use curvine_model::{WorkerAddress, WorkerInfo};
        WorkerInfo::new(
            WorkerAddress {
                worker_id: 9,
                ..Default::default()
            },
            1,
        )
    };
    let workers = [worker.clone()];

    // Snapshot says the worker is idle, ceiling is 1: the first decision is
    // admitted and debits the ledger.
    scheduler.inflight.apply_snapshot(1, &workers);
    assert!(scheduler.choose_worker(&workers, 10).is_some());
    // ...and the second one is refused: dispatch stops instead of probing.
    assert!(scheduler.choose_worker(&workers, 10).is_none());

    // The worker drained, but the ledger's local delta still says "busy". The
    // next snapshot (a plain 10s tick, no submit involved) clears it.
    scheduler.inflight.apply_snapshot(2, &workers);
    assert!(
        scheduler.choose_worker(&workers, 10).is_some(),
        "a new snapshot must reopen dispatch without any submit answer"
    );
}
