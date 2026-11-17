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

use crate::common::UfsFactory;
use crate::transfer::{
    is_store_unavailable_error, job_mount_snapshot_async, job_mount_snapshot_with_version_async,
    merge_summaries, record_master_rpc, transfer_failure_message, MountSnapshotError, TaskCache,
    TransferMountSnapshotUpdate, TransferPlannedTasks, TransferPlanner, TransferRequeueUpdate,
    TransferRuntimeConfig, TransferStore, TransferTaskStateUpdate,
};
use curvine_client_core::file::CurvineFileSystem;
use curvine_config::TransferConf;
use curvine_error::FsError;
use curvine_error::FsResult;
use curvine_fs_api::{Path, GET_BLOCK_LOCATIONS_BATCH_MAX_PATHS};
use curvine_model::{
    agreed_plan_source_mtime, classify_transfer_plan, normalize_group_resume, BatchManifest,
    BlockLocation, CommitBlock, LoadJobInfo, LoadTaskInfo, LocatedBlock, SetAttrOptsBuilder,
    TaskAttemptStart, TaskBlockLocation, TransferJobRecord, TransferLease, TransferPlanKind,
    TransferProgress, TransferState, TransferStateUpdate, TransferTaskCounts, TransferTaskRecord,
    TransferTaskReport, TransferTaskReportInfo, TransferTaskState, TransferTaskSummary, WorkerInfo,
};
use curvine_runtime::common::LocalTime;
use curvine_runtime::runtime::RpcRuntime;
use dashmap::DashMap;
use futures::stream::{self, StreamExt, TryStreamExt};
use log::{debug, error, info, warn};
use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

/// Scheduler-side per-worker in-flight ledger (design doc §4.1).
///
/// Lazy TTL refresh: consumed only by dispatch decisions, so the store
/// aggregation runs when the TTL expires, not on a timer. Within one refresh
/// window the ledger only grows (dispatch-time atomic increments) — overcount
/// means under-dispatch, which is the conservative direction. Correction
/// happens wholesale on the next refresh; no report-path bookkeeping.
pub(crate) fn terminal_snapshot_progress(
    mut progress: TransferProgress,
    transition_message: &str,
) -> TransferProgress {
    if progress.message.trim().is_empty() && !transition_message.trim().is_empty() {
        progress.message = transition_message.to_string();
    }
    progress
}

fn join_transfer_target(dir: &Path, relative_path: &str) -> FsResult<String> {
    let full = dir.full_path();
    let joined = if full.ends_with(Path::SEPARATOR) {
        format!("{full}{relative_path}")
    } else {
        format!("{full}{}{relative_path}", Path::SEPARATOR)
    };
    Ok(Path::from_str(&joined)?.clone_uri())
}

/// Targets whose segment tasks all completed, paired with the plan-time
/// source mtime their plans agree on (`None` when a plan pins none, or when
/// the plans disagree because the source moved while it was being copied).
///
/// Convergence stamps that mtime as the target's UFS identity. Without it a
/// directory-loaded large file stays `{ufs_mtime: 0, state: Cv}`, which
/// `cv_valid` rejects outright: every read falls back to UFS and every
/// incremental load re-copies the whole file.
pub(crate) fn converged_segment_targets(
    tasks: &[TransferTaskRecord],
) -> Vec<(String, Option<i64>)> {
    use std::collections::HashMap;

    let mut total: HashMap<&str, usize> = HashMap::new();
    let mut completed: HashMap<&str, usize> = HashMap::new();
    let mut plans: HashMap<&str, Vec<&str>> = HashMap::new();
    for task in tasks {
        if !matches!(
            classify_transfer_plan(&task.source_read_plan_json),
            TransferPlanKind::Segment | TransferPlanKind::SegmentGroup
        ) {
            continue;
        }
        let target = task.target_path.as_str();
        *total.entry(target).or_insert(0) += 1;
        plans
            .entry(target)
            .or_default()
            .push(task.source_read_plan_json.as_str());
        if task.state == TransferTaskState::Completed {
            *completed.entry(target).or_insert(0) += 1;
        }
    }

    let mut targets: Vec<(String, Option<i64>)> = total
        .into_iter()
        .filter(|(target, count)| completed.get(target).copied().unwrap_or(0) == *count)
        .map(|(target, _)| {
            let mtime = agreed_plan_source_mtime(plans.remove(target).unwrap_or_default());
            (target.to_string(), mtime)
        })
        .collect();
    targets.sort();
    targets
}

/// Target path → (block id → worker ids) reported by the completed segment
/// tasks of a run. Only completed segment-task reports carry locations.
pub(crate) fn reported_block_locations(
    tasks: &[TransferTaskRecord],
) -> HashMap<String, HashMap<i64, Vec<u32>>> {
    let mut reported: HashMap<String, HashMap<i64, Vec<u32>>> = HashMap::new();
    for task in tasks {
        if task.state != TransferTaskState::Completed || task.block_locations.is_empty() {
            continue;
        }
        if !matches!(
            classify_transfer_plan(&task.source_read_plan_json),
            TransferPlanKind::Segment | TransferPlanKind::SegmentGroup
        ) {
            continue;
        }
        let entry = reported.entry(task.target_path.clone()).or_default();
        for location in &task.block_locations {
            entry.insert(location.block_id, location.worker_ids.clone());
        }
    }
    reported
}

/// Build the commit entry for one layout block. Reported worker ids win: the
/// master layout carries no locations for blocks no client has committed yet.
pub(crate) fn commit_block_for(
    located: &LocatedBlock,
    reported: Option<&HashMap<i64, Vec<u32>>>,
) -> CommitBlock {
    let mut commit = CommitBlock::from(located);
    if commit.locations.is_empty() {
        if let Some(worker_ids) = reported.and_then(|map| map.get(&located.block.id)) {
            commit.locations = worker_ids
                .iter()
                .map(|worker_id| BlockLocation::new(*worker_id, located.block.storage_type))
                .collect();
        }
    }
    commit
}

fn planned_target_replacements(
    tasks: &[TransferTaskRecord],
) -> FsResult<(HashSet<String>, HashSet<String>)> {
    let mut segment_targets = HashSet::new();
    let mut replacement_targets = HashSet::new();
    for task in tasks {
        match classify_transfer_plan(&task.source_read_plan_json) {
            TransferPlanKind::Segment | TransferPlanKind::SegmentGroup => {
                segment_targets.insert(task.target_path.clone());
                if task.replace_target {
                    replacement_targets.insert(task.target_path.clone());
                }
            }
            TransferPlanKind::Batch => {
                let manifest: BatchManifest = serde_json::from_str(&task.source_read_plan_json)
                    .map_err(|err| FsError::common(format!("invalid batch manifest: {err}")))?;
                let target_dir = Path::from_str(&task.target_path)?;
                for entry in manifest.batch {
                    if entry.replace_target {
                        replacement_targets
                            .insert(join_transfer_target(&target_dir, &entry.rel_path)?);
                    }
                }
            }
            TransferPlanKind::Plain if task.replace_target => {
                replacement_targets.insert(task.target_path.clone());
            }
            TransferPlanKind::Plain => {}
        }
    }
    Ok((segment_targets, replacement_targets))
}

/// Short-lived exclusion of workers that just failed a dispatch.
///
/// A worker that dies stays in the cluster snapshot until its next refresh, so
/// without this a retry can keep choosing the same dead node and burn the whole
/// retry budget without ever trying a healthy one.
struct WorkerBlacklist {
    until_ms: DashMap<u32, i64>,
}

impl WorkerBlacklist {
    /// Long enough to cover a snapshot refresh, short enough that a restarted
    /// worker is usable again quickly.
    const TTL_MS: i64 = 15_000;

    fn new() -> Self {
        Self {
            until_ms: DashMap::new(),
        }
    }

    fn block(&self, worker_id: u32, now_ms: i64) {
        self.until_ms
            .insert(worker_id, now_ms.saturating_add(Self::TTL_MS));
    }

    fn is_blocked(&self, worker_id: u32, now_ms: i64) -> bool {
        match self.until_ms.get(&worker_id).map(|until| *until) {
            Some(until) if until > now_ms => true,
            Some(_) => match self.until_ms.entry(worker_id) {
                dashmap::mapref::entry::Entry::Occupied(entry) if *entry.get() > now_ms => true,
                dashmap::mapref::entry::Entry::Occupied(entry) => {
                    entry.remove();
                    false
                }
                dashmap::mapref::entry::Entry::Vacant(_) => false,
            },
            None => false,
        }
    }

    fn clear(&self) {
        self.until_ms.clear();
    }
}

/// Scheduler-side per-worker in-flight ledger.
///
/// Capacity is a *pushed* signal: the worker reports its admitted load on every
/// heartbeat, the master relays it in the cluster snapshot, and every dispatch
/// decision reapplies that snapshot as the base. `account` adds what this
/// instance dispatched since then, so two writers keep the estimate honest and
/// neither of them is a submit answer.
///
/// The distinction matters: an estimate derived from submit answers freezes as
/// soon as the scheduler stops submitting, which is exactly when it is at its
/// ceiling (the CV-86 stall). A snapshot-derived base always refreshes, so a
/// full ledger is a bounded state, not a deadlock.
///
/// A lock-guarded map, not DashMap: under 8 scheduler threads the
/// DashMap/parking_lot shard-lock path was observed to park futex waiters
/// forever (all-scheduler freeze), and the live set is only worker-count sized.
struct InflightLedger {
    entries: std::sync::RwLock<std::collections::HashMap<u32, InflightEntry>>,
    /// Largest task estimate this scheduler has dispatched. A count ceiling is
    /// only reachable if this many bytes also fit under the byte ceiling.
    max_task_bytes: AtomicI64,
}

/// One worker's dispatch accounting.
///
/// `base_*` is the worker's own admission state as of `version` (the cluster
/// snapshot the transfer last applied); `pending_*` counts what this instance
/// dispatched since then. The estimate is their sum, so it can only overstate
/// load (never understate), and it is replaced wholesale by the next snapshot:
/// the capacity signal is the heartbeat, never a submit answer.
#[derive(Clone, Copy, Default)]
struct InflightEntry {
    version: u64,
    base_tasks: u64,
    base_bytes: i64,
    pending_tasks: u64,
    pending_bytes: i64,
}

impl InflightLedger {
    fn new(_ttl: Duration) -> Self {
        Self {
            entries: std::sync::RwLock::new(Default::default()),
            max_task_bytes: AtomicI64::new(0),
        }
    }

    fn max_task_bytes(&self) -> i64 {
        self.max_task_bytes.load(Ordering::Relaxed)
    }

    /// Adopts a cluster snapshot as the new base for every worker in it.
    ///
    /// This is the only way the base moves: a scheduler with no submissions at
    /// all still converges to the workers' own load every refresh period, which
    /// is what makes "the ledger is full" a bounded state instead of a freeze.
    fn apply_snapshot(&self, version: u64, workers: &[WorkerInfo]) {
        let mut entries = self.entries.write().unwrap_or_else(|e| e.into_inner());
        let mut seen = std::collections::HashSet::with_capacity(workers.len());
        for worker in workers {
            let worker_id = worker.worker_id();
            seen.insert(worker_id);
            let entry = entries.entry(worker_id).or_default();
            if version <= entry.version {
                continue;
            }
            entry.version = version;
            entry.base_tasks = worker.transfer_inflight_tasks;
            entry.base_bytes = worker.transfer_inflight_bytes.max(0);
            entry.pending_tasks = 0;
            entry.pending_bytes = 0;
            record_metric(|metrics| {
                metrics.set_worker_inflight(worker_id, entry.base_tasks, entry.base_bytes)
            });
        }
        // Workers that left the cluster must not keep a stale entry alive.
        entries.retain(|worker_id, _| seen.contains(worker_id));
    }

    /// Accounts tasks this instance just dispatched to `worker_id`.
    fn account(&self, worker_id: u32, count: u64, bytes: i64) {
        let seen = self.max_task_bytes.load(Ordering::Relaxed);
        if bytes > seen {
            self.max_task_bytes.store(bytes, Ordering::Relaxed);
        }
        let mut entries = self.entries.write().unwrap_or_else(|e| e.into_inner());
        let entry = entries.entry(worker_id).or_default();
        entry.pending_tasks = entry.pending_tasks.saturating_add(count);
        entry.pending_bytes = entry.pending_bytes.saturating_add(bytes);
        let (tasks, bytes) = (
            entry.base_tasks.saturating_add(entry.pending_tasks),
            entry.base_bytes.saturating_add(entry.pending_bytes),
        );
        drop(entries);
        record_metric(|metrics| metrics.set_worker_inflight(worker_id, tasks, bytes));
    }

    /// Estimated in-flight load: heartbeat base plus local unacknowledged work.
    fn get(&self, worker_id: u32) -> (u64, i64) {
        self.entries
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .get(&worker_id)
            .map(|entry| {
                (
                    entry.base_tasks.saturating_add(entry.pending_tasks),
                    entry.base_bytes.saturating_add(entry.pending_bytes),
                )
            })
            .unwrap_or((0, 0))
    }
}

const PLANNING_REQUEUE_DELAY_MS: i64 = 1_000;
const MOUNT_SNAPSHOT_MAX_RETRIES: u32 = 3;
/// Max automatic retries for a retryable planning failure (rate limiting,
/// timeouts). Budget: ~1s + 2s + 4s + 8s + jitter, then terminal failure.
const PLANNING_LIST_MAX_RETRIES: u32 = 4;

#[derive(Debug, PartialEq, Eq)]
enum MountSnapshotFailureAction {
    Requeue { retry_count: u32, delay_ms: i64 },
    Fail,
}

enum StaleTaskProbeOutcome {
    Found(TransferTaskReport),
    Retry {
        task: TransferTaskRecord,
        reason: &'static str,
        error: String,
    },
    Missing {
        task: TransferTaskRecord,
        reason: StaleTaskMissingReason,
    },
}

#[derive(Clone, Copy)]
enum StaleTaskMissingReason {
    WorkerSessionGone,
    TaskAttemptMissing,
}

impl StaleTaskMissingReason {
    fn message(self) -> &'static str {
        match self {
            Self::WorkerSessionGone => "assigned Worker session is no longer live",
            Self::TaskAttemptMissing => "assigned Worker does not have this task attempt",
        }
    }
}

#[derive(Default)]
struct StaleTaskMissingCandidates {
    worker_session_gone: Vec<TransferTaskRecord>,
    task_attempt_missing: Vec<TransferTaskRecord>,
}

fn mount_snapshot_failure_action(attempts: u32) -> MountSnapshotFailureAction {
    if attempts >= MOUNT_SNAPSHOT_MAX_RETRIES {
        return MountSnapshotFailureAction::Fail;
    }
    MountSnapshotFailureAction::Requeue {
        retry_count: attempts.saturating_add(1),
        delay_ms: PLANNING_REQUEUE_DELAY_MS.saturating_mul(2_i64.saturating_pow(attempts)),
    }
}

/// Retryable planning failures get bounded exponential backoff instead of an
/// immediate terminal failure (e.g. UFS rate limiting during list/stat).
/// Returns the next attempt count, or None when the budget is exhausted.
fn planning_retry_attempt(attempts: u32) -> Option<u32> {
    if attempts >= PLANNING_LIST_MAX_RETRIES {
        return None;
    }
    Some(attempts.saturating_add(1))
}

/// Backoff for a planning retry: base * 2^n plus jitter. Jitter spreads jobs
/// that hit the same rate limiter so they do not retry in lockstep and
/// amplify the limit.
fn planning_retry_delay_ms(attempts: u32, job_key: &str, now_ms: i64) -> i64 {
    let base = PLANNING_REQUEUE_DELAY_MS.saturating_mul(2_i64.saturating_pow(attempts.min(10)));
    // Per-job entropy (hash) so a 429 burst hitting several planning workers
    // does not retry in lockstep; wall-clock alone maps nearby failures to
    // nearby retries.
    let mut hash: u64 = 1469598103934665603;
    for b in job_key.as_bytes() {
        hash ^= *b as u64;
        hash = hash.wrapping_mul(1099511628211);
    }
    let jitter = (hash % (PLANNING_REQUEUE_DELAY_MS.max(1) as u64)) as i64;
    base.saturating_add(jitter)
        .saturating_add(now_ms.abs() % 64)
}

/// Whether a planning failure is worth an automatic retry. Structural errors
/// (missing source, oversized directory, bad args) fail fast; only
/// throttling/timeout/overload signals back off. Message matching is
/// intentionally narrow: misclassifying permanent as retryable only delays
/// the failure within a small bounded budget, while the reverse drops jobs
/// that would succeed minutes later.
fn is_planning_retryable(err: &FsError) -> bool {
    use curvine_error::ErrorKind;
    if matches!(
        err.kind(),
        ErrorKind::Timeout | ErrorKind::TransferOverloaded | ErrorKind::TransferStoreUnavailable
    ) {
        return true;
    }
    // UFS adapters surface OpenDAL throttling as a generic error; match the
    // stable upstream markers.
    let msg = err.to_string();
    msg.contains("RateLimited")
        || msg.contains("TooManyRequests")
        || msg.contains("too many requests")
}

use crate::transfer::{
    ClusterMetadataCache, TransferMetrics, MAX_JOBS_PER_STORE_TRANSACTION,
    MAX_TASKS_PER_STORE_TRANSACTION,
};

const TRANSFER_CLEANUP_INTERVAL_MS: u64 = 60_000;
const TRANSFER_METRICS_INTERVAL_MS: u64 = 10_000;
/// Task-state gauges are a store scan plus in-memory counts, so this is the
/// only thing bounding how long a missed increment can survive. Drift was
/// observed as a negative `transfer_tasks_pending` for nearly an hour.
const TRANSFER_TASK_COUNTS_INTERVAL_MS: u64 = 30_000;
const TRANSFER_CLEANUP_LEASE_MS: i64 = 120_000;
/// Max terminal task rows deleted per cleanup transaction (§8 C2).
/// A huge job spans multiple rounds instead of one giant transaction.
const TERMINAL_TASK_PURGE_ROW_LIMIT: usize = MAX_TASKS_PER_STORE_TRANSACTION;
const TERMINAL_JOB_PURGE_LIMIT: usize = MAX_JOBS_PER_STORE_TRANSACTION;
/// Upper bound on the dispatch work of one tick: a job whose pending set is huge
/// would otherwise keep this inner loop running for minutes and hold its
/// scheduler thread. Leftovers are picked up by the next tick.
const SCHEDULER_TICK_BUDGET: Duration = Duration::from_millis(1_000);
/// Task ceiling per tick, independent of the clock so a fast store cannot turn
/// the budget into an unbounded burst.
const SCHEDULER_TICK_TASK_LIMIT: usize = 500;
const SCHEDULER_ACTIVE_INTERVAL: Duration = Duration::from_millis(200);
const SCHEDULER_IDLE_INTERVAL: Duration = Duration::from_secs(1);
const SCHEDULER_MIN_IDLE_INTERVAL: Duration = Duration::from_millis(1);
/// Every N scheduler ticks, prefer acquiring a new Pending job over
/// continuing to service already-held jobs (anti-starvation fairness).
const ACQUIRE_PENDING_EVERY_N_TICKS: u64 = 4;
/// Hand-backs tolerated before this instance keeps the lease and plans locally.
///
/// The count accumulates per run and is not reset by a successful claim: a job
/// that is really claimed stops being Planning, so it never comes back here.
/// Keeping the count monotonic is what makes the takeover decision stick.
///
/// A hand-back only lands when the recorded placement names an instance that is
/// alive and recognises itself. That does not hold for a job whose placement
/// names a decommissioned endpoint, a drifted endpoint list, or a rerun whose
/// fresh request id hashes outside the ring: the lease holder is then the only
/// instance with the row, `owns_addr` never answers yes for it, and the job
/// cycles Pending -> Planning -> Pending forever (observed in production: tens
/// of thousands of acquires and zero planning attempts). Planning here is safe
/// because the lease, not the placement, is what fences concurrent runs.
const MAX_PLACEMENT_HAND_BACKS: u32 = 5;
/// Upper bound on the hand-back ledger so a long-lived process cannot grow it
/// without limit.
const HAND_BACK_LEDGER_MAX_ENTRIES: usize = 4096;

pub struct TransferScheduler<S> {
    store: Arc<S>,
    planner: TransferPlanner,
    cache: ClusterMetadataCache,
    factory: Arc<UfsFactory>,
    /// CV client used to finalize segmented target files (design §14 T4):
    /// after all segment tasks complete, the scheduler queries each
    /// segmented file's block layout and issues complete_file_by_id.
    fs: CurvineFileSystem,
    owner: String,
    /// This instance's own rpc address, advertised to workers on every dispatch.
    /// A report always goes back to the instance that dispatched the task,
    /// which is the lease owner and therefore the only holder of that job's
    /// in-memory task state. It is deliberately NOT the configured endpoint
    /// list: that list is static, while ownership moves with the lease.
    self_report_target: String,
    conf: TransferConf,
    /// Live knobs shared with the runtime-config registry. Held by `Arc`, never
    /// snapshotted into this struct: a pushed value must be visible to the very
    /// next dispatch decision.
    runtime_config: Arc<TransferRuntimeConfig>,
    worker_cursor: Arc<AtomicUsize>,
    acquire_tick: Arc<AtomicU64>,
    last_cleanup_ms: Arc<AtomicU64>,
    last_metrics_ms: Arc<AtomicU64>,
    last_task_counts_ms: Arc<AtomicU64>,
    inflight: Arc<InflightLedger>,
    /// Workers that recently failed a dispatch, excluded for a short window.
    worker_blacklist: Arc<WorkerBlacklist>,
    /// File-level task state. Those tasks are never persisted, so every task
    /// read below merges this cache with the store.
    task_cache: Arc<TaskCache>,
    /// Placement owner lookup (discovery mode only). A job must be run by the
    /// instance its submit was relayed to, so that instance is the one holding
    /// the job's in-memory task state.
    forwarder: Option<Arc<crate::transfer::Forwarder>>,
    /// `job_id:run_id` -> consecutive hand-backs, see
    /// [`MAX_PLACEMENT_HAND_BACKS`].
    placement_hand_backs: Arc<DashMap<String, u32>>,
    /// Last reported result of the two-ceiling consistency check, so the log
    /// only fires on a transition instead of every metrics tick.
    capacity_mismatch: Arc<std::sync::atomic::AtomicBool>,
    /// Peer liveness for the lease-free ownership model (CV-105 §3.2):
    /// `transfer.endpoints`, per-peer probe state, and when we probed last.
    peers: Arc<Vec<String>>,
    peer_health: Arc<std::sync::Mutex<crate::transfer::ownership::PeerHealth>>,
    last_probe_ms: Arc<AtomicU64>,
    /// The boot sweep (probe + death scan + stop-then-run drive) runs on the
    /// first tick after start, not in the constructor, because it needs I/O.
    boot_sweep_done: Arc<std::sync::atomic::AtomicBool>,
    /// Keyset cursor over the jobs this instance owns, so re-ticking rotates
    /// without writing `updated_at` on every tick (R1). Empty = start over.
    retick_cursor: Arc<std::sync::Mutex<String>>,
    /// Settle passes (owner-lost + TTL + cancel advance) are throttled: they scan
    /// every owned job, so running them at the tick rate was the read
    /// amplification measured in R2.
    last_settle_ms: Arc<AtomicU64>,
}

impl<S> Clone for TransferScheduler<S> {
    fn clone(&self) -> Self {
        Self {
            store: self.store.clone(),
            planner: self.planner.clone(),
            cache: self.cache.clone(),
            factory: self.factory.clone(),
            fs: self.fs.clone(),
            owner: self.owner.clone(),
            self_report_target: self.self_report_target.clone(),
            conf: self.conf.clone(),
            runtime_config: self.runtime_config.clone(),
            worker_cursor: self.worker_cursor.clone(),
            acquire_tick: self.acquire_tick.clone(),
            last_cleanup_ms: self.last_cleanup_ms.clone(),
            last_metrics_ms: self.last_metrics_ms.clone(),
            last_task_counts_ms: self.last_task_counts_ms.clone(),
            inflight: self.inflight.clone(),
            worker_blacklist: self.worker_blacklist.clone(),
            task_cache: self.task_cache.clone(),
            forwarder: self.forwarder.clone(),
            placement_hand_backs: self.placement_hand_backs.clone(),
            capacity_mismatch: self.capacity_mismatch.clone(),
            peers: self.peers.clone(),
            peer_health: self.peer_health.clone(),
            last_probe_ms: self.last_probe_ms.clone(),
            boot_sweep_done: self.boot_sweep_done.clone(),
            retick_cursor: self.retick_cursor.clone(),
            last_settle_ms: self.last_settle_ms.clone(),
        }
    }
}

impl<S> TransferScheduler<S>
where
    S: TransferStore,
{
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        store: Arc<S>,
        planner: TransferPlanner,
        cache: ClusterMetadataCache,
        factory: Arc<UfsFactory>,
        fs: CurvineFileSystem,
        owner: String,
        self_report_target: String,
        conf: TransferConf,
        runtime_config: Arc<TransferRuntimeConfig>,
        task_cache: Arc<TaskCache>,
        forwarder: Option<Arc<crate::transfer::Forwarder>>,
    ) -> Self {
        let inflight_ttl =
            Duration::from_millis(TransferConf::DEFAULT_SCHEDULER_INFLIGHT_CACHE_TTL_MS);
        let peer_endpoints: Vec<String> = conf
            .endpoints
            .iter()
            .map(|endpoint| endpoint.trim().to_string())
            .filter(|endpoint| !endpoint.is_empty())
            .collect();
        Self {
            store,
            planner,
            cache,
            factory,
            fs,
            owner,
            self_report_target,
            conf,
            runtime_config,
            worker_cursor: Arc::new(AtomicUsize::new(0)),
            acquire_tick: Arc::new(AtomicU64::new(0)),
            last_cleanup_ms: Arc::new(AtomicU64::new(0)),
            last_metrics_ms: Arc::new(AtomicU64::new(0)),
            last_task_counts_ms: Arc::new(AtomicU64::new(0)),
            inflight: Arc::new(InflightLedger::new(inflight_ttl)),
            worker_blacklist: Arc::new(WorkerBlacklist::new()),
            task_cache,
            forwarder,
            placement_hand_backs: Arc::new(DashMap::new()),
            capacity_mismatch: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            peers: Arc::new(peer_endpoints),
            peer_health: Arc::new(std::sync::Mutex::new(
                crate::transfer::ownership::PeerHealth::new(
                    crate::transfer::ownership::PEER_DEAD_AFTER,
                ),
            )),
            last_probe_ms: Arc::new(AtomicU64::new(0)),
            boot_sweep_done: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            retick_cursor: Arc::new(std::sync::Mutex::new(String::new())),
            last_settle_ms: Arc::new(AtomicU64::new(0)),
        }
    }

    pub fn start(self, rt: Arc<curvine_runtime::runtime::Runtime>, stop: Arc<AtomicBool>) {
        let workers = self.conf.scheduler_workers();
        for index in 0..workers {
            let scheduler = self.clone();
            let stop = stop.clone();
            let rt = rt.clone();
            // Dedicated thread (same model as the report workers and the
            // worker heartbeat): the loop lives off the shared runtime, so a
            // stalled tick can never park every worker including the IO
            // driver. Without this a slow store call froze accept(), which
            // showed up as the pod failing readiness for ~25 minutes.
            let spawn_result = std::thread::Builder::new()
                .name(format!("transfer-scheduler-{index}"))
                .spawn(move || {
                    while !stop.load(Ordering::Relaxed) {
                        let delay = match rt.block_on(scheduler.run_once()) {
                            Ok(true) => SCHEDULER_ACTIVE_INTERVAL,
                            Ok(false) => scheduler.idle_interval(),
                            Err(err) => {
                                warn!("transfer scheduler worker {} tick failed: {}", index, err);
                                SCHEDULER_ACTIVE_INTERVAL
                            }
                        };
                        // Heartbeat: a stalled scheduler stops this counter, which
                        // is the only externally visible sign that the control
                        // plane stopped making progress.
                        record_metric(|metrics| metrics.inc_scheduler_tick());
                        std::thread::sleep(delay);
                    }
                });
            if let Err(err) = spawn_result {
                error!(
                    "failed to start transfer scheduler worker {}: {}",
                    index, err
                );
            }
        }
    }

    /// Probe every configured peer and fold the answers into [`PeerHealth`].
    ///
    /// One RPC per peer, 2s timeout, at most every `PEER_PROBE_INTERVAL`. A peer
    /// whose `ring_members` disagree with ours is reported as drift and excluded
    /// from judgement: "I cannot see you" is then a config bug, not a death.
    async fn probe_peers(&self, now_ms: i64, force: bool) {
        use crate::transfer::ownership::{PeerHealth, PEER_PROBE_INTERVAL, PEER_PROBE_TIMEOUT};

        if !force
            && now_ms.saturating_sub(self.last_probe_ms.load(Ordering::Acquire) as i64)
                < PEER_PROBE_INTERVAL.as_millis() as i64
        {
            return;
        }
        self.last_probe_ms.store(now_ms as u64, Ordering::Release);
        if self.peers.is_empty() {
            return;
        }
        let local = self.peers.as_ref().clone();
        for peer in self.peers.iter() {
            if peer == &self.self_report_target || peer == &self.owner {
                continue;
            }
            let client = match curvine_job_client::TransferClient::with_endpoint(
                &self.fs.fs_context(),
                peer.as_str(),
            ) {
                Ok(client) => client,
                Err(err) => {
                    log::debug!(
                        "transfer peer probe client for {} unavailable: {}",
                        peer,
                        err
                    );
                    continue;
                }
            };
            let answer = tokio::time::timeout(PEER_PROBE_TIMEOUT, client.service_info()).await;
            let mut health = self.peer_health.lock().unwrap_or_else(|e| e.into_inner());
            match answer {
                Ok(Ok(info)) => {
                    let report = crate::transfer::ownership::PeerReport {
                        instance_id: info.instance_id.unwrap_or_default(),
                        ring_members: info.ring_members.clone(),
                    };
                    let drift = {
                        let mut reported = report.ring_members.clone();
                        let mut configured = local.clone();
                        reported.sort();
                        configured.sort();
                        !reported.is_empty() && reported != configured
                    };
                    if drift {
                        if !health.is_drifted(peer) {
                            health.observe_drift(peer, report.ring_members.clone(), &local);
                        }
                    } else {
                        health.observe_ok(peer, report, &local);
                    }
                }
                Ok(Err(err)) => {
                    log::debug!("transfer peer probe {} failed: {}", peer, err);
                    if health.observe_failure(peer) {
                        log::warn!("transfer peer {} failed {} probes in a row", peer, 3);
                    }
                }
                Err(_) => {
                    if health.observe_failure(peer) {
                        log::warn!(
                            "transfer peer {} timed out {} probes in a row",
                            peer,
                            crate::transfer::ownership::PEER_DEAD_AFTER
                        );
                    }
                }
            }
        }
        self.peer_health
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .finish_round();
        let drifted = self
            .peer_health
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .drifted_peers()
            .len();
        record_metric(|metrics| metrics.set_peer_config_drift(drifted));
        let _ = std::any::type_name::<PeerHealth>();
    }

    /// Jobs whose owner is definitively gone go straight into `Canceling` and
    /// have their tasks stopped — the first half of stop-then-run.
    ///
    /// Concurrency: two instances may sweep the same dead owner at once;
    /// `mark_canceling` is fenced by `where owner = X`, so the second update
    /// matches zero rows and does nothing. No extra coordination is needed.
    async fn settle_owner_lost_jobs(&self, now_ms: i64) -> FsResult<()> {
        use crate::transfer::ownership::PEER_DEAD_AFTER;

        let owned = match self.store.list_active_owned_jobs(256) {
            Ok(owned) => owned,
            Err(err) => {
                log::warn!(
                    "transfer ownership sweep could not list owned jobs: {}",
                    err
                );
                return Ok(());
            }
        };
        if owned.is_empty() {
            return Ok(());
        }
        let peers = self.peers.as_ref().clone();
        for (job_id, run_id, owner) in owned {
            if owner == self.owner {
                continue;
            }
            let alive = {
                let health = self.peer_health.lock().unwrap_or_else(|e| e.into_inner());
                health.owner_is_alive(&owner, &peers)
            };
            if alive {
                continue;
            }
            if !self
                .store
                .mark_canceling(&job_id, run_id, &self.owner, "owner_lost", now_ms)?
            {
                // Another instance got there first; the row is already moving.
                continue;
            }
            record_metric(|metrics| {
                metrics.inc_requeue("owner_lost");
                metrics.inc_owner_lost();
            });
            log::warn!(
                "transfer {} run {} owner {} is not reachable by any peer ({} failures each); \
                 canceling it and waiting for its tasks to stop",
                job_id,
                run_id,
                owner,
                PEER_DEAD_AFTER
            );
            let job = match self.store.get_transfer(&job_id)? {
                Some(job) => job,
                None => continue,
            };
            let lease = TransferLease {
                job_id: job.job_id.clone(),
                run_id: job.run_id,
                owner: self.owner.clone(),
                fence: job.run_id,
            };
            self.stop_recoverable_tasks(&job, &lease, "owner lost")
                .await?;
        }
        Ok(())
    }

    /// Jobs past their submit deadline go through the same stop-then-run
    /// machine with `ttl_expired`, which the convergence step refuses to revive.
    async fn settle_expired_jobs(&self, now_ms: i64) -> FsResult<()> {
        let expired = match self.store.list_expired_jobs(now_ms, 256) {
            Ok(expired) => expired,
            Err(err) => {
                log::warn!("transfer TTL sweep could not list expired jobs: {}", err);
                return Ok(());
            }
        };
        for (job_id, run_id) in expired {
            if !self
                .store
                .mark_canceling(&job_id, run_id, &self.owner, "ttl_expired", now_ms)?
            {
                continue;
            }
            record_metric(|metrics| metrics.inc_ttl_expired());
            log::warn!(
                "transfer {} run {} passed its deadline; canceling it",
                job_id,
                run_id
            );
            let job = match self.store.get_transfer(&job_id)? {
                Some(job) => job,
                None => continue,
            };
            let lease = TransferLease {
                job_id: job.job_id.clone(),
                run_id: job.run_id,
                owner: self.owner.clone(),
                fence: job.run_id,
            };
            self.stop_recoverable_tasks(&job, &lease, "ttl expired")
                .await?;
        }
        Ok(())
    }

    /// Second half of stop-then-run: a job in `Canceling` either waits (tasks
    /// still reporting somewhere) or converges into a re-plan / terminal
    /// failure. Waiting writes nothing at all; there is no timeout that forces a
    /// terminal state, only a warning when the wait gets long.
    async fn advance_canceling_jobs(&self, now_ms: i64) -> FsResult<()> {
        use crate::transfer::ownership::{cancel_outcome, CancelOutcome, CANCELING_WARN_AFTER};

        let owned = match self.store.list_active_owned_jobs(256) {
            Ok(owned) => owned,
            Err(_) => return Ok(()),
        };
        for (job_id, _run_id, owner) in owned {
            if owner != self.owner {
                continue;
            }
            let job = match self.store.get_transfer(&job_id)? {
                Some(job) => job,
                None => continue,
            };
            if job.state != TransferState::Canceling {
                continue;
            }
            let recoverable = self
                .store
                .has_recoverable_tasks(&job.job_id, job.run_id)
                .unwrap_or(true);
            // Only owner loss may start another run; a TTL expiry is final.
            let replan_allowed = job.cancel_reason != "ttl_expired";
            match cancel_outcome(recoverable, job.run_id, job.max_runs, replan_allowed) {
                CancelOutcome::Wait => {
                    let waited = now_ms.saturating_sub(job.updated_at);
                    record_metric(|metrics| metrics.set_canceling_wait_seconds(waited / 1000));
                    if waited >= CANCELING_WARN_AFTER.as_millis() as i64 {
                        log::warn!(
                            "transfer {} run {} has been cancelling for {}s (reason {}); \
                             still waiting for its tasks to stop",
                            job.job_id,
                            job.run_id,
                            waited / 1000,
                            if job.cancel_reason.is_empty() {
                                "unknown"
                            } else {
                                job.cancel_reason.as_str()
                            }
                        );
                    }
                }
                CancelOutcome::Revive => {
                    if self.store.revive_run(&job.job_id, job.run_id, now_ms)? {
                        log::info!(
                            "transfer {} run {} re-planning as run {} after owner loss",
                            job.job_id,
                            job.run_id,
                            job.run_id + 1
                        );
                    }
                }
                CancelOutcome::Fail => {
                    let reason = if job.cancel_reason.is_empty() {
                        "owner_lost"
                    } else {
                        job.cancel_reason.as_str()
                    };
                    if self
                        .store
                        .fail_job(&job.job_id, job.run_id, reason, now_ms)?
                    {
                        record_metric(|metrics| metrics.inc_job_failed(reason));
                        log::warn!(
                            "transfer {} run {} failed after {} runs: {}",
                            job.job_id,
                            job.run_id,
                            job.max_runs,
                            reason
                        );
                    }
                }
            }
        }
        Ok(())
    }

    async fn run_once(&self) -> FsResult<bool> {
        let now_ms = now_ms();
        // Store calls are blocking; keep them off the shared runtime workers.
        // The per-tick hot path (cleanup/counts/acquire/get) is bridged to the
        // blocking pool here; state handlers still run their own store calls
        // inline, but each one is now bounded by the MySQL socket timeout.
        // Ownership watchdog (CV-105 §3.2/§3.3): probe peers, then settle every
        // job whose owner stopped existing, then advance jobs that are waiting
        // for their workers to stop. The first tick after start runs this
        // unconditionally: a restarted instance's own previous jobs are owned by
        // its old (dead) instance id, so they arrive here through the very same
        // death-scan entry point — one state machine, two entry points.
        let boot_sweep = !self.boot_sweep_done.swap(true, Ordering::AcqRel);
        self.probe_peers(now_ms, boot_sweep).await;
        // Settle passes touch every owned job, so they run at the probe cadence
        // (5s) rather than at the tick rate (R2).
        let settle_due = boot_sweep
            || now_ms.saturating_sub(self.last_settle_ms.load(Ordering::Acquire) as i64)
                >= crate::transfer::ownership::PEER_PROBE_INTERVAL.as_millis() as i64;
        if settle_due {
            self.last_settle_ms.store(now_ms as u64, Ordering::Release);
            self.settle_owner_lost_jobs(now_ms).await?;
            self.settle_expired_jobs(now_ms).await?;
            self.advance_canceling_jobs(now_ms).await?;
        }

        Self::blocking(self, move |s| s.cleanup_terminal_transfers(now_ms)).await?;
        Self::blocking(self, move |s| s.record_job_counts_if_due(now_ms)).await?;
        // Fairness: every N ticks, prefer acquiring a brand-new Pending job
        // over continuing to service this scheduler's already-held jobs.
        // Without this, a scheduler holding many long-running jobs services
        // them forever and a new Pending job starves (observed: a Pending job
        // waited 34 minutes while 19 existing jobs were dispatched).
        let tick = self.acquire_tick.fetch_add(1, Ordering::Relaxed);
        let prefer_pending = tick.is_multiple_of(ACQUIRE_PENDING_EVERY_N_TICKS);
        let lease_timeout = duration_ms(self.conf.lease_timeout);
        // Read the live ceiling, not the file value: lowering `max_running_transfers`
        // must affect the very next acquire, and it must never abort a running job.
        let max_executing = self.runtime_config.max_running_transfers() as u64;
        // CV-105: one CAS claim, no lease and no schema lock row. `lease_timeout`
        // is still passed for backends that have not switched to the narrow
        // claim query yet (postgres/SQLite), and disappears with them.
        let after = self
            .retick_cursor
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        let acquired = Self::blocking(self, move |s| {
            s.store.claim_next_job(
                &s.owner,
                now_ms,
                max_executing,
                prefer_pending,
                lease_timeout,
                &after,
            )
        })
        .await?;
        let Some(lease) = acquired else {
            // Nothing after the cursor: restart the rotation at the beginning.
            *self.retick_cursor.lock().unwrap_or_else(|e| e.into_inner()) = String::new();
            record_metric(|metrics| metrics.inc_acquire("empty"));
            return Ok(false);
        };
        if lease.owner == self.owner {
            *self.retick_cursor.lock().unwrap_or_else(|e| e.into_inner()) = lease.job_id.clone();
        }
        record_metric(|metrics| metrics.inc_acquire("ok"));

        let job_id = lease.job_id.clone();
        let job = Self::blocking(self, move |s| s.store.get_transfer(&job_id)).await?;
        let Some(job) = job else {
            record_metric(|metrics| metrics.inc_acquire("missing_job"));
            return Ok(true);
        };

        // Only a user cancellation owns this path. An owner-lost / TTL job is in
        // Canceling too, but its exit is decided by `advance_canceling_jobs`
        // (re-plan or fail); letting it run through `cancel_transfer` here raced
        // with that decision and could turn it into a plain Canceled job.
        if job.cancel_requested && (job.cancel_reason.is_empty() || job.cancel_reason == "user") {
            self.cancel_transfer(&job, &lease).await?;
            return Ok(true);
        }

        // Placement: submit relays a job to the instance the ring names, and
        // that instance is the only one holding its in-memory task state. If
        // this instance merely won the lease race, hand the job straight back
        // so the owner picks it up on its next tick.
        if self.hand_back_to_placement(&job, &lease, now_ms)? {
            return Ok(true);
        }

        // Record the acting owner so status/watch can relay by address.
        let mut job = job;
        let self_addr = self
            .forwarder
            .as_ref()
            .map(|forwarder| forwarder.self_addr().to_string());
        if let Some(addr) = self_addr {
            if job.owner_addr != addr {
                // Persist, don't just mutate: the status relay and the console's
                // "running on" both read the stored row, not this tick's copy.
                // Without the write those backends kept an empty `owner_addr`
                // forever, which made every SQL-backed job look unowned.
                self.store.set_owner_addr(&job.job_id, job.run_id, &addr)?;
                job.owner_addr = addr;
            }
        }

        match job.state {
            TransferState::Pending | TransferState::Planning => {
                self.plan_transfer(job, lease).await?;
            }
            TransferState::Dispatching => {
                self.dispatch_pending_tasks(job, lease).await?;
            }
            TransferState::Running => {
                self.check_running_transfer(job, lease).await?;
            }
            TransferState::Canceling => {
                self.cancel_transfer(&job, &lease).await?;
            }
            TransferState::Completed
            | TransferState::Failed
            | TransferState::Canceled
            | TransferState::PartialSuccess => {}
        }
        Ok(true)
    }

    /// Returns the lease to the instance placement names, or false when this
    /// instance should plan the job itself.
    ///
    /// Placement was recorded at submit; the stored address is compared rather
    /// than a locally recomputed hash, so two instances with different views
    /// still agree on who should run this job. Addresses are compared after
    /// resolution (`owns_addr`), because `self_addr` comes from
    /// `transfer.hostname` (often a bound IP) while the ring holds
    /// `transfer.endpoints` verbatim (often DNS names); a plain string compare
    /// between the two never matches and livelocks the job between instances.
    ///
    /// When this instance is not in the ring at all no hand-back can ever land,
    /// so the job is planned here instead of being returned forever. The same
    /// applies once the placement target has ignored
    /// [`MAX_PLACEMENT_HAND_BACKS`] consecutive hand-backs: the address may be
    /// dead, decommissioned, or simply not this instance's business any more,
    /// and a job must never be parked in Pending because of it.
    fn hand_back_to_placement(
        &self,
        job: &TransferJobRecord,
        lease: &TransferLease,
        now_ms: i64,
    ) -> FsResult<bool> {
        if job.state != TransferState::Planning {
            return Ok(false);
        }
        let Some(forwarder) = &self.forwarder else {
            return Ok(false);
        };
        let target = if job.placement_addr.is_empty() {
            forwarder.owner_of(&job.client_request_id)
        } else {
            Some(job.placement_addr.clone())
        };
        let Some(target) = target.filter(|addr| !forwarder.owns_addr(addr)) else {
            return Ok(false);
        };
        if !forwarder.in_ring() {
            warn!(
                "this instance {} is not listed in transfer.endpoints; planning job {} locally instead of handing it back to {}",
                forwarder.self_addr(),
                job.job_id,
                target
            );
            return Ok(false);
        }
        let attempts = self.note_placement_hand_back(job);
        if attempts > MAX_PLACEMENT_HAND_BACKS {
            warn!(
                "placement {} did not claim job {} after {} hand-backs; planning it on {} instead",
                target,
                job.job_id,
                attempts.saturating_sub(1),
                forwarder.self_addr()
            );
            record_metric(|metrics| {
                metrics.inc_planning(transfer_kind_label(job.kind), "takeover")
            });
            return Ok(false);
        }
        record_metric(|metrics| metrics.inc_requeue("placed_elsewhere"));
        self.store.requeue_transfer(TransferRequeueUpdate {
            job_id: job.job_id.clone(),
            run_id: job.run_id,
            owner: lease.owner.clone(),
            fence: lease.fence,
            message: job.summary.message.clone(),
            next_attempt_at_ms: now_ms,
            mount_snapshot_attempts: None,
            planning_attempts: None,
            now_ms,
        })?;
        Ok(true)
    }

    /// Counts consecutive hand-backs for one job. The counter is kept above the
    /// limit once reached (see [`MAX_PLACEMENT_HAND_BACKS`]) so the decision
    /// stays made: dropping it would restart the count on the next acquire and
    /// leak five more hand-backs before every local attempt.
    ///
    /// Reruns get a fresh `run_id` and therefore a fresh count, which is correct
    /// because the rerun row carries a new placement.
    fn note_placement_hand_back(&self, job: &TransferJobRecord) -> u32 {
        if self.placement_hand_backs.len() > HAND_BACK_LEDGER_MAX_ENTRIES {
            self.placement_hand_backs.clear();
        }
        let mut attempts = self
            .placement_hand_backs
            .entry(placement_hand_back_key(job))
            .or_insert(0);
        *attempts = attempts.saturating_add(1);
        *attempts
    }

    fn idle_interval(&self) -> Duration {
        scheduler_idle_interval(self.conf.lease_timeout)
    }

    /// Runs a blocking store interaction on the runtime's blocking pool.
    /// Mirrors `job_mount_snapshot_with_version_async`: a blocking closure is
    /// awaited from async context so the shared runtime workers never stall
    /// on synchronous store I/O.
    async fn blocking<F, R>(scheduler: &Self, task: F) -> FsResult<R>
    where
        F: FnOnce(&Self) -> FsResult<R> + Send + 'static,
        R: Send + 'static,
    {
        let scheduler = scheduler.clone();
        tokio::task::spawn_blocking(move || task(&scheduler))
            .await
            .map_err(|_| FsError::common("transfer scheduler blocking task panicked"))?
    }

    fn transfer_counts(&self) -> FsResult<(u64, u64)> {
        Ok((
            self.store.count_active_transfers()?,
            self.store.count_executing_transfers()?,
        ))
    }

    fn record_job_counts(&self, active: u64, executing: u64) {
        if let Ok(metrics) = TransferMetrics::get() {
            metrics.set_job_counts(active, executing);
        }
    }

    fn record_job_counts_if_due(&self, now_ms: i64) -> FsResult<()> {
        let now = now_ms.max(0) as u64;
        let last = self.last_metrics_ms.load(Ordering::Relaxed);
        if now.saturating_sub(last) < TRANSFER_METRICS_INTERVAL_MS
            || self
                .last_metrics_ms
                .compare_exchange(last, now, Ordering::Relaxed, Ordering::Relaxed)
                .is_err()
        {
            return Ok(());
        }
        let (active, executing) = match self.transfer_counts() {
            Ok(counts) => counts,
            Err(err) => {
                self.last_metrics_ms.store(0, Ordering::Relaxed);
                return Err(err);
            }
        };
        self.record_job_counts(active, executing);
        self.record_dispatch_capacity();
        // Global task-state counts come from a full-range GROUP BY on
        // transfer_tasks plus the tasks this instance holds only in memory
        // (file-level tasks never get a row). The deltas above are smoothed
        // estimates; this is the correction, and it has to run often enough
        // that drift cannot grow into a wrong dashboard for an hour.
        let now = now_ms.max(0) as u64;
        let last_tasks = self.last_task_counts_ms.load(Ordering::Relaxed);
        if now.saturating_sub(last_tasks) >= TRANSFER_TASK_COUNTS_INTERVAL_MS
            && self
                .last_task_counts_ms
                .compare_exchange(last_tasks, now, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
        {
            match self.calibrated_task_counts(now_ms) {
                Ok(counts) => {
                    if let Ok(metrics) = TransferMetrics::get() {
                        metrics.set_task_counts(&counts);
                    }
                }
                Err(err) => {
                    self.last_task_counts_ms.store(0, Ordering::Relaxed);
                    warn!("count_tasks_global failed: {}", err);
                }
            }
        }
        Ok(())
    }

    /// Reports the two dispatch ceilings' consistency, once per metrics tick.
    ///
    /// Both knobs are the transfer's own runtime configuration, so this is the
    /// only place that can notice they contradict each other: if
    /// `count_ceiling x task_bytes` exceeds `byte_ceiling`, the byte gate closes
    /// first and the count knob silently stops meaning anything. The metric is
    /// the signal; the numbers go to the log on transition only.
    fn record_dispatch_capacity(&self) {
        let count_ceiling = self.runtime_config.worker_task_limit() as i64;
        let byte_ceiling = self.runtime_config.worker_inflight_budget_bytes();
        let task_bytes = match self.inflight.max_task_bytes() {
            0 => TransferConf::DEFAULT_TASK_ESTIMATED_BYTES,
            seen => seen,
        };
        let mismatch = count_ceiling > 0
            && byte_ceiling > 0
            && count_ceiling.saturating_mul(task_bytes) > byte_ceiling;
        if let Ok(metrics) = TransferMetrics::get() {
            metrics.set_dispatch_capacity(mismatch, task_bytes);
        }
        let previous = self.capacity_mismatch.swap(mismatch, Ordering::Relaxed);
        if mismatch && !previous {
            warn!(
                "transfer dispatch ceilings contradict each other: worker_max_concurrent_tasks={} x largest task estimate {} B exceeds worker_inflight_budget_bytes={} B, so the real ceiling is ~{} tasks per worker. Raise the budget or lower the count",
                count_ceiling,
                task_bytes,
                byte_ceiling,
                if task_bytes > 0 { byte_ceiling / task_bytes } else { 0 }
            );
        }
        if !mismatch && previous {
            info!(
                "transfer dispatch ceilings are consistent again: worker_max_concurrent_tasks={} x {} B <= worker_inflight_budget_bytes={} B",
                count_ceiling, task_bytes, byte_ceiling
            );
        }
    }

    /// Task counts as the gauges should report them: persisted rows plus the
    /// file-level tasks that only exist in this process's cache.
    ///
    /// File-level tasks never get a `transfer_tasks` row, so a store-only scan
    /// reports zero for a job that is actively copying thousands of files.
    fn calibrated_task_counts(&self, now_ms: i64) -> FsResult<TransferTaskCounts> {
        let mut counts = self.store.count_tasks_global()?;
        let cached = self.task_cache.counts(now_ms);
        counts.pending += cached.pending;
        counts.running += cached.running;
        counts.completed += cached.completed;
        counts.failed += cached.failed;
        counts.canceled += cached.canceled;
        counts.stale += cached.stale;
        counts.completed_size += cached.completed_size;
        Ok(counts)
    }

    fn cleanup_terminal_transfers(&self, tick_now_ms: i64) -> FsResult<()> {
        let now = tick_now_ms.max(0) as u64;
        let last = self.last_cleanup_ms.load(Ordering::Relaxed);
        if now.saturating_sub(last) < TRANSFER_CLEANUP_INTERVAL_MS {
            return Ok(());
        }
        if self
            .last_cleanup_ms
            .compare_exchange(last, now, Ordering::Relaxed, Ordering::Relaxed)
            .is_err()
        {
            return Ok(());
        }
        let retention_ms =
            i64::try_from(self.conf.terminal_retention.as_millis()).unwrap_or(i64::MAX);
        let task_retention_ms =
            i64::try_from(self.conf.task_retention.as_millis()).unwrap_or(i64::MAX);
        let task_older_than = tick_now_ms.saturating_sub(task_retention_ms);
        let job_older_than = tick_now_ms.saturating_sub(retention_ms);
        // Renew immediately before each bounded delete phase. The Store also
        // locks and validates this lease in the delete transaction; renewal
        // keeps a slow first phase from making the second phase run on an
        // already-expired lease.
        let task_cleanup_now_ms = now_ms();
        if !self.store.try_acquire_cleanup_lease(
            &self.owner,
            task_cleanup_now_ms,
            TRANSFER_CLEANUP_LEASE_MS,
        )? {
            return Ok(());
        }
        let purged_tasks = self.store.purge_terminal_tasks_with_cleanup_lease(
            task_older_than,
            TERMINAL_TASK_PURGE_ROW_LIMIT,
            &self.owner,
            task_cleanup_now_ms,
        )?;
        let job_cleanup_now_ms = now_ms();
        let purged_jobs = if self.store.try_acquire_cleanup_lease(
            &self.owner,
            job_cleanup_now_ms,
            TRANSFER_CLEANUP_LEASE_MS,
        )? {
            self.store.purge_terminal_jobs_with_cleanup_lease(
                job_older_than,
                TERMINAL_JOB_PURGE_LIMIT,
                &self.owner,
                job_cleanup_now_ms,
            )?
        } else {
            0
        };
        if purged_tasks >= TERMINAL_TASK_PURGE_ROW_LIMIT || purged_jobs >= TERMINAL_JOB_PURGE_LIMIT
        {
            self.last_cleanup_ms.store(0, Ordering::Relaxed);
        }
        if purged_jobs > 0 {
            if let Ok(metrics) = TransferMetrics::get() {
                metrics.inc_cleanup_purged(purged_jobs);
                metrics.inc_cleanup_purged_tasks(purged_tasks);
            }
        } else if purged_tasks > 0 {
            if let Ok(metrics) = TransferMetrics::get() {
                metrics.inc_cleanup_purged_tasks(purged_tasks);
            }
        }
        if purged_tasks > 0 || purged_jobs > 0 {
            info!(
                "purged {} terminal transfer tasks (older than {}) and {} jobs (older than {})",
                purged_tasks, task_older_than, purged_jobs, job_older_than
            );
        }
        Ok(())
    }

    async fn plan_transfer(
        &self,
        mut job: TransferJobRecord,
        lease: TransferLease,
    ) -> FsResult<()> {
        let resuming_planning = job.state == TransferState::Planning;
        if !self.transition(
            &job,
            &lease,
            &[TransferState::Pending, TransferState::Planning],
            TransferState::Planning,
            "planning transfer",
        )? {
            record_metric(|metrics| metrics.inc_planning(transfer_kind_label(job.kind), "stale"));
            return Ok(());
        }

        // Observability only: how long the job waited in Pending before its
        // first planning pass. A resumed Planning attempt is not queue time, so
        // only the Pending -> Planning edge is observed. This is the term that
        // explains "why is my transfer taking so long" when the executing-slot
        // cap or the worker byte budget is saturated.
        if !resuming_planning {
            let wait_ms = now_ms().saturating_sub(job.created_at);
            let kind = transfer_kind_label(job.kind);
            record_metric(|metrics| metrics.observe_job_pending_wait(kind, wait_ms));
        }

        if resuming_planning
            && (self.store.has_transfer_tasks(&job.job_id, job.run_id)?
                || !self.task_cache.is_empty(&job.job_id, job.run_id))
        {
            let err = FsError::common(
                "interrupted task-plan persistence cannot be safely recomputed; retry transfer",
            );
            self.fail_transfer(
                &job,
                &lease,
                &[TransferState::Planning],
                transfer_failure_message(
                    job.kind,
                    "planning",
                    &job.source_path,
                    &job.target_path,
                    &err,
                ),
            )?;
            return Err(err);
        }

        if job.mount_snapshot_json.trim().is_empty() || job.mount_snapshot_json == "{}" {
            let snapshot = match job_mount_snapshot_with_version_async(&job, &self.cache).await {
                Ok(snapshot) => Ok(snapshot),
                Err(MountSnapshotError::Retryable(err)) => {
                    match mount_snapshot_failure_action(job.mount_snapshot_attempts) {
                        MountSnapshotFailureAction::Requeue {
                            retry_count,
                            delay_ms,
                        } => {
                            let now = now_ms();
                            let requeued = self.store.requeue_transfer(TransferRequeueUpdate {
                                job_id: job.job_id.clone(),
                                run_id: job.run_id,
                                owner: lease.owner.clone(),
                                fence: lease.fence,
                                message: format!("mount snapshot refresh delayed: {}", err),
                                next_attempt_at_ms: now.saturating_add(delay_ms),
                                mount_snapshot_attempts: Some(retry_count),
                                planning_attempts: None,
                                now_ms: now,
                            })?;
                            if requeued {
                                record_metric(|metrics| {
                                    metrics.inc_planning(transfer_kind_label(job.kind), "retry")
                                });
                                info!(
                                    "transfer mount snapshot requeued job_id={} run_id={} kind={:?} tenant={} owner={} fence={} retry_count={} next_attempt_at_ms={} reason={}",
                                    job.job_id,
                                    job.run_id,
                                    job.kind,
                                    job.tenant,
                                    lease.owner,
                                    lease.fence,
                                    retry_count,
                                    now.saturating_add(delay_ms),
                                    err
                                );
                            }
                            return Ok(());
                        }
                        MountSnapshotFailureAction::Fail => Err(err),
                    }
                }
                Err(MountSnapshotError::Terminal(err)) => Err(err),
            };
            let (mount, version) = match snapshot {
                Ok(snapshot) => snapshot,
                Err(err) => {
                    record_metric(|metrics| {
                        metrics.inc_planning(transfer_kind_label(job.kind), "failure")
                    });
                    self.fail_transfer(
                        &job,
                        &lease,
                        &[TransferState::Planning],
                        transfer_failure_message(
                            job.kind,
                            "planning",
                            &job.source_path,
                            &job.target_path,
                            &err,
                        ),
                    )?;
                    return Err(err);
                }
            };
            let snapshot_json = match serde_json::to_string(&mount) {
                Ok(json) => json,
                Err(err) => {
                    let err = FsError::common(format!(
                        "Unable to serialize transfer mount snapshot: {}",
                        err
                    ));
                    record_metric(|metrics| {
                        metrics.inc_planning(transfer_kind_label(job.kind), "failure")
                    });
                    self.fail_transfer(
                        &job,
                        &lease,
                        &[TransferState::Planning],
                        transfer_failure_message(
                            job.kind,
                            "planning",
                            &job.source_path,
                            &job.target_path,
                            &err,
                        ),
                    )?;
                    return Err(err);
                }
            };
            if !self
                .store
                .set_transfer_mount_snapshot(TransferMountSnapshotUpdate {
                    job_id: job.job_id.clone(),
                    run_id: job.run_id,
                    owner: lease.owner.clone(),
                    fence: lease.fence,
                    mount_snapshot_json: snapshot_json.clone(),
                    cluster_snapshot_version: version,
                    now_ms: now_ms(),
                })?
            {
                warn!(
                    "stop planning transfer {} because mount snapshot could not be persisted",
                    job.job_id
                );
                return Ok(());
            }
            job.mount_snapshot_json = snapshot_json;
            job.cluster_snapshot_version = version;
        }

        let Some(planning_result) = self
            .await_with_planning_lease(&lease, self.planner.plan(&job))
            .await?
        else {
            return Ok(());
        };
        let planned = match planning_result {
            Ok(planned) => planned,
            Err(err @ FsError::InProgress(_)) => {
                record_metric(|metrics| {
                    metrics.inc_planning(transfer_kind_label(job.kind), "retry")
                });
                let now = now_ms();
                let requeued = self.store.requeue_transfer(TransferRequeueUpdate {
                    job_id: job.job_id.clone(),
                    run_id: job.run_id,
                    owner: lease.owner.clone(),
                    fence: lease.fence,
                    message: format!("planning delayed: {}", err),
                    next_attempt_at_ms: now.saturating_add(PLANNING_REQUEUE_DELAY_MS),
                    mount_snapshot_attempts: None,
                    planning_attempts: None,
                    now_ms: now,
                })?;
                if requeued {
                    info!(
                        "transfer state requeue job_id={} run_id={} kind={:?} tenant={} owner={} fence={} state={:?} next_attempt_at_ms={} reason={}",
                        job.job_id,
                        job.run_id,
                        job.kind,
                        job.tenant,
                        lease.owner,
                        lease.fence,
                        job.state,
                        now.saturating_add(PLANNING_REQUEUE_DELAY_MS),
                        err
                    );
                }
                return Ok(());
            }
            Err(err) if is_planning_retryable(&err) => {
                match planning_retry_attempt(job.planning_attempts) {
                    Some(retry_count) => {
                        let now = now_ms();
                        let delay_ms =
                            planning_retry_delay_ms(job.planning_attempts, &job.job_id, now);
                        record_metric(|metrics| {
                            metrics.inc_planning(transfer_kind_label(job.kind), "retry")
                        });
                        let requeued = self.store.requeue_transfer(TransferRequeueUpdate {
                            job_id: job.job_id.clone(),
                            run_id: job.run_id,
                            owner: lease.owner.clone(),
                            fence: lease.fence,
                            message: format!(
                                "planning throttled, retry {}/{}: {}",
                                retry_count, PLANNING_LIST_MAX_RETRIES, err
                            ),
                            next_attempt_at_ms: now.saturating_add(delay_ms),
                            mount_snapshot_attempts: None,
                            planning_attempts: Some(retry_count),
                            now_ms: now,
                        })?;
                        if requeued {
                            info!(
                                "transfer planning throttled job_id={} run_id={} kind={:?} tenant={} owner={} fence={} retry_count={} next_attempt_at_ms={} reason={}",
                                job.job_id,
                                job.run_id,
                                job.kind,
                                job.tenant,
                                lease.owner,
                                lease.fence,
                                retry_count,
                                now.saturating_add(delay_ms),
                                err
                            );
                        }
                        return Ok(());
                    }
                    None => {
                        record_metric(|metrics| {
                            metrics.inc_planning(transfer_kind_label(job.kind), "failure")
                        });
                        self.fail_transfer(
                            &job,
                            &lease,
                            &[TransferState::Planning],
                            transfer_failure_message(
                                job.kind,
                                "planning",
                                &job.source_path,
                                &job.target_path,
                                &err,
                            ),
                        )?;
                        return Err(err);
                    }
                }
            }
            Err(err) => {
                record_metric(|metrics| {
                    metrics.inc_planning(transfer_kind_label(job.kind), "failure")
                });
                self.fail_transfer(
                    &job,
                    &lease,
                    &[TransferState::Planning],
                    transfer_failure_message(
                        job.kind,
                        "planning",
                        &job.source_path,
                        &job.target_path,
                        &err,
                    ),
                )?;
                return Err(err);
            }
        };

        if let Some(cv_metadata_epoch) = planned.cv_metadata_epoch {
            let updated = self.store.set_transfer_cv_metadata_epoch(
                &job.job_id,
                job.run_id,
                &lease.owner,
                lease.fence,
                cv_metadata_epoch,
                now_ms(),
            )?;
            if !updated {
                warn!(
                    "stop planning export transfer {} because cv metadata epoch {} could not be persisted",
                    job.job_id, cv_metadata_epoch
                );
                return Ok(());
            }
        }

        if planned.tasks.is_empty() {
            let message = format!(
                "incremental transfer complete: skipped_files={} skipped_size={}",
                planned.skipped_files, planned.skipped_size
            );
            // Nothing was planned because every target was already canonical,
            // which is the normal outcome of a re-plan after a failover or a
            // repeated load. A zero-progress terminal snapshot would read as
            // "copied nothing", so report what was already in place. This
            // branch has no tasks by definition, so the numbers are files and
            // cannot be confused with task counts.
            if planned.skipped_files > 0 {
                self.store.update_terminal_snapshot(
                    &job.job_id,
                    job.run_id,
                    curvine_model::TransferProgress {
                        loaded_size: planned.skipped_size,
                        total_size: planned.skipped_size,
                        update_time: now_ms(),
                        message: message.clone(),
                    },
                    curvine_model::TransferTaskCounts {
                        completed: planned.skipped_files as u64,
                        completed_size: planned.skipped_size,
                        ..Default::default()
                    },
                    now_ms(),
                )?;
            }
            self.transition(
                &job,
                &lease,
                &[TransferState::Planning],
                TransferState::Completed,
                &message,
            )?;
            record_metric(|metrics| {
                metrics.inc_planning(transfer_kind_label(job.kind), "empty");
                metrics.inc_terminal("completed", "empty");
            });
            info!(
                "transfer {} completed with no file tasks: skipped_files={} skipped_size={}",
                job.job_id, planned.skipped_files, planned.skipped_size
            );
            return Ok(());
        }

        let task_count = planned.tasks.len();
        let total_size = planned.total_size;
        let skipped_files = planned.skipped_files;
        let skipped_size = planned.skipped_size;
        let job_info = planned.job_info;
        let message = format!(
            "planned total_size={total_size} skipped_files={skipped_files} skipped_size={skipped_size}"
        );
        let batch_size = self
            .conf
            .planning_batch_size()
            .clamp(1, MAX_TASKS_PER_STORE_TRANSACTION);
        // Segment tasks keep their rows (they pre-allocate a shared target and
        // resume mid-file); file-level tasks go to the in-memory cache only.
        // Replaying a file-level task is just re-copying the file, so a row per
        // task would buy nothing but a write.
        let (cached_tasks, persisted_tasks): (Vec<_>, Vec<_>) =
            planned.tasks.iter().cloned().partition(|task| {
                !matches!(
                    classify_transfer_plan(&task.source_read_plan_json),
                    TransferPlanKind::Segment | TransferPlanKind::SegmentGroup
                )
            });
        self.task_cache.seed(&job.job_id, job.run_id, cached_tasks);

        for tasks in persisted_tasks.chunks(batch_size) {
            let appended = self.store.append_planned_tasks(TransferPlannedTasks {
                job_id: job.job_id.clone(),
                run_id: job.run_id,
                owner: lease.owner.clone(),
                fence: lease.fence,
                tasks: tasks.to_vec(),
                message: String::new(),
                now_ms: now_ms(),
            })?;
            if !appended {
                warn!(
                    "stop planning transfer {} because owner {} lease epoch {} is stale",
                    job.job_id, lease.owner, lease.fence
                );
                return Ok(());
            }
        }
        let Some(preparation_result) = self
            .await_with_planning_lease(
                &lease,
                self.prepare_transfer_targets(&planned.tasks, job_info.overwrite.unwrap_or(false)),
            )
            .await?
        else {
            return Ok(());
        };
        if let Err(err) = preparation_result {
            self.fail_transfer(
                &job,
                &lease,
                &[TransferState::Planning],
                transfer_failure_message(
                    job.kind,
                    "planning",
                    &job.source_path,
                    &job.target_path,
                    &err,
                ),
            )?;
            return Err(err);
        }
        let persisted = self.store.persist_planned_tasks(TransferPlannedTasks {
            job_id: job.job_id.clone(),
            run_id: job.run_id,
            owner: lease.owner.clone(),
            fence: lease.fence,
            tasks: Vec::new(),
            message,
            now_ms: now_ms(),
        })?;
        if !persisted {
            warn!(
                "stop planning transfer {} because owner {} lease epoch {} is stale",
                job.job_id, lease.owner, lease.fence
            );
            return Ok(());
        }
        debug!(
            "transfer {} planned with job info source={}, target={}",
            job.job_id, job_info.source_path, job_info.target_path
        );
        record_metric(|metrics| {
            metrics.inc_planning(transfer_kind_label(job.kind), "success");
            metrics.inc_dispatch("planned_tasks", task_count);
            metrics.task_planned(task_count);
        });

        self.dispatch_pending_tasks(job, lease).await
    }

    /// Planning work no longer needs a lease heartbeat: ownership is
    /// process-scoped, so a long plan simply keeps running (CV-105 §3.1).
    async fn await_with_planning_lease<T, F>(
        &self,
        _lease: &TransferLease,
        work: F,
    ) -> FsResult<Option<T>>
    where
        F: Future<Output = T>,
    {
        Ok(Some(work.await))
    }

    /// Remove targets that need replacement only after all plan rows are
    /// durable while the job remains Planning. This covers shared segment
    /// targets and incremental-load files with mismatched replicas.
    async fn prepare_transfer_targets(
        &self,
        tasks: &[TransferTaskRecord],
        overwrite: bool,
    ) -> FsResult<()> {
        let (segment_targets, replacement_targets) = planned_target_replacements(tasks)?;
        for target_path in &segment_targets {
            let target = Path::from_str(target_path)?;
            if !target.is_cv() {
                continue;
            }
            let start = std::time::Instant::now();
            let status = self.fs.get_status(&target).await;
            record_master_rpc(
                "target_prepare",
                "get_file_status",
                if status.is_ok() { "success" } else { "error" },
                start,
            );
            match status {
                Ok(status) if status.is_dir => {
                    return Err(FsError::common(format!(
                        "Transfer target {} is a directory; refusing to overwrite directory with file",
                        target.full_path()
                    )));
                }
                Ok(_) if !overwrite && !replacement_targets.contains(target_path) => {
                    return Err(FsError::file_exists(target.full_path()));
                }
                Ok(_) => {
                    self.fs.delete(&target, false).await?;
                }
                Err(FsError::FileNotFound(_)) => {}
                Err(err) => return Err(err),
            }
        }
        for target_path in replacement_targets {
            if segment_targets.contains(&target_path) {
                continue;
            }
            let target = Path::from_str(&target_path)?;
            if !target.is_cv() {
                continue;
            }
            let start = std::time::Instant::now();
            let status = self.fs.get_status(&target).await;
            record_master_rpc(
                "target_prepare",
                "get_file_status",
                if status.is_ok() { "success" } else { "error" },
                start,
            );
            match status {
                Ok(status) if status.is_dir => {
                    return Err(FsError::common(format!(
                        "Transfer target {} is a directory; refusing to replace directory with file",
                        target.full_path()
                    )));
                }
                Ok(_) => {
                    self.fs.delete(&target, false).await?;
                }
                Err(FsError::FileNotFound(_)) => {}
                Err(err) => return Err(err),
            }
        }
        Ok(())
    }

    async fn dispatch_pending_tasks(
        &self,
        job: TransferJobRecord,
        lease: TransferLease,
    ) -> FsResult<()> {
        let mut dispatched = 0usize;
        let mut blocked_total = 0usize;
        let tick_started = std::time::Instant::now();
        // Worker capacity comes from the workers themselves (each submit response
        // carries their live load), so the tick never queries the task table.
        // A stale snapshot only aims at a worker that is already full, which
        // rejects with `worker_at_capacity` and lands in the cooldown below.
        // File-level tasks live only in this process. If a restart (or a
        // failover) left a Dispatching/Running job with no tasks at all, the
        // work was never planned into the store: re-plan from the source
        // rather than letting an empty task set look like "everything done".
        // Re-planning is safe because a file-level replay is a full re-copy.
        if job.state != TransferState::Planning
            && self.task_cache.is_empty(&job.job_id, job.run_id)
            && self
                .store
                .claim_pending_tasks(&job.job_id, job.run_id, 1)?
                .is_empty()
            && !self.store.has_transfer_tasks(&job.job_id, job.run_id)?
        {
            warn!(
                "re-planning transfer {} because no task state survived the restart",
                job.job_id
            );
            self.requeue_for_replan(&job, &lease)?;
            return Ok(());
        }
        loop {
            // Fairness: never let one job's task set monopolize the tick.
            if dispatched >= SCHEDULER_TICK_TASK_LIMIT
                || tick_started.elapsed() >= SCHEDULER_TICK_BUDGET
            {
                break;
            }
            if self.cancel_requested(&job, &lease).await? {
                return Ok(());
            }
            let limit = self
                .conf
                .planning_batch_size()
                .min(SCHEDULER_TICK_TASK_LIMIT.saturating_sub(dispatched))
                .max(1);
            let mut tasks = self
                .store
                .claim_pending_tasks(&job.job_id, job.run_id, limit)?;
            if tasks.len() < limit {
                // File-level tasks live only in memory; top the batch up.
                tasks.extend(self.task_cache.claim_pending(
                    &job.job_id,
                    job.run_id,
                    limit - tasks.len(),
                    now_ms(),
                ));
            }
            if tasks.is_empty() {
                break;
            }

            let (started, blocked) = match self.dispatch_batch(&job, &lease, tasks).await {
                Ok((started, blocked)) => (started, blocked),
                Err(err) => {
                    record_metric(|metrics| metrics.inc_dispatch("failure", 1));
                    if is_store_unavailable_error(&err) {
                        warn!(
                            "pause dispatching transfer {} because transfer store is unavailable: {}",
                            job.job_id, err
                        );
                        return Err(err);
                    }
                    self.fail_transfer(
                        &job,
                        &lease,
                        &[TransferState::Dispatching],
                        transfer_failure_message(
                            job.kind,
                            "planning",
                            &job.source_path,
                            &job.target_path,
                            &err,
                        ),
                    )?;
                    return Err(err);
                }
            };
            dispatched += started;
            blocked_total += blocked;
            record_metric(|metrics| metrics.inc_dispatch("started", started));
            if started == 0 {
                record_metric(|metrics| metrics.inc_dispatch("not_started", 1));
                break;
            }
        }

        if self.cancel_requested(&job, &lease).await? {
            return Ok(());
        }
        // "dispatched 0 tasks" explains nothing when tasks were blocked by
        // worker capacity; surface the real reason instead (and flush it so
        // the persisted message matches even before this transition).
        let message = dispatch_result_message(dispatched, blocked_total);
        if blocked_total > 0 {
            record_metric(|metrics| {
                metrics.inc_dispatch("blocked_no_worker", blocked_total);
            });
            let _ = self
                .store
                .flush_job_progress_message(&job.job_id, job.run_id, &message);
        }
        let _ = self.transition(
            &job,
            &lease,
            &[TransferState::Dispatching],
            TransferState::Running,
            message,
        )?;
        Ok(())
    }

    async fn cancel_requested(
        &self,
        job: &TransferJobRecord,
        lease: &TransferLease,
    ) -> FsResult<bool> {
        let Some(current) = self.store.get_transfer(&job.job_id)? else {
            return Ok(false);
        };
        if current.run_id != job.run_id || !current.cancel_requested {
            return Ok(false);
        }
        self.cancel_transfer(&current, lease).await?;
        Ok(true)
    }

    async fn dispatch_batch(
        &self,
        job: &TransferJobRecord,
        lease: &TransferLease,
        tasks: Vec<TransferTaskRecord>,
    ) -> FsResult<(usize, usize)> {
        let workers = match self.cache.live_workers_versioned() {
            Ok((version, workers)) if !workers.is_empty() => {
                // The decision below must use the same snapshot it was derived
                // from, so the ledger's base is advanced here and nowhere else.
                self.inflight.apply_snapshot(version, &workers);
                workers
            }
            Ok(_) | Err(_) => {
                // No candidate worker at all: the remaining tasks stay
                // Pending and the caller must see why.
                return Ok((0, tasks.len()));
            }
        };
        let mount = job_mount_snapshot_async(job, &self.cache).await?;
        let job_info = self.planner_job_info(job, &mount).await?;
        let limit = self.conf.worker_dispatch_concurrency();
        // Count tasks that could not be placed this round because no worker had
        // capacity (choose_worker returned None). They stay Pending in store and
        // are retried next round; this counter makes the backpressure visible.
        let mut blocked_no_worker = 0usize;

        let started = stream::iter(tasks.into_iter().filter_map(|task| {
            let estimated_bytes = self.task_estimated_bytes(&task);
            // No worker has room (design §4.2): drop the task from this batch
            // (stays Pending in store) so remaining tasks still get placed.
            let Some(worker) = self.choose_worker(&workers, estimated_bytes) else {
                blocked_no_worker += 1;
                return None;
            };
            let store = self.store.clone();
            let factory = self.factory.clone();
            let cache = self.cache.clone();
            let job_info = job_info.clone();
            let owner = lease.owner.clone();
            let fence = lease.fence;
            let report_endpoints = vec![self.self_report_target.clone()];
            let task_stale_timeout_ms = duration_ms(self.conf.task_stale_timeout);
            let task_cache = self.task_cache.clone();
            let worker_blacklist = self.worker_blacklist.clone();
            Some(async move {
                dispatch_one(DispatchTaskRequest {
                    store,
                    factory,
                    cache,
                    job_info,
                    owner,
                    fence,
                    task,
                    worker,
                    report_endpoints,
                    task_stale_timeout_ms,
                    task_cache,
                    worker_blacklist,
                })
                .await
            })
        }))
        .buffer_unordered(limit)
        .try_collect::<Vec<_>>()
        .await?;
        let started_count = started.into_iter().filter(|value| *value).count();
        Ok((started_count, blocked_no_worker))
    }

    /// Sends a job back to Planning so the planner rebuilds its task set.
    fn requeue_for_replan(&self, job: &TransferJobRecord, lease: &TransferLease) -> FsResult<()> {
        // `requeue_transfer` only accepts a Planning job, so an executing job
        // must be moved back first; otherwise the requeue silently no-ops.
        if job.state != TransferState::Planning {
            self.transition(
                job,
                lease,
                &[TransferState::Dispatching, TransferState::Running],
                TransferState::Planning,
                "re-planning: this instance holds no task state for the job",
            )?;
        }
        self.store.requeue_transfer(TransferRequeueUpdate {
            job_id: job.job_id.clone(),
            run_id: job.run_id,
            owner: lease.owner.clone(),
            fence: lease.fence,
            message: "requeued: task state was lost across a restart".to_string(),
            next_attempt_at_ms: now_ms(),
            mount_snapshot_attempts: None,
            planning_attempts: None,
            now_ms: now_ms(),
        })?;
        Ok(())
    }

    /// Store summary merged with file-level (cache) counts.
    fn task_summary(&self, job: &TransferJobRecord) -> FsResult<TransferTaskSummary> {
        let store = self
            .store
            .get_transfer_task_summary(&job.job_id, job.run_id, now_ms())?;
        Ok(merge_summaries(
            store,
            self.task_cache.summary(&job.job_id, job.run_id, now_ms()),
        ))
    }

    async fn planner_job_info(
        &self,
        job: &TransferJobRecord,
        mount: &curvine_model::MountInfo,
    ) -> FsResult<curvine_model::LoadJobInfo> {
        self.planner.load_job_info(job, mount).await
    }

    /// Redeliver transiently failed tasks instead of failing the job.
    /// Returns true when this recovery batch has retryable failures but no
    /// terminal failure. Budget and backoff reuse the stale-retry machinery
    /// so recovery cadence stays bounded.
    async fn recover_transient_failed_tasks(
        &self,
        job: &TransferJobRecord,
        lease: &TransferLease,
        now: i64,
    ) -> FsResult<bool> {
        let failed = self.store.list_failed_tasks(&job.job_id, job.run_id)?;
        if failed.iter().any(|task| {
            !is_task_retryable(task) || task.retry_count as usize >= self.conf.task_max_retries
        }) {
            return Ok(false);
        }

        // Every failure here is retryable and still inside the retry budget,
        // so the job must not fail this tick: tasks waiting out their backoff
        // window are simply not redelivered yet.
        let retryable_remaining = !failed.is_empty();
        // Same backoff cadence as stale redelivery: do not hammer a
        // throttled/limited substrate every tick.
        for task in failed
            .into_iter()
            .filter(|task| {
                let last_attempt_ms = task.progress.update_time.max(task.updated_at);
                now.saturating_sub(last_attempt_ms) >= stale_retry_backoff_ms(task.retry_count)
            })
            .take(self.conf.task_probe_concurrency())
        {
            if !self.store.update_task_state(TransferTaskStateUpdate {
                job_id: task.job_id.clone(),
                run_id: task.run_id,
                owner: lease.owner.clone(),
                fence: lease.fence,
                task_id: task.task_id.clone(),
                from_states: vec![TransferTaskState::Failed],
                state: TransferTaskState::Pending,
                message: format!("auto-retry transient failure: {}", task.progress.message),
                now_ms: now,
                bump_retry: true,
            })? {
                return Ok(retryable_remaining);
            }
            record_metric(|metrics| {
                metrics.inc_stale_retry("transient_retry");
                metrics.inc_requeue("transient_retry");
                // Failed→Pending: the task is queued again, so the gauge has to
                // leave `failed` and rejoin `pending` together with it.
                metrics.task_retried(1);
            });
        }
        Ok(retryable_remaining)
    }

    async fn check_running_transfer(
        &self,
        job: TransferJobRecord,
        lease: TransferLease,
    ) -> FsResult<()> {
        let now = now_ms();
        // A job in an executing state must have task state somewhere. File-level
        // tasks live only in the process that planned them, so after a failover
        // this instance sees neither cache entries nor rows. Concluding
        // "all tasks completed" from that would report success for work that
        // never ran: re-plan instead (a file-level replay is idempotent and the
        // planner skips targets that are already canonical).
        if self.task_cache.is_empty(&job.job_id, job.run_id)
            && !self.store.has_transfer_tasks(&job.job_id, job.run_id)?
        {
            warn!(
                "re-planning transfer {} because this instance holds no task state for it",
                job.job_id
            );
            self.requeue_for_replan(&job, &lease)?;
            return Ok(());
        }
        // File-level tasks live only in the cache, so every store-side guard
        // below (has_recoverable_tasks, claim_pending_tasks) is blind to them
        // and would complete the job with work still in flight. Handle that
        // task set here: recycle finished attempts, re-dispatch whatever is
        // pending, and report still-running work.
        if !self.task_cache.is_empty(&job.job_id, job.run_id) {
            let resumable = self
                .task_cache
                .claim_pending(&job.job_id, job.run_id, usize::MAX, now);
            if !resumable.is_empty() {
                self.transition(
                    &job,
                    &lease,
                    &[TransferState::Running],
                    TransferState::Dispatching,
                    "redispatch file-level tasks",
                )?;
                self.dispatch_pending_tasks(job, lease).await?;
                return Ok(());
            }
            let summary = self.task_summary(&job)?;
            if summary.counts.running > 0 {
                return Ok(());
            }
        }
        let missing_tasks = self.probe_stale_running_tasks(&job, now).await?;
        for (mut candidates, message) in [
            (
                missing_tasks.worker_session_gone,
                StaleTaskMissingReason::WorkerSessionGone.message(),
            ),
            (
                missing_tasks.task_attempt_missing,
                StaleTaskMissingReason::TaskAttemptMissing.message(),
            ),
        ] {
            for candidate in &mut candidates {
                candidate.progress.message = message.to_string();
            }
            for candidates in candidates.chunks(MAX_TASKS_PER_STORE_TRANSACTION) {
                let staled = self.store.mark_stale_attempts(
                    &job.job_id,
                    job.run_id,
                    &lease.owner,
                    lease.fence,
                    now,
                    candidates.to_vec(),
                )?;
                record_metric(|metrics| {
                    for _ in &staled {
                        metrics.task_staled();
                    }
                });
            }
        }
        // Recover all Stale tasks (newly marked + orphan leftovers from CAS races).
        let stale_tasks = self.store.list_tasks_by_state(
            &job.job_id,
            job.run_id,
            TransferTaskState::Stale,
            self.conf.task_probe_concurrency(),
        )?;
        for task in stale_tasks {
            if task.retry_count as usize > self.conf.task_max_retries {
                if !self.store.update_task_state(TransferTaskStateUpdate {
                    job_id: task.job_id.clone(),
                    run_id: task.run_id,
                    owner: lease.owner.clone(),
                    fence: lease.fence,
                    task_id: task.task_id.clone(),
                    from_states: vec![TransferTaskState::Stale],
                    state: TransferTaskState::Failed,
                    message: stale_retry_exhausted_message(&task),
                    now_ms: now,
                    bump_retry: false,
                })? {
                    return Ok(());
                }
                self.finish_failed_transfer(&job, &lease).await?;
                record_metric(|metrics| {
                    metrics.inc_stale_retry("exhausted");
                    metrics.task_stale_exhausted();
                });
                return Ok(());
            }
            // Backoff: do not redeliver a stale task immediately. The task was
            // stale because its worker did not report in time; hammering it
            // every scheduler tick (200ms) turns one bad worker into a
            // redispatch storm that starves other jobs (observed: 121
            // redispatch cycles in 35min blocked a new Pending job for 34min).
            // Delay grows exponentially with retry_count, capped.
            let backoff_ms = stale_retry_backoff_ms(task.retry_count);
            let last_attempt_ms = task.progress.update_time.max(task.updated_at);
            if now.saturating_sub(last_attempt_ms) < backoff_ms {
                record_metric(|metrics| metrics.inc_stale_retry("backoff"));
                continue;
            }
            if !self.store.update_task_state(TransferTaskStateUpdate {
                job_id: task.job_id.clone(),
                run_id: task.run_id,
                owner: lease.owner.clone(),
                fence: lease.fence,
                task_id: task.task_id.clone(),
                from_states: vec![TransferTaskState::Stale],
                state: TransferTaskState::Pending,
                message: format!("retrying after stale attempt: {}", task.progress.message),
                now_ms: now,
                bump_retry: true,
            })? {
                return Ok(());
            }
            record_metric(|metrics| {
                metrics.inc_stale_retry("scheduled");
                metrics.inc_requeue("stale");
                metrics.task_stale_requeued();
            });
        }

        // Transient task failures (throttling, timeouts, capacity, session
        // loss) are redelivered automatically within the retry budget instead
        // of failing the whole job; permanent failures fall through to
        // finish_failed_transfer below.
        let retryable_failed = self
            .recover_transient_failed_tasks(&job, &lease, now)
            .await?;

        // Cache tasks have no rows, so the store-side transient recovery below
        // cannot see them: retry their transport failures here, bounded by the
        // same retry budget.
        let cache_retried = self.task_cache.retry_failed(
            &job.job_id,
            job.run_id,
            self.conf.task_max_retries as u32,
            now,
            is_task_retryable_message,
        );
        if cache_retried > 0 {
            record_metric(|metrics| {
                metrics.inc_stale_retry("cache_transient_retry");
                metrics.task_retried(cache_retried);
            });
            self.transition(
                &job,
                &lease,
                &[TransferState::Running],
                TransferState::Dispatching,
                "retrying failed file-level tasks",
            )?;
            self.dispatch_pending_tasks(job, lease).await?;
            return Ok(());
        }

        if (self.store.has_failed_tasks(&job.job_id, job.run_id)?
            || self.task_cache.has_failed(&job.job_id, job.run_id))
            && !retryable_failed
        {
            self.finish_failed_transfer(&job, &lease).await?;
            return Ok(());
        }

        if !self
            .store
            .claim_pending_tasks(&job.job_id, job.run_id, 1)?
            .is_empty()
        {
            self.transition(
                &job,
                &lease,
                &[TransferState::Running],
                TransferState::Dispatching,
                "redispatch stale tasks",
            )?;
            self.dispatch_pending_tasks(job, lease).await?;
            return Ok(());
        }

        if retryable_failed || self.store.has_recoverable_tasks(&job.job_id, job.run_id)? {
            // acquire_runnable_transfer already refreshed this lease for the current tick.
            return Ok(());
        }
        // Finalize segmented targets BEFORE marking the job Completed: the file
        // only becomes readable once every segment's blocks are committed via
        // complete_file_by_id. If finalize fails, the data is not fully
        // converged, so the job must not report success (review Critical).
        if let Err(err) = self.finalize_segment_files(&job).await {
            warn!(
                "finalize segment files for transfer {} failed: {}",
                job.job_id, err
            );
            record_metric(|metrics| metrics.inc_terminal("failed", "finalize_segment_failed"));
            self.fail_transfer(
                &job,
                &lease,
                &[TransferState::Running],
                format!("finalize segment files failed: {}", err),
            )?;
            return Ok(());
        }
        let _ = self.transition(
            &job,
            &lease,
            &[TransferState::Running],
            TransferState::Completed,
            "all tasks completed",
        )?;
        record_metric(|metrics| metrics.inc_terminal("completed", "all_tasks_completed"));
        let completed_size = self
            .task_summary(&job)
            .map(|summary| summary.counts.completed_size)
            .unwrap_or(0);
        record_metric(|metrics| metrics.inc_completed_bytes("completed", completed_size));
        Ok(())
    }

    /// Finalize every CV target file written by segment tasks (design §14 T4):
    /// collect distinct targets whose tasks carry a `{"segment": ...}` plan,
    /// query the master block layout for each, and issue complete_file_by_id
    /// so the file becomes visible with its full block list.
    async fn finalize_segment_files(&self, job: &TransferJobRecord) -> FsResult<()> {
        let tasks = self.store.list_transfer_tasks(&job.job_id, job.run_id)?;
        // A target is only finalized when EVERY segment task targeting it has
        // completed. Block metadata cannot distinguish a pre-allocated block
        // (resize records the requested length before any payload lands)
        // from a written one, so file length heuristics would cements holes;
        // the per-task convergence fact is the reliable signal.
        let targets = converged_segment_targets(&tasks);
        if targets.is_empty() {
            return Ok(());
        }
        // The master keeps no locations for pre-allocated segment blocks until
        // a commit carries them, and workers never report writes back, so the
        // layout query alone would complete files with empty locations — shells
        // every read must fall back to UFS from. The completing tasks report
        // the workers they actually wrote to; commit those.
        let mut reported = reported_block_locations(&tasks);
        let target_count = targets.len();
        let mut unstamped = 0usize;
        let mut next_target = 0;
        while next_target < targets.len() {
            let batch_start = next_target;
            let mut paths = Vec::with_capacity(GET_BLOCK_LOCATIONS_BATCH_MAX_PATHS);
            while next_target < targets.len() && paths.len() < GET_BLOCK_LOCATIONS_BATCH_MAX_PATHS {
                paths.push(Path::from_str(&targets[next_target].0)?);
                next_target += 1;
            }
            let target_batch = &targets[batch_start..next_target];
            let layouts = if paths.len() == 1 {
                let start = std::time::Instant::now();
                let result = self.fs.get_block_locations(&paths[0]).await;
                record_master_rpc(
                    "segment_finalize",
                    "get_block_locations",
                    if result.is_ok() { "success" } else { "error" },
                    start,
                );
                vec![result]
            } else {
                let start = std::time::Instant::now();
                let result = self.fs.get_block_locations_batch(&paths).await;
                let metric_result = match &result {
                    Ok(Some(_)) => "success",
                    Ok(None) => "fallback",
                    Err(_) => "error",
                };
                record_master_rpc(
                    "segment_finalize",
                    "get_block_locations_batch",
                    metric_result,
                    start,
                );
                match result? {
                    Some(layouts) => layouts,
                    None => {
                        let mut layouts = Vec::with_capacity(paths.len());
                        for path in &paths {
                            let start = std::time::Instant::now();
                            let result = self.fs.get_block_locations(path).await;
                            record_master_rpc(
                                "segment_finalize",
                                "get_block_locations",
                                if result.is_ok() { "success" } else { "error" },
                                start,
                            );
                            layouts.push(result);
                        }
                        layouts
                    }
                }
            };
            for (((target, source_mtime), path), blocks) in
                target_batch.iter().zip(&paths).zip(layouts)
            {
                let blocks = blocks?;
                if blocks.status.is_complete() {
                    continue;
                }
                if blocks.block_locs.is_empty() {
                    warn!(
                        "skip finalizing segmented target file {} with empty layout",
                        target,
                    );
                    continue;
                }
                let located = reported.remove(target);
                let commit_blocks: Vec<_> = blocks
                    .block_locs
                    .iter()
                    .map(|block| commit_block_for(block, located.as_ref()))
                    .collect();
                let located_blocks = commit_blocks
                    .iter()
                    .filter(|block| !block.locations.is_empty())
                    .count();
                if located_blocks < commit_blocks.len() {
                    warn!(
                        "finalizing segmented target file {} with {} of {} blocks unlocated: no task reported workers for them (older worker? retry?), reads of those blocks fall back to UFS",
                        target,
                        commit_blocks.len() - located_blocks,
                        commit_blocks.len(),
                    );
                }
                // Stamp the plan-time UFS identity together with the completion.
                // No agreed mtime (missing, or the segments disagree because the
                // source moved mid-copy) leaves the target cache-invalid on
                // purpose: readers then fall back to UFS instead of trusting bytes
                // nobody can identify.
                let set_attr_opts = (*source_mtime)
                    .map(|ufs_mtime| SetAttrOptsBuilder::new().ufs_mtime(ufs_mtime).build());
                let stamped = set_attr_opts.is_some();
                if !stamped {
                    unstamped += 1;
                }
                let start = std::time::Instant::now();
                let result = self
                    .fs
                    .fs_client()
                    .complete_file_by_id(
                        path,
                        blocks.status.id,
                        blocks.status.len,
                        commit_blocks,
                        false,
                        set_attr_opts,
                    )
                    .await;
                record_master_rpc(
                    "segment_finalize",
                    "complete_file_by_id",
                    if result.is_ok() { "success" } else { "error" },
                    start,
                );
                result?;
                info!(
                    "finalized segmented target file {} blocks={} located_blocks={} len={} ufs_mtime_stamped={}",
                    target,
                    blocks.block_locs.len(),
                    located_blocks,
                    blocks.status.len,
                    stamped
                );
            }
        }
        if unstamped > 0 {
            warn!(
                "finalized {} of {} segmented target(s) for transfer {} without a UFS identity: no agreed plan-time source mtime, so reads keep falling back to UFS until a later load stamps it",
                unstamped, target_count, job.job_id
            );
        }
        Ok(())
    }

    async fn probe_stale_running_tasks(
        &self,
        job: &TransferJobRecord,
        now: i64,
    ) -> FsResult<StaleTaskMissingCandidates> {
        let tasks = self.store.list_stale_running_tasks(
            &job.job_id,
            job.run_id,
            now,
            self.conf.task_probe_concurrency(),
        )?;
        if tasks.is_empty() {
            return Ok(StaleTaskMissingCandidates::default());
        }
        let workers = match self.cache.live_workers() {
            Ok(workers) => Arc::new(workers),
            Err(err) => {
                let mut summary_dirty = false;
                for task in tasks {
                    summary_dirty |= self.defer_stale_task_retry(
                        &task,
                        now,
                        "cluster worker snapshot unavailable",
                        &err,
                    )?;
                }
                if summary_dirty {
                    self.flush_job_summary_best_effort(&job.job_id, job.run_id);
                }
                return Ok(StaleTaskMissingCandidates::default());
            }
        };
        let outcomes = stream::iter(tasks.into_iter().map(|task| {
            let scheduler = self.clone();
            let workers = workers.clone();
            async move { scheduler.probe_stale_task(task, workers, now).await }
        }))
        .buffer_unordered(self.conf.task_probe_concurrency())
        .collect::<Vec<_>>()
        .await;
        let mut summary_dirty = false;
        let mut missing_tasks = StaleTaskMissingCandidates::default();
        for outcome in outcomes {
            let updated = match outcome {
                StaleTaskProbeOutcome::Found(report) => self.store.update_task_report(report),
                StaleTaskProbeOutcome::Retry {
                    task,
                    reason,
                    error,
                } => self.defer_stale_task_retry(&task, now, reason, &error),
                StaleTaskProbeOutcome::Missing { task, reason } => {
                    match reason {
                        StaleTaskMissingReason::WorkerSessionGone => {
                            missing_tasks.worker_session_gone.push(task)
                        }
                        StaleTaskMissingReason::TaskAttemptMissing => {
                            missing_tasks.task_attempt_missing.push(task)
                        }
                    }
                    continue;
                }
            };
            match updated {
                Ok(updated) => summary_dirty |= updated,
                Err(err) => {
                    if summary_dirty {
                        self.flush_job_summary_best_effort(&job.job_id, job.run_id);
                    }
                    return Err(err);
                }
            }
        }
        if summary_dirty {
            self.flush_job_summary_best_effort(&job.job_id, job.run_id);
        }
        Ok(missing_tasks)
    }

    async fn probe_stale_task(
        &self,
        task: TransferTaskRecord,
        workers: Arc<Vec<WorkerInfo>>,
        now: i64,
    ) -> StaleTaskProbeOutcome {
        let Some(worker) = workers
            .iter()
            .find(|worker| {
                worker.worker_id() == task.worker_id
                    && worker.worker_session_id == task.worker_session_id
            })
            .cloned()
        else {
            return StaleTaskProbeOutcome::Missing {
                task,
                reason: StaleTaskMissingReason::WorkerSessionGone,
            };
        };
        let client = match self.factory.get_worker_client(&worker.address).await {
            Ok(client) => client,
            Err(err) => {
                return StaleTaskProbeOutcome::Retry {
                    task,
                    reason: "cannot connect to worker",
                    error: err.to_string(),
                };
            }
        };
        let response = match client
            .query_transfer_task(
                &task.job_id,
                task.run_id,
                &task.task_id,
                task.attempt_id,
                &task.worker_session_id,
            )
            .await
        {
            Ok(response) => response,
            Err(err) => {
                return StaleTaskProbeOutcome::Retry {
                    task,
                    reason: "worker did not answer task probe",
                    error: err.to_string(),
                };
            }
        };
        if !response.found {
            return StaleTaskProbeOutcome::Missing {
                task,
                reason: StaleTaskMissingReason::TaskAttemptMissing,
            };
        }
        StaleTaskProbeOutcome::Found(TransferTaskReport {
            job_id: task.job_id,
            run_id: task.run_id,
            task_id: task.task_id,
            attempt_id: task.attempt_id,
            worker_id: task.worker_id,
            worker_session_id: task.worker_session_id,
            state: transfer_task_state(response.state),
            progress: TransferProgress {
                loaded_size: response.progress.loaded_size,
                total_size: response.progress.total_size,
                update_time: response.progress.update_time,
                message: response.progress.message,
            },
            now_ms: now,
            stale_deadline_at: now.saturating_add(duration_ms(self.conf.task_stale_timeout)),
            accepted_at_ms: 0,
            first_byte_at_ms: 0,
            // The probe recovers tasks whose terminal report was lost; without
            // these workers the attempt's blocks would finalize as shells.
            block_locations: response
                .block_locations
                .iter()
                .map(|loc| TaskBlockLocation {
                    block_id: loc.block_id,
                    worker_ids: loc.worker_ids.clone(),
                })
                .collect(),
        })
    }

    fn defer_stale_task_retry(
        &self,
        task: &TransferTaskRecord,
        now: i64,
        reason: &str,
        err: &impl std::fmt::Display,
    ) -> FsResult<bool> {
        let mut progress = task.progress.clone();
        progress.message =
            "worker status is temporarily unavailable; preserving the current attempt before retry"
                .to_string();
        progress.update_time = now;
        let deferred = self.store.update_task_report(TransferTaskReport {
            job_id: task.job_id.clone(),
            run_id: task.run_id,
            task_id: task.task_id.clone(),
            attempt_id: task.attempt_id,
            worker_id: task.worker_id,
            worker_session_id: task.worker_session_id.clone(),
            state: TransferTaskState::Running,
            progress,
            now_ms: now,
            stale_deadline_at: now.saturating_add(duration_ms(self.conf.task_stale_timeout)),
            accepted_at_ms: 0,
            first_byte_at_ms: 0,
            block_locations: Vec::new(),
        })?;
        if deferred {
            debug!(
                "defer stale transfer task retry because {}: job={} run={} task={} attempt={} worker_id={} worker_session={} err={}",
                reason,
                task.job_id,
                task.run_id,
                task.task_id,
                task.attempt_id,
                task.worker_id,
                task.worker_session_id,
                err,
            );
        }
        Ok(deferred)
    }

    fn flush_job_summary_best_effort(&self, job_id: &str, run_id: u64) {
        if let Err(err) = self.store.flush_job_progress_summary(job_id, run_id) {
            warn!(
                "flush transfer job progress summary failed after scheduler report: job={} run={} err={}",
                job_id, run_id, err
            );
        }
    }

    async fn cancel_transfer(
        &self,
        job: &TransferJobRecord,
        lease: &TransferLease,
    ) -> FsResult<()> {
        self.stop_recoverable_tasks(job, lease, "transfer canceled")
            .await?;
        if self.store.has_recoverable_tasks(&job.job_id, job.run_id)? {
            // Ownership is process-scoped now: waiting for the tasks to stop
            // needs no write at all. The next tick re-checks convergence.
            let _ = lease;
            return Ok(());
        }

        let transitioned = self.transition(
            job,
            lease,
            &[TransferState::Canceling],
            TransferState::Canceled,
            "transfer canceled",
        )?;
        if transitioned {
            record_metric(|metrics| metrics.inc_terminal("canceled", "cancel_requested"));
        }
        Ok(())
    }

    async fn finish_failed_transfer(
        &self,
        job: &TransferJobRecord,
        lease: &TransferLease,
    ) -> FsResult<()> {
        self.stop_recoverable_tasks(job, lease, "transfer stopped after a task failed")
            .await?;
        if self.store.has_recoverable_tasks(&job.job_id, job.run_id)? {
            // Ownership is process-scoped now: waiting for the tasks to stop
            // needs no write at all. The next tick re-checks convergence.
            let _ = lease;
            return Ok(());
        }
        let summary = self.task_summary(job)?;
        // Complete converged segment files before landing the terminal
        // state, so a retry skips them instead of retransmitting everything.
        // Best-effort: finalize trouble must not block failure handling.
        if let Err(err) = self.finalize_segment_files(job).await {
            warn!(
                "finalize segment files for failed transfer {} failed: {}",
                job.job_id, err
            );
        }
        let state = if summary.counts.completed > 0 {
            TransferState::PartialSuccess
        } else {
            TransferState::Failed
        };
        let message = if state == TransferState::PartialSuccess {
            format!(
                "{}; {} file(s) completed and remain cached",
                summary.progress.message, summary.counts.completed
            )
        } else {
            summary.progress.message
        };
        if self.transition(job, lease, &[TransferState::Running], state, message)? {
            record_metric(|metrics| {
                metrics.inc_terminal(
                    if state == TransferState::PartialSuccess {
                        "partial_success"
                    } else {
                        "failed"
                    },
                    "task_failed",
                );
                metrics.inc_completed_bytes(
                    if state == TransferState::PartialSuccess {
                        "partial_success"
                    } else {
                        "failed"
                    },
                    summary.counts.completed_size,
                );
            });
        }
        Ok(())
    }

    async fn stop_recoverable_tasks(
        &self,
        job: &TransferJobRecord,
        lease: &TransferLease,
        message: &str,
    ) -> FsResult<()> {
        let now = now_ms();
        // One statement for the whole batch: cancelling a plan of 100k tasks used
        // to be 100k transactions (measured ~110 tasks/s, CV-86).
        let canceled = self.store.cancel_recoverable_tasks(
            &job.job_id,
            job.run_id,
            &lease.owner,
            lease.fence,
            message,
            now,
        )?;
        // Memory-only tasks have no rows, so they are cancelled in place; the ones
        // still reporting tell us which workers must be told to stop.
        let mut workers: HashSet<(u32, String)> = self
            .task_cache
            .cancel_recoverable(&job.job_id, job.run_id, message, now)
            .into_iter()
            .collect();
        workers.extend(self.store.running_task_workers(&job.job_id, job.run_id)?);
        if canceled > 0 {
            debug!(
                "canceled {} recoverable task rows of transfer {} in one batch",
                canceled, job.job_id
            );
        }

        let live_workers = match self.cache.live_workers() {
            Ok(workers) => workers,
            Err(err) => {
                warn!(
                    "cancel transfer {} without live worker snapshot: {}",
                    job.job_id, err
                );
                Vec::new()
            }
        };
        for worker in live_workers {
            if workers.contains(&(worker.worker_id(), worker.worker_session_id.clone())) {
                match self.factory.get_worker_client(&worker.address).await {
                    Ok(client) => {
                        if let Err(err) = client.cancel_job(&job.job_id, job.run_id).await {
                            warn!(
                                "cancel transfer {} on worker {} failed: {}",
                                job.job_id, worker, err
                            );
                        }
                    }
                    Err(err) => warn!("connect worker {} for cancel failed: {}", worker, err),
                }
            }
        }

        Ok(())
    }

    /// Effective estimated size used by scheduler-side gating. Unknown (0)
    /// sizes fall back to the configured default so they cannot bypass the
    /// byte budget (design §3.1).
    fn task_estimated_bytes(&self, task: &TransferTaskRecord) -> i64 {
        let size = if task.estimated_size > 0 {
            task.estimated_size
        } else {
            task.progress.total_size
        };
        if size > 0 {
            size
        } else {
            TransferConf::DEFAULT_TASK_ESTIMATED_BYTES
        }
    }

    /// Least-loaded worker choice under count/byte gates (design §4.2).
    /// Returns None when no worker has room: callers leave the task Pending.
    /// The winning worker's ledger is debited synchronously so concurrent
    /// decisions observe updated accounts instead of stampeding one worker;
    /// exact ties are broken by the polling cursor.
    fn choose_worker(&self, workers: &[WorkerInfo], estimated_bytes: i64) -> Option<WorkerInfo> {
        let now = now_ms();
        let mut candidates: Vec<WorkerInfo> = workers
            .iter()
            .filter(|worker| !self.worker_blacklist.is_blocked(worker.worker_id(), now))
            .cloned()
            .collect();
        if candidates.is_empty() && !workers.is_empty() {
            // Every worker is blocked. Drop the blacklist rather than stall the
            // job: a retry on a still-broken worker is no worse than waiting.
            self.worker_blacklist.clear();
            candidates.extend(workers.iter().cloned());
        }
        let workers = candidates.as_slice();
        // Both limits are loaded per decision, not per scheduler: a runtime
        // config push must change the very next dispatch. They are the transfer
        // role's own numbers; a worker's report is load, not a ceiling.
        let admitted = choose_worker_admitted(
            workers,
            estimated_bytes,
            self.runtime_config.worker_task_limit(),
            self.runtime_config.worker_inflight_budget_bytes(),
            |worker_id| self.inflight.get(worker_id),
        );
        if admitted.is_empty() {
            // No worker has room: report backpressure and dispatch nothing.
            // Never aim at a worker we already estimated as full; the next
            // heartbeat snapshot is what can change this answer.
            return None;
        }
        // Tie-break among equally loaded workers via the polling cursor.
        let min_bytes = admitted.iter().map(|(bytes, _)| *bytes).min().unwrap_or(0);
        let mut ties: Vec<usize> = admitted
            .into_iter()
            .filter(|(bytes, _)| *bytes == min_bytes)
            .map(|(_, index)| index)
            .collect();
        let pick = if ties.len() == 1 {
            ties.remove(0)
        } else {
            ties[self.worker_cursor.fetch_add(1, Ordering::Relaxed) % ties.len()]
        };
        let worker = workers[pick].clone();
        self.inflight
            .account(worker.worker_id(), 1, estimated_bytes);
        Some(worker)
    }

    fn transition(
        &self,
        job: &TransferJobRecord,
        lease: &TransferLease,
        from_states: &[TransferState],
        to_state: TransferState,
        message: impl Into<String>,
    ) -> FsResult<bool> {
        let message = message.into();
        if to_state.is_terminal() {
            self.snapshot_terminal(job, &message)?;
            // Terminal snapshot carries the file-level counts; the live
            // in-memory bucket is no longer needed and must not leak.
            self.task_cache.remove(&job.job_id, job.run_id);
        }
        let updated = self.store.update_transfer_state(TransferStateUpdate {
            job_id: lease.job_id.clone(),
            run_id: lease.run_id,
            owner: lease.owner.clone(),
            fence: lease.fence,
            from_states: from_states.to_vec(),
            to_state,
            message: message.clone(),
            now_ms: now_ms(),
        })?;
        if updated {
            info!(
                "transfer state transition job_id={} run_id={} kind={:?} tenant={} owner={} fence={} from={:?} to={:?} message={}",
                lease.job_id,
                lease.run_id,
                job.kind,
                job.tenant,
                lease.owner,
                lease.fence,
                from_states,
                to_state,
                message
            );
        }
        Ok(updated)
    }

    fn fail_transfer(
        &self,
        job: &TransferJobRecord,
        lease: &TransferLease,
        from_states: &[TransferState],
        message: impl Into<String>,
    ) -> FsResult<()> {
        let message = message.into();
        error!("transfer {} failed: {}", job.job_id, message);
        let _ = self.transition(job, lease, from_states, TransferState::Failed, message)?;
        record_metric(|metrics| metrics.inc_terminal("failed", "error"));
        Ok(())
    }

    fn snapshot_terminal(&self, job: &TransferJobRecord, message: &str) -> FsResult<()> {
        let summary = self.task_summary(job)?;
        let progress = terminal_snapshot_progress(summary.progress, message);
        self.store.update_terminal_snapshot(
            &job.job_id,
            job.run_id,
            progress,
            summary.counts,
            now_ms(),
        )
    }
}

struct DispatchTaskRequest<S> {
    store: Arc<S>,
    factory: Arc<UfsFactory>,
    cache: ClusterMetadataCache,
    job_info: LoadJobInfo,
    owner: String,
    fence: u64,
    task: TransferTaskRecord,
    worker: WorkerInfo,
    report_endpoints: Vec<String>,
    task_stale_timeout_ms: i64,
    task_cache: Arc<TaskCache>,
    worker_blacklist: Arc<WorkerBlacklist>,
}

/// Returns a task to Pending after the worker refused it before accepting.
///
/// File-level tasks live only in the cache and have no `transfer_tasks` row, so
/// a store write there answers "not found" and the task would stay Running until
/// a stale deadline expires - i.e. a rejection that costs a whole
/// `task_stale_timeout` of latency for no reason. Release those in memory
/// instead, which is what the cache field is for.
#[allow(clippy::too_many_arguments)]
fn requeue_rejected_task<S>(
    store: &S,
    task_cache: &TaskCache,
    task: &TransferTaskRecord,
    owner: &str,
    fence: u64,
    reason: &str,
    bump_retry: bool,
    now_ms: i64,
) -> FsResult<bool>
where
    S: TransferStore + ?Sized,
{
    if task_cache.owns_task(&task.job_id, task.run_id, &task.task_id) {
        // Memory-only: no row exists, so this is the whole bookkeeping.
        return Ok(task_cache.release_attempt(&task.job_id, task.run_id, &task.task_id, now_ms));
    }
    store.update_task_state(TransferTaskStateUpdate {
        job_id: task.job_id.clone(),
        run_id: task.run_id,
        owner: owner.to_string(),
        fence,
        task_id: task.task_id.clone(),
        from_states: vec![TransferTaskState::Running],
        state: TransferTaskState::Pending,
        message: format!("worker rejected task before accept: {}", reason),
        now_ms,
        bump_retry,
    })
}

async fn dispatch_one<S>(request: DispatchTaskRequest<S>) -> FsResult<bool>
where
    S: TransferStore,
{
    let DispatchTaskRequest {
        store,
        factory,
        cache,
        job_info,
        owner,
        fence,
        task,
        worker,
        report_endpoints,
        task_stale_timeout_ms,
        task_cache,
        worker_blacklist,
    } = request;
    let now = now_ms();
    let attempt_id = task.attempt_id.saturating_add(1);
    let resume_loaded_size = match classify_transfer_plan(&task.source_read_plan_json) {
        TransferPlanKind::SegmentGroup => {
            normalize_group_resume(&task.source_read_plan_json, task.progress.loaded_size)
        }
        _ => 0,
    };
    if resume_loaded_size > 0 {
        record_metric(|metrics| metrics.inc_group_resume(resume_loaded_size));
    }
    let client = match factory.get_worker_client(&worker.address).await {
        Ok(client) => client,
        Err(err) => {
            // The node is unreachable: keep it out of the next few rounds so the
            // retry lands on a healthy worker instead of burning the budget.
            worker_blacklist.block(worker.worker_id(), now_ms());
            warn!(
                "connect worker for transfer task failed before submit; retry in next scheduler tick: job={} run={} task={} attempt={} worker_id={} worker_session={} source={} target={} err={}",
                task.job_id,
                task.run_id,
                task.task_id,
                attempt_id,
                worker.worker_id(),
                worker.worker_session_id,
                task.source_path,
                task.target_path,
                err
            );
            return Ok(false);
        }
    };
    let attempt = TaskAttemptStart {
        job_id: task.job_id.clone(),
        run_id: task.run_id,
        owner: owner.clone(),
        fence,
        task_id: task.task_id.clone(),
        attempt_id,
        worker_id: worker.worker_id(),
        worker_session_id: worker.worker_session_id.clone(),
        report_target_json: serde_json::to_string(&report_endpoints)
            .map_err(|_| FsError::common("Unable to prepare transfer report endpoints"))?,
        now_ms: now,
        stale_deadline_at: now.saturating_add(task_stale_timeout_ms),
        resume_loaded_size,
    };
    // File-level tasks are memory-only, so their attempt starts in the cache.
    let started = if task_cache.owns_task(&task.job_id, task.run_id, &task.task_id) {
        task_cache.start_attempt(&attempt)
    } else {
        store.start_task_attempt(attempt)?
    };
    if !started {
        return Ok(false);
    }

    let load_task = LoadTaskInfo {
        job: job_info,
        task_id: task.task_id.clone(),
        worker: worker.address.clone(),
        source_path: task.source_path.clone(),
        target_path: task.target_path.clone(),
        create_time: now,
        source_read_plan_json: task.source_read_plan_json.clone(),
        estimated_size: task.estimated_size.max(task.progress.total_size),
        transfer_report: Some(TransferTaskReportInfo {
            run_id: task.run_id,
            attempt_id,
            worker_id: worker.worker_id(),
            worker_session_id: worker.worker_session_id.clone(),
            report_target: report_endpoints.first().cloned().unwrap_or_default(),
            report_endpoints,
            resume_loaded_size,
            // Same instant `start_task_attempt` persisted above; the worker
            // echoes it so the report path never re-reads the task row.
            attempt_started_at_ms: now,
        }),
    };

    match client.submit_load_task_response(load_task).await {
        Ok(response) if response.accepted.unwrap_or(true) => {
            // The ledger already counted this dispatch and the next heartbeat
            // snapshot replaces the estimate wholesale, so the answer's load
            // numbers are deliberately not read here: capacity comes from the
            // heartbeat, never from a submit answer.
            record_metric(|metrics| metrics.task_assigned());
            Ok(true)
        }
        Ok(response) => {
            let reason = response.reject_reason.unwrap_or_default();
            // A capacity refusal (the common case) changes nothing on the
            // control plane: no skip, no cooldown, no refresh, and its load
            // numbers are not read. The estimate stays whatever the last
            // periodic snapshot said - deliberately allowed to be stale, since
            // the worker's own gate is the backstop and the next scheduled
            // snapshot corrects it.
            //
            // A session-class rejection is different: it says the cached peer
            // session is invalid, which no cadence can fix, so the worker is
            // parked in the short blacklist until its next heartbeat.
            if is_session_rejection(&reason) {
                worker_blacklist.block(worker.worker_id(), now_ms());
            }
            warn!(
                "worker rejected transfer task before accepting it: job={} run={} task={} attempt={} worker_id={} worker_session={} source={} target={} reason={}",
                task.job_id,
                task.run_id,
                task.task_id,
                attempt_id,
                worker.worker_id(),
                worker.worker_session_id,
                task.source_path,
                task.target_path,
                reason
            );
            // Capacity rejections (worker busy, temporary) must not count
            // against the retry ceiling: the worker is healthy and will accept
            // the task once load drops. Only session/capability rejections
            // indicate a real problem worth bounding.
            if !requeue_rejected_task(
                store.as_ref(),
                &task_cache,
                &task,
                &owner,
                fence,
                &reason,
                is_session_rejection(&reason),
                now_ms(),
            )? {
                return Ok(false);
            }
            // Capacity rejections must NOT remove the worker session: the worker
            // is healthy and other tasks on it would be misdiagnosed as
            // WorkerSessionGone by later probes (design §5.2). Only session/
            // capability-class rejections evict the cached session.
            let rejection_class = if is_session_rejection(&reason) {
                "session"
            } else {
                "capacity"
            };
            record_metric(|metrics| {
                metrics.inc_reject(rejection_class);
                metrics.inc_requeue("reject");
            });
            if is_session_rejection(&reason) {
                if let Err(err) = cache.refresh().await {
                    warn!(
                        "refresh cluster metadata after worker rejection failed: {}",
                        err
                    );
                }
                cache.remove_worker_session(worker.worker_id(), &worker.worker_session_id);
            }
            Ok(false)
        }
        Err(err) => {
            warn!(
                "submit transfer task returned an error after attempt start; keep running and let query/stale recovery decide final state: job={} run={} task={} attempt={} worker_id={} worker_session={} source={} target={} err={}",
                task.job_id,
                task.run_id,
                task.task_id,
                attempt_id,
                worker.worker_id(),
                worker.worker_session_id,
                task.source_path,
                task.target_path,
                err
            );
            Ok(true)
        }
    }
}

fn now_ms() -> i64 {
    LocalTime::mills() as i64
}

fn duration_ms(duration: Duration) -> i64 {
    i64::try_from(duration.as_millis()).unwrap_or(i64::MAX)
}

fn scheduler_idle_interval(lease_timeout: Duration) -> Duration {
    SCHEDULER_IDLE_INTERVAL
        .min(lease_timeout / 2)
        .max(SCHEDULER_MIN_IDLE_INTERVAL)
}

/// Ledger key for [`MAX_PLACEMENT_HAND_BACKS`]: a rerun has a new request id but
/// keeps the job id, so the run id has to be part of the key.
fn placement_hand_back_key(job: &TransferJobRecord) -> String {
    format!("{}:{}", job.job_id, job.run_id)
}

/// Message for a dispatch round: capacity backpressure must be visible in the
/// job status Message column, so blocked reasons win over a bare count.
/// Message for a dispatch round: capacity backpressure must be visible in the
/// job status Message column, so blocked reasons win over a bare count.
pub(crate) fn dispatch_result_message(dispatched: usize, blocked: usize) -> String {
    if blocked > 0 {
        format!(
            "waiting for worker capacity: {} task(s) blocked this round (worker in-flight byte budget or concurrency limit reached)",
            blocked
        )
    } else {
        format!("dispatched {} tasks", dispatched)
    }
}

/// Candidates for [`TransferScheduler::choose_worker`]: workers under the
/// count/byte ceilings (oversize tasks only on idle workers, design §4.2).
///
/// The two ceilings are the transfer role's runtime configuration, applied to
/// every worker alike. A worker's heartbeat carries its own numbers, but those
/// are a load reading for operators, not an admission ceiling: one knob has to
/// decide dispatch, or tuning a fleet means touching every worker.
///
/// Pure - the ledger lookup is injected so the gating rules can be unit-tested
/// without a full scheduler.
fn choose_worker_admitted(
    workers: &[WorkerInfo],
    estimated_bytes: i64,
    count_limit: usize,
    byte_limit: i64,
    inflight: impl Fn(u32) -> (u64, i64),
) -> Vec<(i64, usize)> {
    let mut admitted: Vec<(i64, usize)> = Vec::with_capacity(workers.len());
    for (index, worker) in workers.iter().enumerate() {
        let (count, bytes) = inflight(worker.worker_id());
        if count_limit > 0 && count >= count_limit as u64 {
            continue;
        }
        let oversize = byte_limit > 0 && estimated_bytes > byte_limit;
        // Oversize tasks only run on an idle worker to avoid starvation.
        let fits = byte_limit <= 0 || bytes.saturating_add(estimated_bytes) <= byte_limit;
        let take = if oversize { bytes == 0 } else { fits };
        if take {
            admitted.push((bytes, index));
        }
    }
    admitted
}

/// Session/capability-class worker rejections invalidate the cached session;
/// capacity-class rejections do not (design §5.2/§5.3). Classification is an
/// explicit prefix match on the worker's reject_reason. Unknown reasons are
/// treated as session-class (conservative: evict) because the only known
/// capacity reason is `worker_at_capacity`; future capacity reasons must add
/// their prefix here.
/// A reject reason that indicates the worker's session is genuinely stale
/// (worker restarted or session rotated) and warrants evicting the cached
/// worker session. Capacity rejections (worker busy, temporary) and stale
/// attempt rejections (an outdated run/attempt of an already-retried task)
/// are NOT session failures: evicting on them would misdiagnose a healthy
/// worker and force its other tasks to be re-probed (review Required).
/// Whether a terminally failed task is worth an automatic redelivery instead
/// of failing the whole job. Allowlist is intentionally narrow: anything
/// unrecognized stays permanent (fail fast, current behavior). Throttling,
/// timeouts, capacity pressure, session loss, report races and an
/// unready segment layout are transient — the retry is idempotent
/// (positional segment writes, batch commit markers, temp+rename singles)
/// and bounded by task_max_retries with stale-style backoff.
/// Message-only form of [`is_task_retryable`], for callers that hold a task
/// record outside the store (file-level tasks live only in the cache).
fn is_task_retryable_message(msg: &str) -> bool {
    if msg.contains(curvine_error::RetryClass::RetryElsewhere.token())
        || msg.contains(curvine_error::RetryClass::RetryAfter.token())
        || msg.contains(curvine_error::RetryClass::RetryMaster.token())
    {
        return true;
    }
    msg.contains("RateLimited")
        || msg.contains("TooManyRequests")
        || msg.contains("too many requests")
        || msg.contains("Timed out")
        || msg.contains("timed out")
        || msg.contains("exceed timeout")
        || msg.contains(crate::worker::task::WORKER_AT_CAPACITY_REJECT_PREFIX)
        || msg.contains("Connection reset by peer")
        || msg.contains("Broken pipe")
        || msg.contains("Connection refused")
        || msg.contains("channel closed")
        || msg.contains("os error 104")
        || msg.contains("os error 32")
        || msg.contains("os error 111")
        || msg.contains("not found in target layout")
        || msg.contains("Transfer task report timed out")
}

fn is_task_retryable(task: &TransferTaskRecord) -> bool {
    // Storage/health failures carry the retry class the worker computed; the
    // token keeps that decision intact across the text-only report path.
    let msg = task.progress.message.as_str();
    if msg.contains(curvine_error::RetryClass::RetryElsewhere.token())
        || msg.contains(curvine_error::RetryClass::RetryAfter.token())
        || msg.contains(curvine_error::RetryClass::RetryMaster.token())
    {
        return true;
    }
    msg.contains("RateLimited")
        || msg.contains("TooManyRequests")
        || msg.contains("too many requests")
        || msg.contains("Timed out")
        || msg.contains("timed out")
        // Worker task timeout copy: "Task {} exceed timeout {} ms".
        || msg.contains("exceed timeout")
        || msg.contains(crate::worker::task::WORKER_AT_CAPACITY_REJECT_PREFIX)
        // Transport-level failures mean a peer (worker or master data path)
        // went away mid-copy. Replaying a file-level task is idempotent and
        // segment writes are positional, so these are transient: retrying on a
        // healthy worker is strictly better than failing the whole job because
        // one worker died.
        || msg.contains("Connection reset by peer")
        || msg.contains("Broken pipe")
        || msg.contains("Connection refused")
        || msg.contains("channel closed")
        || msg.contains("os error 104")
        || msg.contains("os error 32")
        || msg.contains("os error 111")
        || msg.contains("not found in target layout")
        || msg.contains("Transfer task report timed out")
        || msg.contains("WorkerSessionGone")
        || msg.contains("stale worker session")
}

fn is_session_rejection(reason: &str) -> bool {
    if reason.starts_with(crate::worker::task::WORKER_AT_CAPACITY_REJECT_PREFIX) {
        return false;
    }
    // "Reject stale transfer task ..." is an outdated attempt, not a session
    // failure; "stale worker session" is a real session rotation.
    if reason.starts_with("Reject stale transfer task") {
        return false;
    }
    if reason.contains("stale worker session") {
        return true;
    }
    // Conservative default: an unrecognized reason may indicate a real
    // problem, so treat it as session-class (matches prior behavior).
    true
}

fn transfer_task_state(state: i32) -> TransferTaskState {
    match state {
        1 => TransferTaskState::Pending,
        2 => TransferTaskState::Running,
        3 => TransferTaskState::Completed,
        4 => TransferTaskState::Failed,
        5 => TransferTaskState::Canceled,
        6 => TransferTaskState::Stale,
        _ => TransferTaskState::Failed,
    }
}

/// Exponential backoff (ms) before redelivering a stale task, based on its
/// retry count. Base 10s, doubling per retry, capped at 120s. This converts a
/// per-tick redispatch storm into a bounded retry cadence.
pub(super) fn stale_retry_backoff_ms(retry_count: u32) -> i64 {
    const BASE_MS: i64 = 10_000;
    const CAP_MS: i64 = 120_000;
    let shift = retry_count.min(4); // 2^4 = 16x -> 160s, capped to 120s
    let backoff = BASE_MS.saturating_mul(1i64 << shift);
    backoff.min(CAP_MS)
}

pub(super) fn stale_retry_exhausted_message(task: &TransferTaskRecord) -> String {
    let reason = match task.progress.message.as_str() {
        "" | "task stale timeout" | "retry stale task attempt" => {            "no worker diagnosis was recorded for this stale attempt; inspect the assigned Worker logs"
        }
        message => message,
    };
    format!(
        "transfer task recovery retries exhausted after {} stale attempt(s): {reason}",
        task.retry_count
    )
}

fn record_metric(f: impl FnOnce(&TransferMetrics)) {
    if let Ok(metrics) = TransferMetrics::get() {
        f(metrics);
    }
}

fn transfer_kind_label(kind: curvine_model::TransferKind) -> &'static str {
    match kind {
        curvine_model::TransferKind::Load => "load",
        curvine_model::TransferKind::Export => "export",
    }
}

#[cfg(test)]
mod tests {
    use super::is_planning_retryable;
    use super::is_task_retryable;
    use super::mount_snapshot_failure_action;
    use super::planning_retry_attempt;
    use super::planning_retry_delay_ms;
    use super::scheduler_idle_interval;
    use super::InflightLedger;
    use super::MountSnapshotFailureAction;
    use super::MOUNT_SNAPSHOT_MAX_RETRIES;
    use super::PLANNING_LIST_MAX_RETRIES;
    use curvine_model::WorkerInfo;
    use std::time::Duration;

    #[test]
    fn ledger_increment_only_grows_within_ttl_window() {
        let ledger = InflightLedger::new(Duration::from_secs(15));
        ledger.account(1, 1, 1024);
        ledger.account(1, 1, 1024);
        assert_eq!(ledger.get(1), (2, 2048));
        assert_eq!(ledger.get(2), (0, 0));
    }

    #[test]
    fn delayed_target_replacement_preserves_plan_type_boundaries() {
        use super::planned_target_replacements;
        use curvine_model::{
            BatchManifest, BatchManifestEntry, TransferProgress, TransferTaskRecord,
            TransferTaskState,
        };

        let task = |task_id: &str, plan: String, target_path: &str, replace_target: bool| {
            TransferTaskRecord {
                job_id: "job".to_string(),
                run_id: 1,
                task_id: task_id.to_string(),
                attempt_id: 0,
                source_path: "file:///source".to_string(),
                target_path: target_path.to_string(),
                worker_id: 0,
                worker_session_id: String::new(),
                source_read_plan_json: plan,
                report_target_json: String::new(),
                state: TransferTaskState::Pending,
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
                replace_target,
            }
        };
        let batch = serde_json::to_string(&BatchManifest {
            batch: vec![
                BatchManifestEntry {
                    rel_path: "keep".to_string(),
                    size: 1,
                    replace_target: false,
                },
                BatchManifestEntry {
                    rel_path: "nested/replace".to_string(),
                    size: 1,
                    replace_target: true,
                },
            ],
        })
        .unwrap();

        let (segments, replacements) = planned_target_replacements(&[
            task("plain", "{}".to_string(), "cv:///plain", true),
            task(
                "segment",
                "{\"segment\":{\"offset\":0}}".to_string(),
                "cv:///segment",
                false,
            ),
            task("batch", batch, "cv:///batch", false),
        ])
        .unwrap();

        assert_eq!(
            segments.into_iter().collect::<Vec<_>>(),
            vec!["cv:///segment"]
        );
        assert!(replacements.contains("cv:///plain"));
        assert!(replacements.contains("cv:///batch/nested/replace"));
        assert!(!replacements.contains("cv:///batch/keep"));
    }

    #[test]
    fn choose_worker_enforces_byte_budget_and_oversize_idle_only() {
        use super::choose_worker_admitted;
        use curvine_model::WorkerAddress;
        let w1 = WorkerInfo::new(
            WorkerAddress {
                worker_id: 1,
                ..Default::default()
            },
            1,
        );
        let w2 = WorkerInfo::new(
            WorkerAddress {
                worker_id: 2,
                ..Default::default()
            },
            1,
        );
        let workers = [w1, w2];
        // Legacy workers use the supplied 100-byte compatibility default. With
        // 80 already used on w1, only w2 fits a 30-byte task.
        let admitted =
            choose_worker_admitted(
                &workers,
                30,
                0,
                100,
                |id| {
                    if id == 1 {
                        (1, 80)
                    } else {
                        (0, 0)
                    }
                },
            );
        assert_eq!(admitted.len(), 1);
        assert_eq!(admitted[0].1, 1); // w2 (index 1) is the only fit
                                      // Oversize task (150 > 100) only admitted when the worker is idle.
        let admitted = choose_worker_admitted(&workers, 150, 0, 100, |_| (0, 0));
        assert_eq!(admitted.len(), 2); // both idle
        let admitted = choose_worker_admitted(&workers, 150, 0, 100, |_| (0, 5));
        assert_eq!(admitted.len(), 0); // neither idle
                                       // Zero byte_limit disables byte gating entirely.
        let admitted = choose_worker_admitted(&workers, 150, 0, 0, |_| (0, 1000));
        assert_eq!(admitted.len(), 2);

        // One byte ceiling for the whole fleet: the worker's own reported
        // budget must not widen it for a single worker.
        let mut w1 = WorkerInfo::new(
            WorkerAddress {
                worker_id: 1,
                ..Default::default()
            },
            1,
        );
        let mut w2 = WorkerInfo::new(
            WorkerAddress {
                worker_id: 2,
                ..Default::default()
            },
            1,
        );
        w1.transfer_inflight_budget_bytes = 100;
        w2.transfer_inflight_budget_bytes = 200;
        let admitted = choose_worker_admitted(&[w1, w2], 150, 0, 100, |id| {
            if id == 1 {
                (1, 1)
            } else {
                (0, 0)
            }
        });
        assert_eq!(
            admitted,
            vec![(0, 1)],
            "a reported 200 budget must not admit a 150-byte task under a 100-byte ceiling"
        );
    }

    #[test]
    fn choose_worker_enforces_count_gate() {
        use super::choose_worker_admitted;
        let w1 = WorkerInfo::new(Default::default(), 1);
        // Default count_limit=2, w1 already at 2: excluded.
        let admitted = choose_worker_admitted(std::slice::from_ref(&w1), 10, 2, 0, |_| (2, 0));
        assert!(admitted.is_empty());
        // count_limit=0 disables the count gate.
        let admitted = choose_worker_admitted(&[w1], 10, 0, 0, |_| (2, 0));
        assert_eq!(admitted.len(), 1);
        // A worker's own report is advisory: it neither raises nor lowers the
        // ceiling the transfer decides with.
        let mut w2 = WorkerInfo::new(Default::default(), 1);
        w2.worker_max_concurrent_tasks = 5;
        let admitted = choose_worker_admitted(std::slice::from_ref(&w2), 10, 2, 0, |_| (2, 0));
        assert!(
            admitted.is_empty(),
            "a reported 5 must not raise a limit of 2"
        );
        w2.worker_max_concurrent_tasks = 1;
        let admitted = choose_worker_admitted(std::slice::from_ref(&w2), 10, 2, 0, |_| (1, 0));
        assert_eq!(
            admitted.len(),
            1,
            "a reported 1 must not lower a limit of 2"
        );
    }

    /// The snapshot is the only writer of the base: local dispatches add on
    /// top, and the next snapshot replaces both.
    #[test]
    fn snapshot_replaces_base_and_clears_local_dispatches() {
        use curvine_model::WorkerAddress;
        let ledger = InflightLedger::new(Duration::from_secs(15));
        let worker = |id: u32, load: (u64, i64)| {
            let mut info = WorkerInfo::new(
                WorkerAddress {
                    worker_id: id,
                    ..Default::default()
                },
                1,
            );
            info.transfer_inflight_tasks = load.0;
            info.transfer_inflight_bytes = load.1;
            info
        };

        ledger.apply_snapshot(1, &[worker(1, (7, 7000))]);
        assert_eq!(ledger.get(1), (7, 7000));

        // Local dispatches add on top of the base until the next snapshot.
        ledger.account(1, 1, 1000);
        assert_eq!(ledger.get(1), (8, 8000));

        // A stale snapshot must not move the base backwards.
        ledger.apply_snapshot(1, &[worker(1, (0, 0))]);
        assert_eq!(ledger.get(1), (8, 8000));

        // The next snapshot wins outright and clears the local delta - this is
        // what makes a saturated ledger recover without any submit.
        ledger.apply_snapshot(2, &[worker(1, (2, 2000))]);
        assert_eq!(ledger.get(1), (2, 2000));

        // Dispatch decisions are a pure function of the snapshot: nothing a
        // worker answers can mark it as skipped, only the next snapshot moves
        // the base.
        assert_eq!(ledger.get(1), (2, 2000));
    }

    #[test]
    fn retryable_mount_snapshot_failures_use_bounded_backoff() {
        assert_eq!(
            mount_snapshot_failure_action(0),
            MountSnapshotFailureAction::Requeue {
                retry_count: 1,
                delay_ms: 1_000,
            }
        );
        assert_eq!(
            mount_snapshot_failure_action(1),
            MountSnapshotFailureAction::Requeue {
                retry_count: 2,
                delay_ms: 2_000,
            }
        );
        assert_eq!(
            mount_snapshot_failure_action(MOUNT_SNAPSHOT_MAX_RETRIES),
            MountSnapshotFailureAction::Fail
        );
    }

    #[test]
    fn planning_throttle_uses_bounded_backoff_with_jitter() {
        assert_eq!(planning_retry_attempt(0), Some(1));
        assert_eq!(planning_retry_attempt(PLANNING_LIST_MAX_RETRIES), None);
        // Exponential growth with bounded jitter.
        let d0 = planning_retry_delay_ms(0, "job-a", 1_000_000);
        let d1 = planning_retry_delay_ms(1, "job-a", 1_000_000);
        let d2 = planning_retry_delay_ms(2, "job-a", 1_000_000);
        assert!((1_000..2_000).contains(&d0));
        assert!((2_000..3_000).contains(&d1));
        assert!((4_000..5_000).contains(&d2));
        // Same attempt, different timestamps spread out.
        assert_ne!(
            planning_retry_delay_ms(1, "job-a", 1_000_000),
            planning_retry_delay_ms(1, "job-a", 1_000_001)
        );
    }

    #[test]
    fn task_retryable_covers_transient_signals_only() {
        use curvine_model::{TransferProgress, TransferTaskRecord, TransferTaskState};
        fn failed_task(message: &str) -> TransferTaskRecord {
            TransferTaskRecord {
                job_id: "job".to_string(),
                run_id: 1,
                task_id: "task".to_string(),
                attempt_id: 1,
                source_path: "/s".to_string(),
                target_path: "/t".to_string(),
                worker_id: 1,
                worker_session_id: "session".to_string(),
                source_read_plan_json: "{}".to_string(),
                report_target_json: "{}".to_string(),
                state: TransferTaskState::Failed,
                progress: TransferProgress {
                    message: message.to_string(),
                    ..Default::default()
                },
                estimated_size: 0,
                retry_count: 0,
                attempt_started_at: 0,
                accepted_at_ms: 0,
                first_byte_at_ms: 0,
                block_locations: Vec::new(),
                last_report_at: 0,
                stale_deadline_at: 0,
                updated_at: 0,
                replace_target: false,
            }
        }
        // Transient: throttling, timeouts, capacity, session loss, report
        // races, unready segment layout.
        for message in [
            "Failed to list directory x: RateLimited (persistent) at list",
            "S3Error { code: \"TooManyRequests\" }",
            "Timed out while accessing s3://bucket/key; retry",
            "worker_at_capacity: busy, retry later",
            "segment task t block 1 not found in target layout (blocks=0)",
            "Transfer task report timed out after 5000 ms: job=..",
            "WorkerSessionGone: worker restarted",
            "stale worker session rotated",
            // Storage health failures: the worker said "try elsewhere".
            "Load (UFS -> Curvine) failed during execution: source=oss://b/k; target=/t; cause=retry:elsewhere storage_unavailable no usable Disk data dir",
        ] {
            assert!(is_task_retryable(&failed_task(message)), "{}", message);
        }
        // Permanent: missing source, bad args, oversized data, short reads.
        for message in [
            "Load (UFS -> Curvine) failed during execution: source=oss://b/k; target=/t; cause=object not found",
            "Transfer target /t already exists and overwrite=false",
            "segment task t short read: 100 bytes missing",
            "Not enough Curvine worker disk space",
            "transfer task failed",
        ] {
            assert!(!is_task_retryable(&failed_task(message)), "{}", message);
        }
    }

    #[test]
    fn planning_retryable_covers_throttle_signals_only() {
        use curvine_error::FsError;
        assert!(is_planning_retryable(&FsError::common(
            "Failed to list directory x: RateLimited (persistent) at list"
        )));
        assert!(is_planning_retryable(&FsError::common(
            "S3Error { code: \"TooManyRequests\" }"
        )));
        assert!(!is_planning_retryable(&FsError::file_not_found("/x")));
        assert!(!is_planning_retryable(&FsError::common(
            "Transfer target /x already exists and overwrite=false"
        )));
    }

    #[test]
    fn idle_backoff_renews_short_leases_before_they_expire() {
        assert_eq!(
            scheduler_idle_interval(Duration::from_secs(120)),
            Duration::from_secs(1)
        );
        assert_eq!(
            scheduler_idle_interval(Duration::from_millis(100)),
            Duration::from_millis(50)
        );
    }

    #[path = "dispatch_recovery_test.rs"]
    mod dispatch_recovery_test;
    #[path = "worker_blacklist_test.rs"]
    mod worker_blacklist_test;
}
