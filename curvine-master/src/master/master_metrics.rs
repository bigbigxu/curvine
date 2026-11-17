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

#![allow(unused)]

use crate::master::fs::MasterFilesystem;
use crate::master::meta::inode::ttl::TtlBucket;
use crate::master::{JobStore, Master, MasterPhase};
use curvine_core_error::CommonResult;
use curvine_metrics::{
    Counter, CounterVec, Gauge, GaugeVec, Histogram, HistogramVec, Metrics, Metrics as m,
};
use curvine_model::{JobTaskProgress, JobTaskState, JobTaskType, MetricValue, WorkerInfo};
use curvine_raft::raft::storage::{LogStorage, RocksLogStorage};
use curvine_runtime::common::LocalTime;
use curvine_runtime::sync::FastDashMap;
use curvine_sys::version::ReleaseVersion;
use dashmap::DashMap;
use log::{debug, info, warn};
use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::time::{Duration, Instant};

/// Release version label of a client that sent no component info yet.
const CLIENT_VERSION_NONE: &str = "none";
/// Release version label of a client whose reported version cannot be parsed.
/// Parsing keeps the label set bounded by real builds, not by client input.
const CLIENT_VERSION_UNKNOWN: &str = "unknown";

/// Job kinds and the states the master job manager moves a job through.
/// `load_jobs` exports every combination, with 0 when no job is in it, so a
/// dashboard series does not disappear while the store is empty.
const LOAD_JOB_KINDS: [JobTaskType; 2] = [JobTaskType::Load, JobTaskType::Export];
const LOAD_JOB_STATES: [JobTaskState; 6] = [
    JobTaskState::Pending,
    JobTaskState::Loading,
    JobTaskState::Completed,
    JobTaskState::Failed,
    JobTaskState::Canceled,
    JobTaskState::PartialSuccess,
];

fn job_kind_label(kind: JobTaskType) -> &'static str {
    match kind {
        JobTaskType::Load => "load",
        JobTaskType::Export => "export",
    }
}

fn job_state_label(state: JobTaskState) -> &'static str {
    match state {
        JobTaskState::UNKNOWN => "unknown",
        JobTaskState::Pending => "pending",
        JobTaskState::Loading => "loading",
        JobTaskState::Completed => "completed",
        JobTaskState::Failed => "failed",
        JobTaskState::Canceled => "canceled",
        JobTaskState::PartialSuccess => "partial_success",
    }
}

struct ClientCompat {
    release_version: String,
    verdict: &'static str,
    last_seen: Instant,
}

/// Last compatibility verdict of each client address, aggregated into
/// `(release_version, verdict)` counts when metrics are scraped.
///
/// The client address is state here, never a metric label, so the number of
/// series depends on the number of client builds, not on the number of
/// addresses. Addresses that stop calling are dropped after `TTL`.
#[derive(Default)]
pub(crate) struct ClientCompatTracker {
    clients: DashMap<String, ClientCompat>,
}

impl ClientCompatTracker {
    pub(crate) const TTL: Duration = Duration::from_secs(60 * 60);

    /// Record the verdict of one request. `release_version` is `None` when the
    /// request carried no component info, which a client sends only on the
    /// first call of a session; its later calls keep the version and verdict
    /// recorded for the address. Returns true when the verdict of the address
    /// changed, including its first observation.
    pub(crate) fn record(
        &self,
        client_ip: &str,
        release_version: Option<&str>,
        verdict: &'static str,
        now: Instant,
    ) -> bool {
        if let Some(mut known) = self.clients.get_mut(client_ip) {
            known.last_seen = now;
            let Some(version) = release_version else {
                return false;
            };
            let changed = known.verdict != verdict;
            known.release_version = Self::version_label(version);
            known.verdict = verdict;
            return changed;
        }

        self.clients.insert(
            client_ip.to_string(),
            ClientCompat {
                release_version: release_version
                    .map(Self::version_label)
                    .unwrap_or_else(|| CLIENT_VERSION_NONE.to_string()),
                verdict,
                last_seen: now,
            },
        );
        true
    }

    /// Drop addresses not seen within the TTL and count the rest by
    /// `(release_version, verdict)`.
    pub(crate) fn counts(&self, now: Instant) -> HashMap<(String, &'static str), i64> {
        self.clients
            .retain(|_, c| now.saturating_duration_since(c.last_seen) <= Self::TTL);
        let mut counts = HashMap::new();
        for entry in self.clients.iter() {
            *counts
                .entry((entry.release_version.clone(), entry.verdict))
                .or_insert(0) += 1;
        }
        counts
    }

    fn version_label(version: &str) -> String {
        ReleaseVersion::parse(version)
            .map(|v| v.to_string())
            .unwrap_or_else(|_| CLIENT_VERSION_UNKNOWN.to_string())
    }
}

pub struct MasterMetrics {
    pub(crate) rpc_request_total_count: Counter,
    pub(crate) rpc_request_total_time: Counter,
    pub(crate) follower_read_accepted_total: CounterVec,
    pub(crate) follower_read_refused_total: CounterVec,
    pub(crate) active_connections: Gauge,

    pub(crate) capacity: Gauge,
    pub(crate) available: Gauge,
    pub(crate) fs_used: Gauge,
    pub(crate) block_num: Gauge,
    pub(crate) blocks_size_avg: Gauge,
    pub(crate) allocatable_capacity: Gauge,
    pub(crate) allocatable_available: Gauge,
    pub(crate) scheduled_bytes: GaugeVec,

    pub(crate) worker_num: GaugeVec,

    /// Worker state transitions, labelled with the `admin+liveness` pair on both
    /// sides. Answers "who moved this node and from where".
    pub(crate) worker_state_transition_count: CounterVec,
    /// Blocks whose replica count fell when locations were dropped (force
    /// completion or a lost worker).
    pub(crate) worker_orphan_blocks: Counter,
    /// Blocks still to be copied off a draining worker.
    pub(crate) worker_drain_blocks_remaining: GaugeVec,
    /// Workers whose drain cannot make progress, by reason
    /// (`stale` / `no_target_worker`).
    pub(crate) worker_drain_stalled: GaugeVec,
    /// Block locations that are the last copy of their block, summed over
    /// draining workers: forcing those drains would lose data.
    pub(crate) worker_drain_single_replica_blocks: Gauge,
    /// Bytes still held by `DECOMMISSIONED` nodes: retained, not counted as
    /// available capacity.
    pub(crate) worker_retained_bytes: Gauge,

    pub(crate) journal_queue_len: Gauge,
    pub(crate) journal_flush_count: Counter,
    pub(crate) journal_flush_time: Counter,
    /// Journal flush latency distribution. `journal_flush_time` only carries the
    /// sum, so an average hides the flush that took 2s while a million took
    /// 200us. Quantiles over this histogram are what the dashboard shows.
    pub(crate) journal_flush_duration_us: Histogram,
    pub(crate) journal_committed: Gauge,
    pub(crate) journal_term: Gauge,
    pub(crate) journal_applied: Gauge,
    pub(crate) journal_ufs_applied: Gauge,
    pub(crate) ufs_replay_pending: Gauge,
    pub(crate) master_safe_mode: Gauge,
    pub(crate) master_ufs_caught_up: Gauge,
    pub(crate) journal_ufs: GaugeVec,
    /// `MasterPhase` discriminant; see `MasterPhase::as_str` for the mapping.
    /// Exported as a number so a dashboard can chart transitions without
    /// parsing strings (#CV-94).
    pub(crate) master_service_phase: Gauge,
    /// Committed entries this node has not applied yet. Zero while it serves.
    pub(crate) master_replay_lag: Gauge,
    /// Percent of the current replay episode already applied, 100 when there is
    /// nothing left. Only meaningful while the denominator holds still. Integer
    /// like every other gauge here; `/phase` carries the exact ratio.
    pub(crate) master_replay_progress_percent: Gauge,
    /// Applied entries per second over the sampling window.
    pub(crate) master_apply_rate: Gauge,
    /// Journal entries written since the last registered snapshot. This is what
    /// a restart would replay, so it is the number that says "snapshot now".
    pub(crate) master_snapshot_lag_entries: Gauge,

    // Metadata DB keys as produced by `DBEngine::get_rocksdb_metrics`; journal
    // DB keys carry an extra `journal_` prefix.
    pub(crate) rocksdb_metrics: GaugeVec,

    // Metadata checkpoints, created by leader and follower snapshots. The leader's
    // periodic snapshot runs under the fs_dir write lock, so a slow checkpoint
    // stalls every metadata RPC. `checkpoint_count` counts
    // every attempt, including the failed ones in `checkpoint_fail_count`.
    // `checkpoint_flush_time` is the memtable flush part of `checkpoint_time`.
    pub(crate) checkpoint_count: Counter,
    pub(crate) checkpoint_fail_count: Counter,
    pub(crate) checkpoint_time: Counter,
    pub(crate) checkpoint_flush_time: Counter,
    pub(crate) checkpoint_last_duration_ms: Gauge,

    pub(crate) inode_dir_num: Gauge,
    pub(crate) inode_file_num: Gauge,

    // for the replication manager
    pub(crate) replication_staging_number: Gauge,
    pub(crate) replication_inflight_number: Gauge,
    pub(crate) replication_uncertain_number: Gauge,
    pub(crate) replication_failure_count: Counter,
    pub(crate) replication_timeout_count: Counter,

    pub(crate) operation_duration: HistogramVec,

    // Delay from request receipt to the executor starting to run the handler,
    // i.e. how long a request waited in the RPC runtime's queue. operation_duration
    // above only covers processing time, so this is what exposes executor
    // saturation (e.g. a read burst occupying the blocking pool) before clients
    // start timing out. The executor label separates the two blocking pools:
    // worker heartbeats/block reports and client session heartbeats wait on
    // actor_rt, all other sync RPCs wait on the main runtime (see
    // MasterHandler::uses_actor_rt). Client heartbeats must not queue behind a
    // metadata burst: a stall longer than master.client_session_timeout would
    // expire every session and release its lock ownership.
    pub(crate) rpc_queue_duration: HistogramVec,

    // for quota eviction (LRU)
    pub(crate) eviction_lru_cache_size: Gauge,
    pub(crate) eviction_trigger_count: Counter,
    pub(crate) eviction_files_deleted: Counter,
    pub(crate) eviction_bytes_freed: Counter,

    // Trash purge progress. `trash_pending_entries` is the number of entries
    // waiting in `/.Trash/.pending`; `trash_purging_entries` counts staged
    // synchronous deletes left in `/.Trash/.purging` for the purger (#1864).
    // The counters let operators derive average batch cost and purge
    // throughput without a per-entry histogram.
    pub(crate) trash_pending_entries: Gauge,
    pub(crate) trash_purging_entries: Gauge,
    pub(crate) trash_purge_batches: Counter,
    pub(crate) trash_purge_batch_duration_ms: Counter,
    pub(crate) trash_purge_inodes: Counter,
    pub(crate) trash_purge_errors: Counter,

    pub(crate) ttl_bucket_len: Gauge,
    pub(crate) ttl_total_inodes: Gauge,
    pub(crate) ttl_skipped_inodes: Counter,

    // fs_dir global lock health. The metadata lock cannot be sharded yet, so a
    // stalled or wedged lock freezes the whole control plane. These give the
    // stall an observable signal instead of a silent multi-minute freeze.
    pub(crate) fs_dir_stalled: Gauge,
    pub(crate) fs_dir_stall_total: Counter,
    pub(crate) fs_dir_probe_acquire_us: Gauge,

    // Compatibility metrics.
    // Per-verdict gauge: 1 for the current verdict of each worker
    // (compatible, missing_info, blocked, protocol_mismatch, version_too_old,
    // version_unknown). Only one label per worker is active at a time.
    pub(crate) compat_worker_verdict: GaugeVec,
    // Number of clients per release version and verdict. Client addresses are
    // not a label because they are not bounded by cluster size.
    pub(crate) compat_client_verdict: GaugeVec,
    // Number of times a client address changed to a verdict, by verdict.
    pub(crate) compat_client_verdict_total: CounterVec,
    compat_clients: ClientCompatTracker,
    // Total number of enforce-mode rejections, labelled by component and verdict.
    pub(crate) compat_enforce_rejected_total: CounterVec,

    // Dir-space snapshot observability (design doc §12). Point gauges are
    // aggregates over all tracked points — never per-path labels.
    pub(crate) dir_space_global_ready: Gauge,
    pub(crate) dir_space_global_generation: Gauge,
    pub(crate) dir_space_global_age_seconds: Gauge,
    pub(crate) dir_space_global_entries: Gauge,
    pub(crate) dir_space_points_configured: Gauge,
    pub(crate) dir_space_points_ready: Gauge,
    pub(crate) dir_space_points_oldest_age_seconds: Gauge,
    pub(crate) dir_space_points_entries: Gauge,
    pub(crate) dir_space_scan_duration_ms: HistogramVec,
    pub(crate) dir_space_scan_total: CounterVec,
    pub(crate) dir_space_scan_inodes: GaugeVec,
    pub(crate) dir_space_scan_edges: GaugeVec,
    pub(crate) dir_space_scan_anomalies: GaugeVec,

    // Client session lease (#1885). `active_sessions` is a gauge because sessions
    // are leader-local: after a failover it drops to zero and climbs back as
    // clients re-register, which is exactly what an operator needs to see. The
    // counters make a crash storm visible as reclamation work.
    pub(crate) active_sessions: Gauge,
    pub(crate) expired_sessions_total: Counter,
    pub(crate) reaped_locks_total: Counter,
    pub(crate) reaped_writers_total: Counter,
    pub(crate) reaped_stale_hints_total: Counter,

    // Load and export jobs of the master job manager (#224), labelled by kind
    // (load, export) and state. fs_mode exports are submitted from journal
    // apply, so no client ever sees their job id; these metrics are how an
    // operator notices that they fail. `load_jobs` is rebuilt from the job
    // store on every scrape, and finished runs stay in it until
    // `job.terminal_retention` expires. The counters see every run that enters
    // the job store; a direct submit that needs no work (target already cached,
    // nothing to copy) returns at submit time and is not counted.
    pub(crate) load_jobs: GaugeVec,
    pub(crate) load_job_finished_total: CounterVec,
    // From job creation (including the wait for a load slot) to the terminal state.
    pub(crate) load_job_duration_ms: HistogramVec,
    // Worker task reports. A report for a run that has already finished is
    // ignored by the job store and is not counted.
    pub(crate) load_task_finished_total: CounterVec,
    // Bytes copied by load and export tasks, summed from the growth of each
    // task's reported `loaded_size`. Use rate() for throughput.
    pub(crate) load_task_loaded_bytes_total: CounterVec,
}

impl MasterMetrics {
    pub fn new() -> CommonResult<Self> {
        let buckets = vec![
            10.0,
            50.0,
            100.0,
            500.0,
            1000.0,
            5000.0,
            10000.0,
            50000.0,
            100000.0,
            500_000.0,
            1_000_000.0,
            5_000_000.0,
            10_000_000.0,
            30_000_000.0,
        ];
        let wm = Self {
            rpc_request_total_count: m::new_counter(
                "rpc_request_total_count",
                "Number of rpc request",
            )?,
            rpc_request_total_time: m::new_counter(
                "rpc_request_time",
                "Rpc request time duration(ms)",
            )?,
            follower_read_accepted_total: m::new_counter_vec(
                "master_follower_read_accepted_total",
                "Follower read RPCs served after freshness and location readiness checks",
                &["rpc_code"],
            )?,
            follower_read_refused_total: m::new_counter_vec(
                "master_follower_read_refused_total",
                "Follower RPCs refused by the allowlist or freshness/readiness checks",
                &["rpc_code", "reason"],
            )?,
            active_connections: m::new_gauge(
                "master_active_connections",
                "Number of active RPC connections to master",
            )?,

            capacity: m::new_gauge("capacity", "Total storage capacity")?,
            available: m::new_gauge("available", "Storage directory available space")?,
            fs_used: m::new_gauge("fs_used", "Space used by the file system")?,
            block_num: m::new_gauge("num_blocks", "Total block number")?,
            blocks_size_avg: m::new_gauge("blocks_size_avg", "Average block size")?,
            allocatable_capacity: m::new_gauge(
                "allocatable_capacity",
                "Storage capacity eligible for new writes (Live workers only)",
            )?,
            allocatable_available: m::new_gauge(
                "allocatable_available",
                "Available space eligible for new writes (Live workers only)",
            )?,
            scheduled_bytes: m::new_gauge_vec(
                "scheduled_bytes",
                "In-flight allocations not yet reflected in heartbeat available space, labelled by live worker connect address",
                &["worker_addr"],
            )?,
            worker_num: m::new_gauge_vec("worker_num", "The number of lived workers", &["tag"])?,
            worker_state_transition_count: m::new_counter_vec(
                "worker_state_transition_count",
                "Worker state transitions by from/to state",
                &["from", "to"],
            )?,
            worker_orphan_blocks: m::new_counter(
                "worker_orphan_blocks",
                "Block locations dropped by force completion or worker loss",
            )?,
            worker_drain_blocks_remaining: m::new_gauge_vec(
                "worker_drain_blocks_remaining",
                "Block locations still to be copied off a draining worker",
                &["worker"],
            )?,
            worker_drain_stalled: m::new_gauge_vec(
                "worker_drain_stalled",
                "Draining workers that cannot make progress, by reason",
                &["reason"],
            )?,
            worker_drain_single_replica_blocks: m::new_gauge(
                "worker_drain_single_replica_blocks",
                "Block locations on draining workers that are the last copy of their block",
            )?,
            worker_retained_bytes: m::new_gauge(
                "worker_retained_bytes",
                "Bytes retained on decommissioned workers",
            )?,

            journal_queue_len: m::new_gauge("journal_queue_len", "Journal queue length")?,
            journal_flush_count: m::new_counter(
                "journal_flush_count",
                "Number of flushes of log entries",
            )?,
            journal_flush_time: m::new_counter("journal_flush_time", "Log entry flush time")?,
            journal_flush_duration_us: m::new_histogram_with_buckets(
                "journal_flush_duration_us",
                "Journal batch flush latency in microseconds",
                &buckets,
            )?,
            journal_committed: m::new_gauge(
                "journal_committed",
                "Raft commit index of the journal",
            )?,
            journal_term: m::new_gauge("journal_term", "Current Raft term of the journal")?,
            journal_applied: m::new_gauge(
                "journal_applied",
                "Last applied index of the journal state machine",
            )?,
            journal_ufs_applied: m::new_gauge(
                "journal_ufs_applied",
                "Last UFS-applied index of the journal state machine",
            )?,
            ufs_replay_pending: m::new_gauge(
                "ufs_replay_pending",
                "Journal entries whose UFS side effect is not finished",
            )?,
            master_safe_mode: m::new_gauge(
                "master_safe_mode",
                "1 when the master has stopped metadata service after unrecoverable damage",
            )?,
            master_ufs_caught_up: m::new_gauge(
                "master_ufs_caught_up",
                "1 when the leader UFS watermark has reached the metadata watermark",
            )?,
            journal_ufs: m::new_gauge_vec(
                "journal_ufs",
                "UFS replay progress by kind: pending backlog, stalled flag, failed attempts",
                &["kind"],
            )?,
            master_service_phase: m::new_gauge(
                "master_service_phase",
                "MasterPhase discriminant: 1 Starting, 2 Replaying, 3 CatchingUp, 4 Serving, \
                 5 StandingBy, 6 Quarantined, 7 SafeMode, 8 Exiting",
            )?,
            master_replay_lag: m::new_gauge(
                "master_replay_lag",
                "Committed entries not yet applied by this master",
            )?,
            master_replay_progress_percent: m::new_gauge(
                "master_replay_progress_percent",
                "Percent of the current replay episode applied, 100 when caught up \
                 (static denominator only)",
            )?,
            master_apply_rate: m::new_gauge(
                "master_apply_rate",
                "Metadata entries applied per second over the sampling window",
            )?,
            master_snapshot_lag_entries: m::new_gauge(
                "master_snapshot_lag_entries",
                "Journal entries written since the last registered snapshot",
            )?,

            rocksdb_metrics: m::new_gauge_vec("rocksdb_metrics", "RocksDB metrics", &["tag"])?,

            checkpoint_count: m::new_counter(
                "checkpoint_count",
                "Number of metadata checkpoint attempts, including failed ones",
            )?,
            checkpoint_fail_count: m::new_counter(
                "checkpoint_fail_count",
                "Number of failed metadata checkpoint attempts",
            )?,
            checkpoint_time: m::new_counter(
                "checkpoint_time",
                "Total time spent creating metadata checkpoints (ms); divide by checkpoint_count for the average",
            )?,
            checkpoint_flush_time: m::new_counter(
                "checkpoint_flush_time",
                "Part of checkpoint_time spent flushing memtables before the checkpoint (ms)",
            )?,
            checkpoint_last_duration_ms: m::new_gauge(
                "checkpoint_last_duration_ms",
                "Duration of the last metadata checkpoint attempt (ms)",
            )?,

            inode_dir_num: m::new_gauge("inode_dir_num", "Total dir")?,

            inode_file_num: m::new_gauge("inode_file_num", "Total file")?,

            replication_staging_number: m::new_gauge(
                "replication_staging_number",
                "Replication stage number",
            )?,
            replication_inflight_number: m::new_gauge(
                "replication_inflight_number",
                "Replication jobs awaiting a result",
            )?,
            replication_uncertain_number: m::new_gauge(
                "replication_uncertain_number",
                "Replication jobs whose remote execution state is uncertain",
            )?,
            replication_failure_count: m::new_counter(
                "replication_failure_count",
                "Total failure count",
            )?,
            replication_timeout_count: m::new_counter(
                "replication_timeout_count",
                "Total replication submit and result timeouts",
            )?,

            operation_duration: m::new_histogram_vec_with_buckets(
                "operation_duration",
                "Operation duration including WorkerHeartbeat",
                &["operation"],
                &buckets,
            )?,

            rpc_queue_duration: m::new_histogram_vec_with_buckets(
                "rpc_queue_duration",
                "Latency(us) from request receipt to handler execution start, by executor pool (main/actor)",
                &["executor"],
                &buckets,
            )?,

            // Quota eviction metrics
            eviction_lru_cache_size: m::new_gauge(
                "eviction_lru_cache_size",
                "Number of files tracked in LRU cache for eviction",
            )?,
            eviction_trigger_count: m::new_counter(
                "eviction_trigger_count",
                "Number of times eviction was triggered",
            )?,
            eviction_files_deleted: m::new_counter(
                "eviction_files_deleted",
                "Total number of files deleted by eviction",
            )?,
            eviction_bytes_freed: m::new_counter(
                "eviction_bytes_freed",
                "Total bytes freed by eviction",
            )?,
            trash_pending_entries: m::new_gauge(
                "trash_pending_entries",
                "Trash entries staged for the async purger in /.Trash/.pending",
            )?,
            trash_purging_entries: m::new_gauge(
                "trash_purging_entries",
                "Staged synchronous deletes left in /.Trash/.purging for the purger",
            )?,
            trash_purge_batches: m::new_counter(
                "trash_purge_batches",
                "Number of bounded trash purge batches applied",
            )?,
            trash_purge_batch_duration_ms: m::new_counter(
                "trash_purge_batch_duration_ms",
                "Total time spent applying trash purge batches (ms); divide by trash_purge_batches for the average",
            )?,
            trash_purge_inodes: m::new_counter(
                "trash_purge_inodes",
                "Inodes reclaimed by the trash purger",
            )?,
            trash_purge_errors: m::new_counter(
                "trash_purge_errors",
                "Failed trash purge batches",
            )?,

            ttl_bucket_len: m::new_gauge("ttl_bucket_len", "Number of TTL buckets")?,
            ttl_total_inodes: m::new_gauge("ttl_total_inodes", "Total number of inodes")?,
            ttl_skipped_inodes: m::new_counter(
                "ttl_skipped_inodes",
                "Total number of inodes skipped while updating TTL buckets",
            )?,
            fs_dir_stalled: m::new_gauge(
                "fs_dir_stalled",
                "1 when the fs_dir metadata lock has been unacquirable beyond the stall threshold",
            )?,
            fs_dir_stall_total: m::new_counter(
                "fs_dir_stall_total",
                "Total number of fs_dir metadata lock stall episodes detected",
            )?,
            fs_dir_probe_acquire_us: m::new_gauge(
                "fs_dir_probe_acquire_us",
                "Latency in microseconds of the last fs_dir watchdog read-lock probe",
            )?,

            compat_worker_verdict: m::new_gauge_vec(
                "compat_worker_verdict",
                "Current compatibility verdict per worker, labelled by worker_id and verdict",
                &["worker_id", "verdict"],
            )?,
            compat_client_verdict: m::new_gauge_vec(
                "compat_client_verdict",
                "Number of clients per release version and compatibility verdict",
                &["release_version", "verdict"],
            )?,
            compat_client_verdict_total: m::new_counter_vec(
                "compat_client_verdict_total",
                "Total number of times a client address changed to a compatibility verdict, labelled by verdict",
                &["verdict"],
            )?,
            compat_clients: ClientCompatTracker::default(),
            compat_enforce_rejected_total: m::new_counter_vec(
                "compat_enforce_rejected_total",
                "Total number of enforce-mode compatibility rejections, labelled by component and verdict",
                &["component", "verdict"],
            )?,
            dir_space_global_ready: m::new_gauge(
                "dir_space_global_ready",
                "1 when a complete global dir-space snapshot has been published",
            )?,
            dir_space_global_generation: m::new_gauge(
                "dir_space_global_generation",
                "Generation of the current global dir-space snapshot",
            )?,
            dir_space_global_age_seconds: m::new_gauge(
                "dir_space_global_age_seconds",
                "Age in seconds of the current global dir-space snapshot, refreshed by the coordinator tick",
            )?,
            dir_space_global_entries: m::new_gauge(
                "dir_space_global_entries",
                "Directory aggregation entries in the current global dir-space snapshot",
            )?,
            dir_space_points_configured: m::new_gauge(
                "dir_space_points_configured",
                "Number of tracked dir-space points currently configured",
            )?,
            dir_space_points_ready: m::new_gauge(
                "dir_space_points_ready",
                "Number of tracked dir-space points with a published generation",
            )?,
            dir_space_points_oldest_age_seconds: m::new_gauge(
                "dir_space_points_oldest_age_seconds",
                "Age in seconds of the oldest published tracked-point snapshot",
            )?,
            dir_space_points_entries: m::new_gauge(
                "dir_space_points_entries",
                "Total directory aggregation entries across all tracked-point snapshots",
            )?,
            dir_space_scan_duration_ms: m::new_histogram_vec(
                "dir_space_scan_duration_ms",
                "Dir-space scan duration in milliseconds, labelled by scope and result",
                &["scope", "result"],
            )?,
            dir_space_scan_total: m::new_counter_vec(
                "dir_space_scan_total",
                "Dir-space scan round outcomes, labelled by scope and result (success/failed/timeout/limit/role_lost/busy)",
                &["scope", "result"],
            )?,
            dir_space_scan_inodes: m::new_gauge_vec(
                "dir_space_scan_inodes",
                "Inodes processed by the last successful dir-space scan round, labelled by scope",
                &["scope"],
            )?,
            dir_space_scan_edges: m::new_gauge_vec(
                "dir_space_scan_edges",
                "Edges processed by the last successful dir-space scan round, labelled by scope",
                &["scope"],
            )?,
            dir_space_scan_anomalies: m::new_gauge_vec(
                "dir_space_scan_anomalies",
                "Anomalies recorded by the last successful dir-space scan round, labelled by scope",
                &["scope"],
            )?,

            active_sessions: m::new_gauge(
                "master_active_sessions",
                "Client sessions currently holding a lease with this master",
            )?,
            expired_sessions_total: m::new_counter(
                "master_expired_sessions_total",
                "Client sessions reclaimed after their lease expired",
            )?,
            reaped_locks_total: m::new_counter(
                "master_reaped_locks_total",
                "Inode lock sets released because their owning session expired",
            )?,
            reaped_writers_total: m::new_counter(
                "master_reaped_writers_total",
                "Writer markers released because their owning session expired",
            )?,
            reaped_stale_hints_total: m::new_counter(
                "master_reaped_stale_hints_total",
                "Ownership hints dropped because the inode or ownership was already gone",
            )?,

            load_jobs: m::new_gauge_vec(
                "load_jobs",
                "Load and export jobs held by the master job manager, labelled by kind and state",
                &["kind", "state"],
            )?,
            load_job_finished_total: m::new_counter_vec(
                "load_job_finished_total",
                "Load and export job runs that reached a terminal state, labelled by kind and state",
                &["kind", "state"],
            )?,
            load_job_duration_ms: m::new_histogram_vec_with_buckets(
                "load_job_duration_ms",
                "Time (ms) from creating a load or export job run to its terminal state, labelled by kind and state",
                &["kind", "state"],
                &[
                    100.0,
                    1_000.0,
                    10_000.0,
                    60_000.0,
                    300_000.0,
                    900_000.0,
                    1_800_000.0,
                    3_600_000.0,
                    10_800_000.0,
                    21_600_000.0,
                    43_200_000.0,
                    86_400_000.0,
                ],
            )?,
            load_task_finished_total: m::new_counter_vec(
                "load_task_finished_total",
                "Load and export tasks reported in a terminal state by workers, labelled by kind and state",
                &["kind", "state"],
            )?,
            load_task_loaded_bytes_total: m::new_counter_vec(
                "load_task_loaded_bytes_total",
                "Bytes copied by load and export tasks as reported by workers, labelled by kind",
                &["kind"],
            )?,
        };

        Ok(wm)
    }

    fn refresh_rocksdb_metrics(metric: &GaugeVec, prefix: &str, values: HashMap<String, u64>) {
        for (key, value) in values {
            metric
                .with_label_values(&[&format!("{}{}", prefix, key)])
                .set(value as i64);
        }
    }

    fn refresh_scheduled_bytes(metric: &GaugeVec, workers: &[WorkerInfo]) {
        metric.reset();
        for worker in workers {
            let worker_addr = worker.address.connect_addr();
            metric
                .with_label_values(&[&worker_addr])
                .set(worker.scheduled_bytes.max(0));
        }
    }

    /// Record the compatibility verdict of a client request.
    /// `release_version` is `None` when the request carried no component info.
    pub(crate) fn record_client_compat(
        &self,
        client_ip: &str,
        release_version: Option<&str>,
        verdict: &'static str,
    ) {
        if self
            .compat_clients
            .record(client_ip, release_version, verdict, Instant::now())
        {
            self.compat_client_verdict_total
                .with_label_values(&[verdict])
                .inc();
        }
    }

    /// Rebuild the client gauge from the tracker, so versions and verdicts no
    /// client holds any more disappear from the output.
    fn refresh_client_verdicts(&self) {
        self.compat_client_verdict.reset();
        for ((version, verdict), count) in self.compat_clients.counts(Instant::now()) {
            self.compat_client_verdict
                .with_label_values(&[&version, verdict])
                .set(count);
        }
    }

    /// Records one metadata checkpoint attempt. `total_ms` covers the memtable
    /// flush (`flush_ms`) and the checkpoint creation; a failed attempt records
    /// the time spent until it failed.
    pub(crate) fn record_checkpoint(&self, flush_ms: u64, total_ms: u64, succeeded: bool) {
        self.checkpoint_count.inc();
        if !succeeded {
            self.checkpoint_fail_count.inc();
        }
        // Total before flush: a reader that samples the flush time first then
        // always sees checkpoint_time >= checkpoint_flush_time.
        self.checkpoint_time.inc_by(total_ms as i64);
        self.checkpoint_flush_time.inc_by(flush_ms as i64);
        self.checkpoint_last_duration_ms.set(total_ms as i64);
    }

    /// Records a job run that reached a terminal state `duration_ms` after it
    /// was created.
    pub(crate) fn record_load_job_finished(
        &self,
        kind: JobTaskType,
        state: JobTaskState,
        duration_ms: u64,
    ) {
        let labels = [job_kind_label(kind), job_state_label(state)];
        self.load_job_finished_total
            .with_label_values(&labels)
            .inc();
        self.load_job_duration_ms
            .with_label_values(&labels)
            .observe(duration_ms as f64);
    }

    /// Records a worker report that moved a task from `previous` to `current`:
    /// the bytes copied since the previous report and, on the first terminal
    /// report, the task's final state.
    pub(crate) fn record_load_task_report(
        &self,
        kind: JobTaskType,
        previous: &JobTaskProgress,
        current: &JobTaskProgress,
    ) {
        let kind = job_kind_label(kind);
        // A retried task restarts from 0; the bytes it copies again are new
        // work, so only growth is counted and a drop is not subtracted.
        let copied = current.loaded_size.saturating_sub(previous.loaded_size);
        if copied > 0 {
            self.load_task_loaded_bytes_total
                .with_label_values(&[kind])
                .inc_by(copied);
        }

        if !previous.state.is_finish() && current.state.is_finish() {
            self.load_task_finished_total
                .with_label_values(&[kind, job_state_label(current.state)])
                .inc();
        }
    }

    /// Rebuilds `load_jobs` from the job store. The master renders it before
    /// `text_output`, which has no access to the job manager.
    pub(crate) fn refresh_load_jobs(&self, jobs: &JobStore) {
        Self::refresh_load_job_counts(&self.load_jobs, jobs);
    }

    fn refresh_load_job_counts(metric: &GaugeVec, jobs: &JobStore) {
        let mut counts = HashMap::new();
        for kind in LOAD_JOB_KINDS {
            for state in LOAD_JOB_STATES {
                counts.insert((job_kind_label(kind), job_state_label(state)), 0);
            }
        }
        for entry in jobs.iter() {
            let job = entry.value();
            let state: JobTaskState = job.state.state();
            *counts
                .entry((job_kind_label(job.kind), job_state_label(state)))
                .or_insert(0) += 1;
        }

        // Count first, so the window in which the gauge is empty stays short.
        metric.reset();
        for ((kind, state), count) in counts {
            metric.with_label_values(&[kind, state]).set(count);
        }
    }

    /// Renders all metrics. `journal_store` adds the journal DB RocksDB metrics;
    /// `None` skips them, e.g. for a filesystem built without a journal system.
    pub fn text_output(
        &self,
        fs: MasterFilesystem,
        journal_store: Option<&RocksLogStorage>,
    ) -> CommonResult<String> {
        let filesystem_info = fs.filesystem_info()?;
        self.refresh_client_verdicts();
        self.capacity.set(filesystem_info.capacity);
        self.available.set(filesystem_info.available);
        self.fs_used.set(filesystem_info.fs_used);
        self.block_num.set(filesystem_info.block_num);
        self.allocatable_capacity
            .set(filesystem_info.allocatable_capacity);
        self.allocatable_available
            .set(filesystem_info.allocatable_available);
        Self::refresh_scheduled_bytes(&self.scheduled_bytes, &filesystem_info.live_workers);
        self.ufs_replay_pending
            .set(fs.master_monitor.ufs_pending() as i64);
        self.master_safe_mode
            .set(i64::from(fs.master_monitor.is_safe_mode()));
        self.master_ufs_caught_up
            .set(i64::from(fs.master_monitor.ufs_caught_up()));

        let quarantined = journal_store
            .map(|store| matches!(store.recovery_state(), Ok(Some(_))))
            .unwrap_or(false);
        let now_ms = LocalTime::mills();
        let phase = fs.master_monitor.phase(quarantined);
        let progress = fs.master_monitor.progress();
        progress.observe_phase(phase, now_ms);
        self.master_service_phase.set(i64::from(i8::from(phase)));

        let applied = progress.applied();
        let committed = progress.commit();
        self.master_replay_lag
            .set(committed.saturating_sub(applied) as i64);

        // A ratio is only exported while the denominator is still; a leader
        // that is still catching up gets a lag number instead.
        let total = committed.saturating_sub(progress.replay_base().min(applied));
        let ratio = if phase == MasterPhase::CatchingUp && total > 0 {
            0.0
        } else if total == 0 {
            1.0
        } else {
            applied.saturating_sub(progress.replay_base()) as f64 / total as f64
        };
        self.master_replay_progress_percent
            .set((ratio * 100.0).round() as i64);

        if let Some(rate) = progress.record_sample(now_ms) {
            self.master_apply_rate
                .set(rate.entries_per_sec.round() as i64);
        }

        if let Some(store) = journal_store {
            if let Ok(Some(snapshot)) = store.latest_snapshot() {
                let snapshot_index = snapshot.get_metadata().index;
                self.master_snapshot_lag_entries
                    .set(applied.saturating_sub(snapshot_index) as i64);
            } else {
                self.master_snapshot_lag_entries.set(applied as i64);
            }
        }

        if filesystem_info.block_num > 0 {
            let avg_size = filesystem_info.fs_used / filesystem_info.block_num;
            self.blocks_size_avg.set(avg_size);
        }

        let fs_dir = fs.fs_dir.read();
        let rocksdb_metrics = fs_dir.get_rocks_store().get_rocksdb_metrics()?;
        Self::refresh_rocksdb_metrics(&self.rocksdb_metrics, "", rocksdb_metrics);

        let ttl_list = fs_dir.get_ttl_bucket_list();
        drop(fs_dir);
        self.ttl_bucket_len.set(ttl_list.buckets_len() as i64);
        self.ttl_total_inodes.set(ttl_list.total_inodes() as i64);

        // Read after releasing fs_dir: the journal DB has its own lock.
        if let Some(journal_store) = journal_store {
            let journal_metrics = journal_store.get_rocksdb_metrics()?;
            Self::refresh_rocksdb_metrics(&self.rocksdb_metrics, "journal_", journal_metrics);
        }

        self.worker_num
            .with_label_values(&["live"])
            .set(filesystem_info.live_workers.len() as i64);
        // `blacklist` and `stale` partition the alive-but-not-writable workers,
        // so live + blacklist + stale + decommission + lost is the fleet size.
        let stale_workers = filesystem_info
            .blacklist_workers
            .iter()
            .filter(|worker| worker.liveness() == curvine_model::Liveness::Stale)
            .count() as i64;
        self.worker_num
            .with_label_values(&["blacklist"])
            .set(filesystem_info.blacklist_workers.len() as i64 - stale_workers);
        self.worker_num
            .with_label_values(&["stale"])
            .set(stale_workers);
        self.worker_num
            .with_label_values(&["decommission"])
            .set(filesystem_info.decommission_workers.len() as i64);
        self.worker_num
            .with_label_values(&["lost"])
            .set(filesystem_info.lost_workers.len() as i64);
        // Bytes kept on drained nodes: retained on disk, never advertised as
        // allocatable capacity.
        self.worker_retained_bytes.set(
            filesystem_info
                .decommission_workers
                .iter()
                .map(|worker| worker.fs_used.max(0))
                .sum(),
        );

        Metrics::text_output()
    }

    pub fn get_or_register(&self, value: &MetricValue) -> CommonResult<CounterVec> {
        if let Some(v) = Metrics::get(&value.name) {
            return v.try_into_counter_vec();
        }

        let label_values: Vec<&str> = value.tags.keys().map(|v| v.as_str()).collect();
        let metric = m::new_counter_vec(&value.name, &value.name, &label_values)?;
        Ok(metric)
    }

    pub fn metrics_report(&self, metrics: Vec<MetricValue>) -> CommonResult<()> {
        for value in metrics {
            let counter = match self.get_or_register(&value) {
                Ok(v) => v,
                Err(e) => {
                    warn!("Metric not found: {}: {}", value.name, e);
                    continue;
                }
            };

            let label_values: Vec<&str> = value.tags.values().map(|v| v.as_str()).collect();
            counter
                .with_label_values(&label_values)
                .inc_by(value.value as i64)
        }

        Ok(())
    }
}

impl Debug for MasterMetrics {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "MasterMetrics")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::master::JobContext;
    use curvine_config::{ClientConf, JournalConf};
    use curvine_model::{LoadJobCommand, MountInfo, WorkerAddress};
    use curvine_runtime::common::Utils;

    fn count(
        counts: &HashMap<(String, &'static str), i64>,
        version: &str,
        verdict: &'static str,
    ) -> i64 {
        counts
            .get(&(version.to_string(), verdict))
            .copied()
            .unwrap_or(0)
    }

    #[test]
    fn client_verdicts_are_aggregated_by_release_version() {
        let tracker = ClientCompatTracker::default();
        let now = Instant::now();
        // Many addresses on one build produce one entry, not one each.
        for i in 0..50 {
            tracker.record(
                &format!("10.0.0.{i}"),
                Some("0.1.0"),
                "version_too_old",
                now,
            );
        }
        tracker.record("10.0.1.1", Some("0.3.0"), "compatible", now);

        let counts = tracker.counts(now);
        assert_eq!(counts.len(), 2);
        assert_eq!(count(&counts, "0.1.0", "version_too_old"), 50);
        assert_eq!(count(&counts, "0.3.0", "compatible"), 1);
    }

    #[test]
    fn later_statfs_without_component_info_keeps_the_recorded_version() {
        let tracker = ClientCompatTracker::default();
        let now = Instant::now();
        assert!(tracker.record("10.0.0.1", Some("0.3.0"), "compatible", now));

        // The following calls of the session carry no component_info.
        assert!(!tracker.record("10.0.0.1", None, "missing_info", now));
        assert!(!tracker.record("10.0.0.1", None, "missing_info", now));

        let counts = tracker.counts(now);
        assert_eq!(counts.len(), 1);
        assert_eq!(count(&counts, "0.3.0", "compatible"), 1);
    }

    #[test]
    fn first_call_without_component_info_is_a_legacy_client() {
        let tracker = ClientCompatTracker::default();
        let now = Instant::now();
        assert!(tracker.record("10.0.0.1", None, "missing_info", now));
        assert_eq!(count(&tracker.counts(now), "none", "missing_info"), 1);
    }

    #[test]
    fn record_reports_verdict_transitions_only() {
        let tracker = ClientCompatTracker::default();
        let now = Instant::now();
        assert!(tracker.record("10.0.0.1", Some("0.2.0"), "blocked", now));
        assert!(!tracker.record("10.0.0.1", Some("0.2.0"), "blocked", now));
        assert!(tracker.record("10.0.0.1", Some("0.3.0"), "compatible", now));
        assert!(tracker.record("10.0.0.1", Some("0.2.0"), "blocked", now));
    }

    #[test]
    fn unparseable_versions_share_one_label() {
        let tracker = ClientCompatTracker::default();
        let now = Instant::now();
        for i in 0..20 {
            tracker.record(
                &format!("10.0.0.{i}"),
                Some(&format!("garbage-{i}")),
                "version_unknown",
                now,
            );
        }
        let counts = tracker.counts(now);
        assert_eq!(counts.len(), 1);
        assert_eq!(count(&counts, "unknown", "version_unknown"), 20);
    }

    #[test]
    fn idle_clients_expire() {
        let tracker = ClientCompatTracker::default();
        let start = Instant::now();
        tracker.record("10.0.0.1", Some("0.3.0"), "compatible", start);
        tracker.record(
            "10.0.0.2",
            Some("0.3.0"),
            "compatible",
            start + ClientCompatTracker::TTL,
        );

        let later = start + ClientCompatTracker::TTL + Duration::from_secs(1);
        assert_eq!(count(&tracker.counts(later), "0.3.0", "compatible"), 1);
        assert!(tracker.counts(later + ClientCompatTracker::TTL).is_empty());
    }

    #[test]
    fn journal_rocksdb_metrics_are_exported_with_a_journal_prefix() {
        let metric_name = "test_journal_rocksdb_metrics_are_exported_with_a_journal_prefix";
        let metric = m::new_gauge_vec(metric_name, metric_name, &["tag"]).unwrap();
        let conf = JournalConf {
            journal_dir: Utils::test_sub_dir(format!("journal-metrics-{}", Utils::rand_str(6))),
            ..Default::default()
        };
        let journal_store = RocksLogStorage::from_conf(&conf, true);

        let values = journal_store.get_rocksdb_metrics().unwrap();
        MasterMetrics::refresh_rocksdb_metrics(&metric, "journal_", values);

        let output = Metrics::text_output().unwrap();
        for cf in ["default", "meta"] {
            let tag = format!("journal_cf_{cf}_rocksdb_num-files-at-level0");
            assert!(
                output.contains(&format!("{metric_name}{{tag=\"{tag}\"}}")),
                "{output}"
            );
        }
        assert!(
            output.contains(&format!(
                "{metric_name}{{tag=\"journal_db_rocksdb_is-write-stopped\"}} 0"
            )),
            "{output}"
        );
    }

    // Counters only increase, so delta assertions stay valid even when other
    // tests in this binary create checkpoints concurrently.
    #[test]
    fn record_checkpoint_counts_attempts_failures_and_durations() {
        Master::init_test_metrics();
        let metrics = Master::get_metrics().unwrap();
        let count = metrics.checkpoint_count.get();
        let fail_count = metrics.checkpoint_fail_count.get();
        let time = metrics.checkpoint_time.get();
        let flush_time = metrics.checkpoint_flush_time.get();

        metrics.record_checkpoint(3, 10, true);
        metrics.record_checkpoint(4, 5, false);

        assert!(metrics.checkpoint_count.get() >= count + 2);
        assert!(metrics.checkpoint_fail_count.get() > fail_count);
        assert!(metrics.checkpoint_time.get() >= time + 15);
        assert!(metrics.checkpoint_flush_time.get() >= flush_time + 7);
    }

    fn live_worker(ip: &str, rpc_port: u32, scheduled_bytes: i64) -> WorkerInfo {
        let mut worker = WorkerInfo::new(
            WorkerAddress {
                worker_id: rpc_port,
                hostname: "worker".to_string(),
                ip_addr: ip.to_string(),
                rpc_port,
                web_port: 0,
            },
            1,
        );
        worker.scheduled_bytes = scheduled_bytes;
        worker
    }

    /// Value of the exported sample for `name` carrying every label in
    /// `labels`, if present.
    ///
    /// Label-order and extra-label independent, so assertions do not break when
    /// the process adds labels at gather time (e.g. `cluster_id`).
    fn sample_value(output: &str, name: &str, labels: &[(&str, &str)]) -> Option<f64> {
        output.lines().find_map(|line| {
            let (series, value) = line.rsplit_once(' ')?;
            let (series_name, series_labels) = match series.split_once('{') {
                Some((n, rest)) => (n, rest.trim_end_matches('}')),
                None => (series, ""),
            };
            if series_name != name {
                return None;
            }
            let matched = labels.iter().all(|(k, v)| {
                let expected = format!("{k}=\"{v}\"");
                series_labels.split(',').any(|pair| pair == expected)
            });
            if !matched {
                return None;
            }
            value.parse::<f64>().ok()
        })
    }

    #[test]
    fn scheduled_bytes_drops_lost_worker_labels() {
        let metric_name = "test_scheduled_bytes_drops_lost_worker_labels";
        let metric = m::new_gauge_vec(metric_name, metric_name, &["worker_addr"]).unwrap();
        let lost = "10.0.0.1:8995";

        MasterMetrics::refresh_scheduled_bytes(&metric, &[live_worker("10.0.0.1", 8995, 128)]);
        let output = Metrics::text_output().unwrap();
        assert_eq!(
            sample_value(&output, metric_name, &[("worker_addr", lost)]),
            Some(128.0),
            "{output}"
        );

        MasterMetrics::refresh_scheduled_bytes(&metric, &[]);
        let output = Metrics::text_output().unwrap();
        assert_eq!(
            sample_value(&output, metric_name, &[("worker_addr", lost)]),
            None,
            "{output}"
        );
    }

    fn job(job_id: &str, source_path: &str, kind: JobTaskType, state: JobTaskState) -> JobContext {
        let mut job = JobContext::with_conf(
            &LoadJobCommand::builder(source_path).build(),
            job_id.to_string(),
            source_path.to_string(),
            "/target".to_string(),
            &MountInfo::default(),
            &ClientConf::default(),
            1,
            kind,
        );
        job.state.advance_state(state, true);
        job
    }

    #[test]
    fn load_jobs_are_counted_by_kind_and_state() {
        let metric_name = "test_load_jobs_are_counted_by_kind_and_state";
        let metric = m::new_gauge_vec(metric_name, metric_name, &["kind", "state"]).unwrap();
        let series = |kind: &str, state: &str, value: i64| {
            format!("{metric_name}{{kind=\"{kind}\",state=\"{state}\"}} {value}")
        };

        let jobs = JobStore::new();
        for (job_id, source_path, kind, state) in [
            (
                "load-1",
                "s3://bucket/a",
                JobTaskType::Load,
                JobTaskState::Loading,
            ),
            (
                "load-2",
                "s3://bucket/b",
                JobTaskType::Load,
                JobTaskState::Loading,
            ),
            (
                "load-3",
                "s3://bucket/c",
                JobTaskType::Load,
                JobTaskState::Failed,
            ),
            (
                "export-1",
                "/fs/a",
                JobTaskType::Export,
                JobTaskState::Pending,
            ),
            (
                "export-2",
                "/fs/b",
                JobTaskType::Export,
                JobTaskState::PartialSuccess,
            ),
        ] {
            jobs.insert(job_id.to_string(), job(job_id, source_path, kind, state));
        }

        MasterMetrics::refresh_load_job_counts(&metric, &jobs);
        let output = Metrics::text_output().unwrap();
        for expected in [
            series("load", "loading", 2),
            series("load", "failed", 1),
            series("load", "pending", 0),
            series("load", "completed", 0),
            series("load", "canceled", 0),
            series("load", "partial_success", 0),
            series("export", "pending", 1),
            series("export", "partial_success", 1),
            series("export", "loading", 0),
        ] {
            assert!(output.contains(&expected), "missing {expected}: {output}");
        }

        // Every job left the store: all known states report 0.
        jobs.clear();
        MasterMetrics::refresh_load_job_counts(&metric, &jobs);
        let output = Metrics::text_output().unwrap();
        assert!(output.contains(&series("load", "loading", 0)), "{output}");
        assert!(output.contains(&series("load", "failed", 0)), "{output}");
        assert!(output.contains(&series("export", "pending", 0)), "{output}");
        assert!(
            output.contains(&series("export", "partial_success", 0)),
            "{output}"
        );
    }

    #[test]
    fn scheduled_bytes_keeps_same_ip_workers_distinct() {
        let metric_name = "test_scheduled_bytes_keeps_same_ip_workers_distinct";
        let metric = m::new_gauge_vec(metric_name, metric_name, &["worker_addr"]).unwrap();
        let workers = [
            live_worker("10.0.0.1", 8995, 128),
            live_worker("10.0.0.1", 8996, 256),
        ];

        MasterMetrics::refresh_scheduled_bytes(&metric, &workers);
        let output = Metrics::text_output().unwrap();
        assert_eq!(
            sample_value(&output, metric_name, &[("worker_addr", "10.0.0.1:8995")]),
            Some(128.0),
            "{output}"
        );
        assert_eq!(
            sample_value(&output, metric_name, &[("worker_addr", "10.0.0.1:8996")]),
            Some(256.0),
            "{output}"
        );
    }
}
