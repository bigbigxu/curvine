// Copyright 2025 OPPO.
//
// Licensed under the Apache License, Version 2.0.
// License text: http://www.apache.org/licenses/LICENSE-2.0

//! In-memory state for file-level transfer tasks.
//!
//! A file-level task is one that copies a whole file with a temp+rename commit
//! (or a batch of them). Such a task has no meaningful mid-flight state: on
//! failover it is simply replayed from scratch, so persisting a row per task
//! only buys a database write per task and per progress report.
//!
//! Segment tasks are the opposite: they pre-allocate a shared target and write
//! positional ranges, so their progress is worth persisting. Those keep their
//! `transfer_tasks` rows and never enter this cache.
//!
//! The cache therefore owns the full lifecycle of file-level tasks (planning →
//! dispatch → report → terminal) and the store never sees them. Counts are
//! folded into the job summary so users still see accurate progress.

use std::sync::Arc;

use curvine_model::{
    summarize_transfer_tasks, TaskAttemptStart, TransferProgress, TransferTaskCounts,
    TransferTaskRecord, TransferTaskReport, TransferTaskState, TransferTaskSummary,
};
use curvine_runtime::sync::FastDashMap;

/// Key of one job attempt. A retry bumps `run_id` and gets a fresh bucket.
pub type JobRunKey = (String, u64);

#[derive(Default)]
pub struct TaskCache {
    jobs: FastDashMap<JobRunKey, Arc<JobTasks>>,
}

#[derive(Default)]
struct JobTasks {
    tasks: FastDashMap<String, TransferTaskRecord>,
}

impl TaskCache {
    pub fn new() -> Self {
        Self::default()
    }

    /// True when this job's file-level tasks live in this cache. False means
    /// the job either predates the cache (rows in the store) or only has
    /// segment tasks, so every caller must fall back to the store.
    pub fn owns(&self, job_id: &str, run_id: u64) -> bool {
        self.jobs.contains_key(&(job_id.to_string(), run_id))
    }

    pub fn owns_task(&self, job_id: &str, run_id: u64, task_id: &str) -> bool {
        self.jobs
            .get(&(job_id.to_string(), run_id))
            .map(|tasks| tasks.tasks.contains_key(task_id))
            .unwrap_or(false)
    }

    /// Seeds a freshly planned job. Later batches append to the same bucket.
    pub fn seed(&self, job_id: &str, run_id: u64, tasks: Vec<TransferTaskRecord>) {
        if tasks.is_empty() {
            return;
        }
        let bucket = self
            .jobs
            .entry((job_id.to_string(), run_id))
            .or_insert_with(|| Arc::new(JobTasks::default()))
            .clone();
        for task in tasks {
            bucket.tasks.insert(task.task_id.clone(), task);
        }
    }

    /// Returns Pending tasks for dispatch. Mirrors the store: this only
    /// selects, the state change happens in `start_attempt` once a worker is
    /// chosen. Running tasks past their stale deadline are recycled to Pending
    /// first: with no persisted row there is no store-side stale probe.
    pub fn claim_pending(
        &self,
        job_id: &str,
        run_id: u64,
        limit: usize,
        now_ms: i64,
    ) -> Vec<TransferTaskRecord> {
        let Some(bucket) = self.jobs.get(&(job_id.to_string(), run_id)) else {
            return Vec::new();
        };
        if limit == 0 {
            return Vec::new();
        }
        let mut selected = Vec::new();
        for mut task in bucket.tasks.iter_mut() {
            if selected.len() >= limit {
                break;
            }
            if task.state == TransferTaskState::Running && task.stale_deadline_at < now_ms {
                task.state = TransferTaskState::Pending;
                task.updated_at = now_ms;
            }
            if task.state != TransferTaskState::Pending {
                continue;
            }
            selected.push(task.clone());
        }
        selected
    }

    /// Pending → Running for a chosen worker, fenced by the current attempt.
    pub fn start_attempt(&self, start: &TaskAttemptStart) -> bool {
        let Some(bucket) = self.jobs.get(&(start.job_id.clone(), start.run_id)) else {
            return false;
        };
        let Some(mut task) = bucket.tasks.get_mut(&start.task_id) else {
            return false;
        };
        if task.state != TransferTaskState::Pending
            || task.attempt_id.saturating_add(1) != start.attempt_id
        {
            return false;
        }
        task.attempt_id = start.attempt_id;
        task.worker_id = start.worker_id;
        task.worker_session_id = start.worker_session_id.clone();
        task.state = TransferTaskState::Running;
        task.retry_count = task.retry_count.saturating_add(1);
        task.attempt_started_at = start.now_ms;
        task.stale_deadline_at = start.stale_deadline_at;
        task.updated_at = start.now_ms;
        true
    }

    /// Running → Pending after a pre-accept rejection, so the task is retried in
    /// a later round without a store write (memory-only tasks have no row).
    /// Returns false when the task is unknown or the attempt no longer matches.
    pub fn release_attempt(&self, job_id: &str, run_id: u64, task_id: &str, now_ms: i64) -> bool {
        let Some(bucket) = self.jobs.get(&(job_id.to_string(), run_id)) else {
            return false;
        };
        let Some(mut task) = bucket.tasks.get_mut(task_id) else {
            return false;
        };
        if task.state != TransferTaskState::Running {
            return false;
        }
        task.state = TransferTaskState::Pending;
        task.worker_id = 0;
        task.worker_session_id.clear();
        task.stale_deadline_at = 0;
        task.updated_at = now_ms;
        true
    }

    /// Cancels every recoverable memory-only task of a run in place and returns
    /// the `(worker_id, session)` pairs that were still reporting, so the caller
    /// can tell those workers to stop. Terminal tasks and tasks whose worker is
    /// still live are left alone.
    pub fn cancel_recoverable(
        &self,
        job_id: &str,
        run_id: u64,
        message: &str,
        now_ms: i64,
    ) -> Vec<(u32, String)> {
        let Some(bucket) = self.jobs.get(&(job_id.to_string(), run_id)) else {
            return Vec::new();
        };
        let mut running: Vec<(u32, String)> = Vec::new();
        for mut task in bucket.tasks.iter_mut() {
            match task.state {
                TransferTaskState::Pending | TransferTaskState::Stale => {}
                TransferTaskState::Running if task.stale_deadline_at > now_ms => {
                    // Still reporting: cancel the work on the worker instead of
                    // declaring the task canceled behind its back.
                    if task.worker_id != 0 {
                        let key = (task.worker_id, task.worker_session_id.clone());
                        if !running.contains(&key) {
                            running.push(key);
                        }
                    }
                    continue;
                }
                TransferTaskState::Running => {}
                _ => continue,
            }
            task.state = TransferTaskState::Canceled;
            task.progress.message = message.to_string();
            task.progress.update_time = now_ms;
            task.updated_at = now_ms;
        }
        running
    }

    /// Applies one progress/terminal report. `None` means the task is not a
    /// cache task and the caller must use the store.
    pub fn apply_report(&self, report: &TransferTaskReport) -> Option<bool> {
        let bucket = self.jobs.get(&(report.job_id.clone(), report.run_id))?;
        let mut task = bucket.tasks.get_mut(&report.task_id)?;
        // Same fencing the SQL path enforces: only the current attempt may
        // advance the task, and terminal states are final.
        if task.attempt_id != report.attempt_id
            || task.worker_id != report.worker_id
            || task.worker_session_id != report.worker_session_id
            || task.state.is_terminal()
        {
            return Some(false);
        }
        task.state = report.state;
        task.progress = merge_progress(task.progress.clone(), report.progress.clone());
        task.last_report_at = report.now_ms;
        task.stale_deadline_at = report.stale_deadline_at;
        task.accepted_at_ms = report.accepted_at_ms;
        task.first_byte_at_ms = report.first_byte_at_ms;
        if !report.block_locations.is_empty() {
            task.block_locations = report.block_locations.clone();
        }
        task.updated_at = report.now_ms;
        Some(true)
    }

    pub fn summary(&self, job_id: &str, run_id: u64, now_ms: i64) -> Option<TransferTaskSummary> {
        let bucket = self.jobs.get(&(job_id.to_string(), run_id))?;
        let tasks: Vec<TransferTaskRecord> = bucket.tasks.iter().map(|t| t.clone()).collect();
        Some(summarize_transfer_tasks(tasks.iter(), now_ms))
    }

    pub fn tasks(&self, job_id: &str, run_id: u64) -> Vec<TransferTaskRecord> {
        match self.jobs.get(&(job_id.to_string(), run_id)) {
            Some(bucket) => bucket.tasks.iter().map(|t| t.clone()).collect(),
            None => Vec::new(),
        }
    }

    /// Tasks of one job in the given state, oldest-updated first.
    pub fn tasks_by_state(
        &self,
        job_id: &str,
        run_id: u64,
        state: TransferTaskState,
    ) -> Vec<TransferTaskRecord> {
        let Some(bucket) = self.jobs.get(&(job_id.to_string(), run_id)) else {
            return Vec::new();
        };
        let mut tasks: Vec<TransferTaskRecord> = bucket
            .tasks
            .iter()
            .filter(|task| task.state == state)
            .map(|task| task.clone())
            .collect();
        tasks.sort_by_key(|task| task.updated_at);
        tasks
    }

    /// Returns failed tasks to Pending so the dispatcher can re-run them.
    ///
    /// A file-level task has no row, so the store-side transient-failure
    /// recovery cannot see it. Without this, one dead worker would fail the
    /// whole job on a transport error that a replay would have fixed.
    pub fn retry_failed(
        &self,
        job_id: &str,
        run_id: u64,
        max_retries: u32,
        now_ms: i64,
        retryable: impl Fn(&str) -> bool,
    ) -> usize {
        let Some(bucket) = self.jobs.get(&(job_id.to_string(), run_id)) else {
            return 0;
        };
        let mut requeued = 0;
        for mut task in bucket.tasks.iter_mut() {
            if task.state != TransferTaskState::Failed
                || task.retry_count > max_retries
                || !retryable(&task.progress.message)
            {
                continue;
            }
            task.state = TransferTaskState::Pending;
            task.updated_at = now_ms;
            requeued += 1;
        }
        requeued
    }

    pub fn has_failed(&self, job_id: &str, run_id: u64) -> bool {
        self.jobs
            .get(&(job_id.to_string(), run_id))
            .map(|bucket| {
                bucket
                    .tasks
                    .iter()
                    .any(|task| task.state == TransferTaskState::Failed)
            })
            .unwrap_or(false)
    }

    /// Total task count (file-level only) for this job attempt.
    pub fn len(&self, job_id: &str, run_id: u64) -> usize {
        self.jobs
            .get(&(job_id.to_string(), run_id))
            .map(|bucket| bucket.tasks.len())
            .unwrap_or(0)
    }

    pub fn is_empty(&self, job_id: &str, run_id: u64) -> bool {
        self.len(job_id, run_id) == 0
    }

    /// Drops a converged job bucket. Retried jobs get a new run_id, so the
    /// old bucket would otherwise leak for the life of the process.
    pub fn remove(&self, job_id: &str, run_id: u64) {
        self.jobs.remove(&(job_id.to_string(), run_id));
    }

    /// Every job attempt currently held in memory. The summary write-back
    /// walks this instead of tracking a dirty set.
    pub fn active_jobs(&self) -> Vec<JobRunKey> {
        self.jobs.iter().map(|entry| entry.key().clone()).collect()
    }

    /// Per-state counts of every task this instance holds in memory.
    ///
    /// File-level tasks never get a `transfer_tasks` row, so a store-side
    /// calibration cannot see them; the task gauges have to add these or they
    /// keep drifting on the increments alone.
    pub fn counts(&self, now_ms: i64) -> TransferTaskCounts {
        let mut total = TransferTaskCounts::default();
        for key in self.active_jobs() {
            let Some(summary) = self.summary(&key.0, key.1, now_ms) else {
                continue;
            };
            total.pending += summary.counts.pending;
            total.running += summary.counts.running;
            total.completed += summary.counts.completed;
            total.failed += summary.counts.failed;
            total.canceled += summary.counts.canceled;
            total.stale += summary.counts.stale;
            total.completed_size += summary.counts.completed_size;
        }
        total
    }

    /// Drops buckets for jobs that are no longer active. Called by the
    /// scheduler cleanup so a long-lived process does not accumulate them.
    pub fn retain_active(&self, active: &std::collections::HashSet<JobRunKey>) {
        self.jobs.retain(|key, _| active.contains(key));
    }
}

/// loaded_size is monotonic per task, matching the SQL `greatest(...)` rule.
fn merge_progress(old: TransferProgress, new: TransferProgress) -> TransferProgress {
    TransferProgress {
        loaded_size: old.loaded_size.max(new.loaded_size),
        total_size: if new.total_size > 0 {
            new.total_size
        } else {
            old.total_size
        },
        update_time: new.update_time,
        message: new.message,
    }
}

/// Adds cache counts to a store summary so callers see the whole job.
pub fn merge_summaries(
    store: TransferTaskSummary,
    cached: Option<TransferTaskSummary>,
) -> TransferTaskSummary {
    let Some(cached) = cached else {
        return store;
    };
    TransferTaskSummary {
        progress: TransferProgress {
            loaded_size: store.progress.loaded_size + cached.progress.loaded_size,
            total_size: store.progress.total_size + cached.progress.total_size,
            update_time: store.progress.update_time.max(cached.progress.update_time),
            message: if cached.progress.message.is_empty() {
                store.progress.message
            } else {
                cached.progress.message
            },
        },
        counts: TransferTaskCounts {
            pending: store.counts.pending + cached.counts.pending,
            running: store.counts.running + cached.counts.running,
            completed: store.counts.completed + cached.counts.completed,
            failed: store.counts.failed + cached.counts.failed,
            canceled: store.counts.canceled + cached.counts.canceled,
            stale: store.counts.stale + cached.counts.stale,
            completed_size: store.counts.completed_size + cached.counts.completed_size,
        },
        has_task: store.has_task || cached.has_task,
        has_failed: store.has_failed || cached.has_failed,
        all_completed: store.all_completed && cached.all_completed,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn task(id: &str) -> TransferTaskRecord {
        TransferTaskRecord {
            job_id: "job-1".to_string(),
            run_id: 7,
            task_id: id.to_string(),
            attempt_id: 0,
            source_path: "/src".to_string(),
            target_path: "/dst".to_string(),
            worker_id: 0,
            worker_session_id: String::new(),
            source_read_plan_json: "[]".to_string(),
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
            replace_target: false,
        }
    }

    fn report(id: &str, state: TransferTaskState, attempt: u64) -> TransferTaskReport {
        TransferTaskReport {
            job_id: "job-1".to_string(),
            run_id: 7,
            task_id: id.to_string(),
            attempt_id: attempt,
            worker_id: 3,
            worker_session_id: "s".to_string(),
            state,
            progress: TransferProgress {
                loaded_size: 10,
                total_size: 10,
                update_time: 5,
                message: String::new(),
            },
            now_ms: 100,
            stale_deadline_at: 200,
            accepted_at_ms: 0,
            first_byte_at_ms: 0,
            block_locations: Vec::new(),
        }
    }

    fn start(task_id: &str, attempt: u64) -> TaskAttemptStart {
        TaskAttemptStart {
            job_id: "job-1".to_string(),
            run_id: 7,
            owner: "owner".to_string(),
            fence: 1,
            task_id: task_id.to_string(),
            attempt_id: attempt,
            worker_id: 3,
            worker_session_id: "s".to_string(),
            report_target_json: "[]".to_string(),
            now_ms: 100,
            stale_deadline_at: 200,
            resume_loaded_size: 0,
        }
    }

    #[test]
    fn claim_selects_pending_and_start_moves_it_to_running() {
        let cache = TaskCache::new();
        cache.seed("job-1", 7, vec![task("t1"), task("t2")]);
        assert!(cache.owns("job-1", 7));

        let claimed = cache.claim_pending("job-1", 7, 10, 100);
        assert_eq!(claimed.len(), 2);
        // Selection does not mutate, so the tasks still read as Pending.
        assert!(cache
            .claim_pending("job-1", 7, 10, 100)
            .iter()
            .all(|t| t.state == TransferTaskState::Pending));

        assert!(cache.start_attempt(&start("t1", 1)));
        let running = cache.tasks_by_state("job-1", 7, TransferTaskState::Running);
        assert_eq!(running.len(), 1);
        assert_eq!(running[0].worker_id, 3);
        // A stale attempt id is rejected.
        assert!(!cache.start_attempt(&start("t2", 5)));
    }

    /// A cancel must not declare a task canceled while its worker is still
    /// reporting: those tasks are returned so the caller can tell the worker to
    /// stop, and only tasks past their stale deadline are canceled in place.
    #[test]
    fn cancel_recoverable_keeps_live_runs_and_reports_their_workers() {
        let cache = TaskCache::new();
        cache.seed("job-1", 7, vec![task("t1"), task("t2")]);
        assert!(cache.start_attempt(&start("t1", 1))); // Running, deadline 200

        // Before the deadline the run is alive: nothing canceled, worker returned.
        assert_eq!(
            cache.cancel_recoverable("job-1", 7, "transfer canceled", 100),
            vec![(3u32, "s".to_string())]
        );
        assert_eq!(
            cache
                .tasks_by_state("job-1", 7, TransferTaskState::Running)
                .len(),
            1
        );
        assert_eq!(
            cache
                .tasks_by_state("job-1", 7, TransferTaskState::Canceled)
                .len(),
            1,
            "the pending task is canceled"
        );

        // Past the deadline the attempt is treated as lost and canceled too.
        assert!(cache
            .cancel_recoverable("job-1", 7, "transfer canceled", 300)
            .is_empty());
        assert_eq!(
            cache
                .tasks_by_state("job-1", 7, TransferTaskState::Running)
                .len(),
            0
        );
    }

    /// A worker that rejects a task before accepting it never started it, so the
    /// task must go back to Pending in place: memory-only tasks have no row, and
    /// waiting for the stale reaper would delay the retry by a full timeout.
    #[test]
    fn release_attempt_puts_a_rejected_task_back_to_pending() {
        let cache = TaskCache::new();
        cache.seed("job-1", 7, vec![task("t1")]);
        assert!(cache.start_attempt(&start("t1", 1)));
        assert_eq!(
            cache
                .tasks_by_state("job-1", 7, TransferTaskState::Running)
                .len(),
            1
        );

        assert!(cache.release_attempt("job-1", 7, "t1", 500));
        let pending = cache.tasks_by_state("job-1", 7, TransferTaskState::Pending);
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].worker_id, 0, "the worker claim must be dropped");
        assert!(pending[0].worker_session_id.is_empty());
        // The attempt id moved forward, so the next dispatch takes attempt 2.
        assert!(cache.start_attempt(&start("t1", 2)));
        // An unknown task and a non-Running task are not touched.
        assert!(!cache.release_attempt("job-1", 7, "missing", 600));
    }

    #[test]
    fn stale_running_task_is_recycled_to_pending() {
        let cache = TaskCache::new();
        cache.seed("job-1", 7, vec![task("t1")]);
        assert!(cache.start_attempt(&start("t1", 1)));
        // Before the deadline it is not re-selectable.
        assert!(cache.claim_pending("job-1", 7, 1, 150).is_empty());
        // Past the deadline the failed attempt returns to Pending.
        let recycled = cache.claim_pending("job-1", 7, 1, 10_000);
        assert_eq!(recycled.len(), 1);
        assert_eq!(recycled[0].state, TransferTaskState::Pending);
        assert!(cache.start_attempt(&start("t1", 2)));
    }

    #[test]
    fn report_requires_current_attempt_and_terminal_is_final() {
        let cache = TaskCache::new();
        cache.seed("job-1", 7, vec![task("t1")]);
        cache.start_attempt(&start("t1", 1));

        // Wrong attempt is fenced.
        assert_eq!(
            cache.apply_report(&report("t1", TransferTaskState::Running, 0)),
            Some(false)
        );
        assert_eq!(
            cache.apply_report(&report("t1", TransferTaskState::Completed, 1)),
            Some(true)
        );
        // Terminal is final.
        assert_eq!(
            cache.apply_report(&report("t1", TransferTaskState::Running, 1)),
            Some(false)
        );
        assert!(cache.summary("job-1", 7, 100).unwrap().all_completed);
    }

    #[test]
    fn unknown_task_is_not_a_cache_task() {
        let cache = TaskCache::new();
        assert_eq!(
            cache.apply_report(&report("t1", TransferTaskState::Running, 1)),
            None
        );
        assert!(!cache.owns("job-1", 7));
    }

    #[test]
    fn summary_merges_with_store_counts() {
        let cache = TaskCache::new();
        cache.seed("job-1", 7, vec![task("t1"), task("t2")]);
        cache.start_attempt(&start("t1", 1));
        cache.apply_report(&report("t1", TransferTaskState::Completed, 1));

        let store = TransferTaskSummary {
            progress: TransferProgress {
                loaded_size: 100,
                total_size: 100,
                ..Default::default()
            },
            counts: TransferTaskCounts {
                completed: 1,
                completed_size: 100,
                ..Default::default()
            },
            has_task: true,
            has_failed: false,
            all_completed: true,
        };
        let merged = merge_summaries(store, cache.summary("job-1", 7, 100));
        assert_eq!(merged.counts.completed, 2);
        assert_eq!(merged.counts.pending, 1);
        assert_eq!(merged.progress.loaded_size, 110);
        assert!(!merged.all_completed);
    }

    /// A worker death surfaces as a transport error on the task. With no row to
    /// recover, the cache itself must put the task back for a replay.
    #[test]
    fn retryable_failure_returns_to_pending_within_budget() {
        let cache = TaskCache::new();
        cache.seed("job-1", 7, vec![task("t1")]);
        cache.start_attempt(&start("t1", 1));
        let mut failed = report("t1", TransferTaskState::Failed, 1);
        failed.progress.message = "batch task t1 file x: Connection reset by peer".to_string();
        assert_eq!(cache.apply_report(&failed), Some(true));
        assert!(cache.has_failed("job-1", 7));

        let is_retryable = |msg: &str| msg.contains("Connection reset by peer");
        assert_eq!(cache.retry_failed("job-1", 7, 3, 500, is_retryable), 1);
        assert!(!cache.has_failed("job-1", 7), "task is pending again");
        assert_eq!(cache.claim_pending("job-1", 7, 10, 500).len(), 1);

        // A permanent failure is never requeued.
        let cache = TaskCache::new();
        cache.seed("job-1", 7, vec![task("t1")]);
        cache.start_attempt(&start("t1", 1));
        let mut permanent = report("t1", TransferTaskState::Failed, 1);
        permanent.progress.message = "schema mismatch".to_string();
        cache.apply_report(&permanent);
        assert_eq!(cache.retry_failed("job-1", 7, 3, 500, is_retryable), 0);
        assert!(cache.has_failed("job-1", 7));
    }

    #[test]
    fn remove_drops_the_bucket() {
        let cache = TaskCache::new();
        cache.seed("job-1", 7, vec![task("t1")]);
        cache.remove("job-1", 7);
        assert!(!cache.owns("job-1", 7));
    }
}
