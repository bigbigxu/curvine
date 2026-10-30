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

use super::dispatch::terminal_snapshot_progress;
use super::service::resolve_terminal_summary;
use curvine_model::{
    TransferJobRecord, TransferKind, TransferProgress, TransferState, TransferTaskCounts,
    TransferTaskSummary,
};

fn failed_job(
    summary: TransferProgress,
    terminal_summary: Option<TransferProgress>,
    terminal_counts: Option<TransferTaskCounts>,
) -> TransferJobRecord {
    TransferJobRecord {
        job_key: "Load:oss://bucket/source:/target".to_string(),
        job_id: "job-1".to_string(),
        run_id: 1,
        kind: TransferKind::Load,
        source_path: "oss://bucket/source".to_string(),
        target_path: "/target".to_string(),
        command_json: "{}".to_string(),
        mount_snapshot_json: "{}".to_string(),
        secret_ref_json: "{}".to_string(),
        cluster_snapshot_version: 0,
        mount_snapshot_attempts: 0,
        cv_metadata_epoch: None,
        state: TransferState::Failed,
        owner: "owner".to_string(),
        lease_epoch: 1,
        lease_expire_at: 0,
        cancel_requested: false,
        summary,
        client_request_id: "request-1".to_string(),
        submitter: "test".to_string(),
        tenant: String::new(),
        created_at: 0,
        updated_at: 0,
        planning_attempts: 0,
        takeover_count: 0,
        terminal_summary,
        terminal_counts,
    }
}

fn empty_live_summary() -> TransferTaskSummary {
    TransferTaskSummary {
        progress: TransferProgress::default(),
        counts: TransferTaskCounts::default(),
        has_task: false,
        has_failed: false,
        all_completed: false,
    }
}

#[test]
fn terminal_snapshot_keeps_planning_failure_message() {
    let message = "Load failed during planning: source not found";
    let progress = terminal_snapshot_progress(TransferProgress::default(), message);
    assert_eq!(progress.message, message);
}

#[test]
fn status_uses_persisted_job_message_when_snapshot_is_empty() {
    let message = "Load failed during planning: source not found";
    let job = failed_job(
        TransferProgress {
            message: message.to_string(),
            ..TransferProgress::default()
        },
        Some(TransferProgress::default()),
        Some(TransferTaskCounts::default()),
    );

    let (progress, counts, expired) = resolve_terminal_summary(&job, &empty_live_summary());

    assert_eq!(progress.message, message);
    assert_eq!(counts, TransferTaskCounts::default());
    assert!(!expired);
}

#[test]
fn status_keeps_job_message_when_terminal_snapshot_is_missing() {
    let message = "Load failed during planning: source not found";
    let job = failed_job(
        TransferProgress {
            message: message.to_string(),
            ..TransferProgress::default()
        },
        None,
        None,
    );

    let (progress, counts, expired) = resolve_terminal_summary(&job, &empty_live_summary());

    assert_eq!(progress.message, message);
    assert_eq!(counts, TransferTaskCounts::default());
    assert!(expired);
}

/// A re-plan that finds every target already canonical (failover, repeated
/// load) must report the files it skipped, not a zero-progress terminal state
/// that reads as "copied nothing".
#[test]
fn incremental_noop_reports_skipped_files_instead_of_zero() {
    use super::store::TransferStore;

    let store = super::MemoryTransferStore::new();
    let mut job = failed_job(TransferProgress::default(), None, None);
    job.state = TransferState::Planning;
    store.create_or_get_by_request_id(job.clone()).unwrap();

    // What the empty-plan branch writes before landing the terminal state.
    let skipped_files = 150u64;
    let skipped_size = 157_286_400i64;
    let snapshot = TransferProgress {
        loaded_size: skipped_size,
        total_size: skipped_size,
        update_time: 10,
        message: "incremental transfer complete".to_string(),
    };
    let counts = TransferTaskCounts {
        completed: skipped_files,
        completed_size: skipped_size,
        ..Default::default()
    };
    store
        .update_terminal_snapshot(&job.job_id, job.run_id, snapshot.clone(), counts, 10)
        .unwrap();

    // The generic terminal snapshot runs afterwards; it must not zero this.
    store
        .update_terminal_snapshot(
            &job.job_id,
            job.run_id,
            TransferProgress::default(),
            TransferTaskCounts::default(),
            11,
        )
        .unwrap();

    let stored = store.get_transfer(&job.job_id).unwrap().unwrap();
    let progress = stored.terminal_summary.expect("terminal snapshot written");
    let counts = stored.terminal_counts.expect("terminal counts written");
    assert_eq!(progress.loaded_size, skipped_size);
    assert_eq!(progress.total_size, skipped_size);
    assert_eq!(counts.completed, skipped_files);
    assert_eq!(counts.completed_size, skipped_size);
    // The job row (read by the list view) must agree with the detail view.
    assert_eq!(stored.summary.loaded_size, skipped_size);
}
