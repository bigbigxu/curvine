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

use curvine_data_transfer::transfer::{SqliteTransferStore, TransferStore};
use curvine_error::ErrorKind;
use curvine_model::{
    TransferJobRecord, TransferKind, TransferListFilter, TransferProgress, TransferState,
};
use std::fs;
use std::path::{Path, PathBuf};

fn unusable_store_path() -> PathBuf {
    let path = std::env::temp_dir().join(format!(
        "curvine-transfer-store-error-{}",
        std::process::id()
    ));
    fs::create_dir_all(&path).unwrap();
    path
}

#[test]
fn sqlite_store_unavailable_error_keeps_backend_detail() {
    // A directory cannot host the database file, so SQLite rejects the open and
    // the reported error must carry that cause.
    let path = unusable_store_path();
    let err = match SqliteTransferStore::open(&path) {
        Ok(_) => panic!("a directory must not be accepted as the sqlite store"),
        Err(err) => err,
    };
    let _ = fs::remove_dir_all(&path);

    assert!(matches!(err.kind(), ErrorKind::TransferStoreUnavailable));
    let message = err.to_string();
    assert!(message.contains("Transfer metadata store is unavailable"));
    assert!(
        message.contains("unable to open database file"),
        "the backend cause must stay in the message: {message}"
    );
}

fn temp_store(tag: &str) -> (SqliteTransferStore, PathBuf) {
    let path = std::env::temp_dir().join(format!(
        "curvine-transfer-sqlite-{}-{tag}.db",
        std::process::id()
    ));
    let _ = fs::remove_file(&path);
    let store = SqliteTransferStore::open(&path).expect("sqlite store");
    (store, path)
}

fn cleanup(path: &Path) {
    let _ = fs::remove_file(path);
}

#[allow(clippy::too_many_arguments)]
fn job(
    job_id: &str,
    created_at: i64,
    updated_at: i64,
    placement_addr: &str,
    owner_addr: &str,
    takeover_count: u32,
) -> TransferJobRecord {
    TransferJobRecord {
        job_key: format!("key-{job_id}"),
        job_id: job_id.to_string(),
        run_id: 1,
        kind: TransferKind::Load,
        source_path: "s3://bucket/src".to_string(),
        target_path: format!("/dst/{job_id}"),
        command_json: "{}".to_string(),
        mount_snapshot_json: "{}".to_string(),
        secret_ref_json: "{}".to_string(),
        cluster_snapshot_version: 1,
        mount_snapshot_attempts: 0,
        planning_attempts: 0,
        takeover_count,
        placement_addr: placement_addr.to_string(),
        owner_addr: owner_addr.to_string(),
        terminal_summary: None,
        terminal_counts: None,
        cv_metadata_epoch: None,
        state: TransferState::Pending,
        owner: String::new(),
        lease_epoch: 0,
        lease_expire_at: 0,
        cancel_requested: false,
        summary: TransferProgress::default(),
        client_request_id: format!("req-{job_id}"),
        submitter: "test".to_string(),
        tenant: "test".to_string(),
        created_at,
        updated_at,
    }
}

fn all_jobs() -> TransferListFilter {
    TransferListFilter {
        kind: None,
        states: Vec::new(),
        submitter: None,
        tenant: None,
        limit: 10,
        offset: 0,
    }
}

#[test]
fn list_orders_by_submission_time() {
    let (store, path) = temp_store("order");
    // `oldest` is updated last: submission time, not last activity, decides.
    for record in [
        job("oldest", 100, 900, "", "", 0),
        job("newest", 300, 300, "", "", 0),
        job("middle", 200, 200, "", "", 0),
    ] {
        store.create_or_get_by_request_id_checked(record).unwrap();
    }

    let listed = store.list_transfers(all_jobs()).unwrap();
    let ids: Vec<&str> = listed.iter().map(|record| record.job_id.as_str()).collect();
    assert_eq!(ids, vec!["newest", "middle", "oldest"]);
    cleanup(&path);
}

#[test]
fn list_rows_match_the_detail_row_on_ownership_fields() {
    // The list query used to omit placement/owner/takeover, so every listed row
    // came back empty while the detail query was correct — and the console reads
    // the list. Assert the invariant instead of the individual values: whatever
    // the detail row says about ownership, the list row must say too.
    let (store, path) = temp_store("ownership");
    store
        .create_or_get_by_request_id_checked(job(
            "owned",
            100,
            100,
            "cv-transfer-0:9010",
            "cv-transfer-1:9010",
            0,
        ))
        .unwrap();

    let listed = store.list_transfers(all_jobs()).unwrap();
    let detail = store.get_transfer("owned").unwrap().expect("stored job");
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].placement_addr, detail.placement_addr);
    assert_eq!(listed[0].owner_addr, detail.owner_addr);
    assert_eq!(listed[0].takeover_count, detail.takeover_count);
    assert_eq!(listed[0].client_request_id, detail.client_request_id);
    assert_eq!(listed[0].placement_addr, "cv-transfer-0:9010");
    assert_eq!(listed[0].owner_addr, "cv-transfer-1:9010");
    cleanup(&path);
}
