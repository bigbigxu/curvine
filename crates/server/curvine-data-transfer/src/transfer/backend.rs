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

use curvine_error::FsError;
use curvine_error::FsResult;
use curvine_model::{
    StaleTaskAttempt, TaskAttemptStart, TransferCountFilter, TransferJobRecord, TransferLease,
    TransferListFilter, TransferProgress, TransferStateCounts, TransferStateUpdate,
    TransferTaskCounts, TransferTaskRecord, TransferTaskReport, TransferTaskState,
    TransferTaskStatus, TransferTaskSummary, TransferTenantSummary,
};
use std::time::Instant;

#[cfg(feature = "transfer-store-mysql")]
use crate::transfer::MysqlTransferStore;
#[cfg(feature = "transfer-store-postgres")]
use crate::transfer::PostgresTransferStore;
#[cfg(feature = "transfer-store-sqlite")]
use crate::transfer::SqliteTransferStore;
use crate::transfer::{
    MemoryTransferStore, TransferMetrics, TransferMountSnapshotUpdate, TransferPlannedTasks,
    TransferRequeueUpdate, TransferStore, TransferTaskStateUpdate,
};

pub enum TransferStoreBackend {
    Memory(MemoryTransferStore),
    #[cfg(feature = "transfer-store-sqlite")]
    Sqlite(SqliteTransferStore),
    #[cfg(feature = "transfer-store-mysql")]
    Mysql(MysqlTransferStore),
    #[cfg(feature = "transfer-store-postgres")]
    Postgres(PostgresTransferStore),
}

impl TransferStore for TransferStoreBackend {
    fn check_available(&self) -> FsResult<()> {
        self.record_store_operation("check_available", || match self {
            Self::Memory(store) => store.check_available(),
            #[cfg(feature = "transfer-store-sqlite")]
            Self::Sqlite(store) => store.check_available(),
            #[cfg(feature = "transfer-store-mysql")]
            Self::Mysql(store) => store.check_available(),
            #[cfg(feature = "transfer-store-postgres")]
            Self::Postgres(store) => store.check_available(),
        })
    }

    fn create_or_get_by_request_id(&self, job: TransferJobRecord) -> FsResult<TransferJobRecord> {
        self.record_store_operation("create_or_get_by_request_id", || match self {
            Self::Memory(store) => store.create_or_get_by_request_id(job),
            #[cfg(feature = "transfer-store-sqlite")]
            Self::Sqlite(store) => store.create_or_get_by_request_id(job),
            #[cfg(feature = "transfer-store-mysql")]
            Self::Mysql(store) => store.create_or_get_by_request_id(job),
            #[cfg(feature = "transfer-store-postgres")]
            Self::Postgres(store) => store.create_or_get_by_request_id(job),
        })
    }

    fn create_or_get_by_request_id_checked(
        &self,
        job: TransferJobRecord,
    ) -> FsResult<TransferJobRecord> {
        self.record_store_operation("create_or_get_by_request_id_checked", || match self {
            Self::Memory(store) => store.create_or_get_by_request_id_checked(job),
            #[cfg(feature = "transfer-store-sqlite")]
            Self::Sqlite(store) => store.create_or_get_by_request_id_checked(job),
            #[cfg(feature = "transfer-store-mysql")]
            Self::Mysql(store) => store.create_or_get_by_request_id_checked(job),
            #[cfg(feature = "transfer-store-postgres")]
            Self::Postgres(store) => store.create_or_get_by_request_id_checked(job),
        })
    }

    fn get_transfer(&self, job_id: &str) -> FsResult<Option<TransferJobRecord>> {
        self.record_store_operation("get_transfer", || match self {
            Self::Memory(store) => store.get_transfer(job_id),
            #[cfg(feature = "transfer-store-sqlite")]
            Self::Sqlite(store) => store.get_transfer(job_id),
            #[cfg(feature = "transfer-store-mysql")]
            Self::Mysql(store) => store.get_transfer(job_id),
            #[cfg(feature = "transfer-store-postgres")]
            Self::Postgres(store) => store.get_transfer(job_id),
        })
    }

    fn get_transfer_by_request(
        &self,
        submitter: &str,
        client_request_id: &str,
    ) -> FsResult<Option<TransferJobRecord>> {
        self.record_store_operation("get_transfer_by_request", || match self {
            Self::Memory(store) => store.get_transfer_by_request(submitter, client_request_id),
            #[cfg(feature = "transfer-store-sqlite")]
            Self::Sqlite(store) => store.get_transfer_by_request(submitter, client_request_id),
            #[cfg(feature = "transfer-store-mysql")]
            Self::Mysql(store) => store.get_transfer_by_request(submitter, client_request_id),
            #[cfg(feature = "transfer-store-postgres")]
            Self::Postgres(store) => store.get_transfer_by_request(submitter, client_request_id),
        })
    }

    fn list_active_transfers(&self) -> FsResult<Vec<TransferJobRecord>> {
        self.record_store_operation("list_active_transfers", || match self {
            Self::Memory(store) => store.list_active_transfers(),
            #[cfg(feature = "transfer-store-sqlite")]
            Self::Sqlite(store) => store.list_active_transfers(),
            #[cfg(feature = "transfer-store-mysql")]
            Self::Mysql(store) => store.list_active_transfers(),
            #[cfg(feature = "transfer-store-postgres")]
            Self::Postgres(store) => store.list_active_transfers(),
        })
    }

    fn has_active_transfer_by_key(&self, job_key: &str) -> FsResult<bool> {
        self.record_store_operation("has_active_transfer_by_key", || match self {
            Self::Memory(store) => store.has_active_transfer_by_key(job_key),
            #[cfg(feature = "transfer-store-sqlite")]
            Self::Sqlite(store) => store.has_active_transfer_by_key(job_key),
            #[cfg(feature = "transfer-store-mysql")]
            Self::Mysql(store) => store.has_active_transfer_by_key(job_key),
            #[cfg(feature = "transfer-store-postgres")]
            Self::Postgres(store) => store.has_active_transfer_by_key(job_key),
        })
    }

    fn find_conflicting_active_transfer(
        &self,
        target_path: &str,
        submitter: &str,
        client_request_id: &str,
    ) -> FsResult<Option<TransferJobRecord>> {
        self.record_store_operation("find_conflicting_active_transfer", || match self {
            Self::Memory(store) => {
                store.find_conflicting_active_transfer(target_path, submitter, client_request_id)
            }
            #[cfg(feature = "transfer-store-sqlite")]
            Self::Sqlite(store) => {
                store.find_conflicting_active_transfer(target_path, submitter, client_request_id)
            }
            #[cfg(feature = "transfer-store-mysql")]
            Self::Mysql(store) => {
                store.find_conflicting_active_transfer(target_path, submitter, client_request_id)
            }
            #[cfg(feature = "transfer-store-postgres")]
            Self::Postgres(store) => {
                store.find_conflicting_active_transfer(target_path, submitter, client_request_id)
            }
        })
    }

    fn count_active_transfers(&self) -> FsResult<u64> {
        self.record_store_operation("count_active_transfers", || match self {
            Self::Memory(store) => store.count_active_transfers(),
            #[cfg(feature = "transfer-store-sqlite")]
            Self::Sqlite(store) => store.count_active_transfers(),
            #[cfg(feature = "transfer-store-mysql")]
            Self::Mysql(store) => store.count_active_transfers(),
            #[cfg(feature = "transfer-store-postgres")]
            Self::Postgres(store) => store.count_active_transfers(),
        })
    }

    fn count_executing_transfers(&self) -> FsResult<u64> {
        self.record_store_operation("count_executing_transfers", || match self {
            Self::Memory(store) => store.count_executing_transfers(),
            #[cfg(feature = "transfer-store-sqlite")]
            Self::Sqlite(store) => store.count_executing_transfers(),
            #[cfg(feature = "transfer-store-mysql")]
            Self::Mysql(store) => store.count_executing_transfers(),
            #[cfg(feature = "transfer-store-postgres")]
            Self::Postgres(store) => store.count_executing_transfers(),
        })
    }

    fn list_transfers(&self, filter: TransferListFilter) -> FsResult<Vec<TransferJobRecord>> {
        self.record_store_operation("list_transfers", || match self {
            Self::Memory(store) => store.list_transfers(filter),
            #[cfg(feature = "transfer-store-sqlite")]
            Self::Sqlite(store) => store.list_transfers(filter),
            #[cfg(feature = "transfer-store-mysql")]
            Self::Mysql(store) => store.list_transfers(filter),
            #[cfg(feature = "transfer-store-postgres")]
            Self::Postgres(store) => store.list_transfers(filter),
        })
    }

    fn count_transfers_by_state(
        &self,
        filter: &TransferCountFilter,
    ) -> FsResult<TransferStateCounts> {
        self.record_store_operation("count_transfers_by_state", || match self {
            Self::Memory(store) => store.count_transfers_by_state(filter),
            #[cfg(feature = "transfer-store-sqlite")]
            Self::Sqlite(store) => store.count_transfers_by_state(filter),
            #[cfg(feature = "transfer-store-mysql")]
            Self::Mysql(store) => store.count_transfers_by_state(filter),
            #[cfg(feature = "transfer-store-postgres")]
            Self::Postgres(store) => store.count_transfers_by_state(filter),
        })
    }

    fn list_tenant_summaries(
        &self,
        limit: usize,
        offset: usize,
    ) -> FsResult<Vec<TransferTenantSummary>> {
        self.record_store_operation("list_tenant_summaries", || match self {
            Self::Memory(store) => store.list_tenant_summaries(limit, offset),
            #[cfg(feature = "transfer-store-sqlite")]
            Self::Sqlite(store) => store.list_tenant_summaries(limit, offset),
            #[cfg(feature = "transfer-store-mysql")]
            Self::Mysql(store) => store.list_tenant_summaries(limit, offset),
            #[cfg(feature = "transfer-store-postgres")]
            Self::Postgres(store) => store.list_tenant_summaries(limit, offset),
        })
    }

    fn purge_terminal_transfers(&self, older_than_ms: i64, limit: usize) -> FsResult<usize> {
        self.record_store_operation("purge_terminal_transfers", || match self {
            Self::Memory(store) => store.purge_terminal_transfers(older_than_ms, limit),
            #[cfg(feature = "transfer-store-sqlite")]
            Self::Sqlite(store) => store.purge_terminal_transfers(older_than_ms, limit),
            #[cfg(feature = "transfer-store-mysql")]
            Self::Mysql(store) => store.purge_terminal_transfers(older_than_ms, limit),
            #[cfg(feature = "transfer-store-postgres")]
            Self::Postgres(store) => store.purge_terminal_transfers(older_than_ms, limit),
        })
    }

    fn purge_terminal_tasks(&self, older_than_ms: i64, task_limit: usize) -> FsResult<usize> {
        self.record_store_operation("purge_terminal_tasks", || match self {
            Self::Memory(store) => store.purge_terminal_tasks(older_than_ms, task_limit),
            #[cfg(feature = "transfer-store-sqlite")]
            Self::Sqlite(store) => store.purge_terminal_tasks(older_than_ms, task_limit),
            #[cfg(feature = "transfer-store-mysql")]
            Self::Mysql(store) => store.purge_terminal_tasks(older_than_ms, task_limit),
            #[cfg(feature = "transfer-store-postgres")]
            Self::Postgres(store) => store.purge_terminal_tasks(older_than_ms, task_limit),
        })
    }

    fn purge_terminal_tasks_with_cleanup_lease(
        &self,
        older_than_ms: i64,
        task_limit: usize,
        owner: &str,
        now_ms: i64,
    ) -> FsResult<usize> {
        self.record_store_operation("purge_terminal_tasks_with_cleanup_lease", || match self {
            Self::Memory(store) => store.purge_terminal_tasks_with_cleanup_lease(
                older_than_ms,
                task_limit,
                owner,
                now_ms,
            ),
            #[cfg(feature = "transfer-store-sqlite")]
            Self::Sqlite(store) => store.purge_terminal_tasks_with_cleanup_lease(
                older_than_ms,
                task_limit,
                owner,
                now_ms,
            ),
            #[cfg(feature = "transfer-store-mysql")]
            Self::Mysql(store) => store.purge_terminal_tasks_with_cleanup_lease(
                older_than_ms,
                task_limit,
                owner,
                now_ms,
            ),
            #[cfg(feature = "transfer-store-postgres")]
            Self::Postgres(store) => store.purge_terminal_tasks_with_cleanup_lease(
                older_than_ms,
                task_limit,
                owner,
                now_ms,
            ),
        })
    }

    fn purge_terminal_jobs(&self, older_than_ms: i64, limit: usize) -> FsResult<usize> {
        self.record_store_operation("purge_terminal_jobs", || match self {
            Self::Memory(store) => store.purge_terminal_jobs(older_than_ms, limit),
            #[cfg(feature = "transfer-store-sqlite")]
            Self::Sqlite(store) => store.purge_terminal_jobs(older_than_ms, limit),
            #[cfg(feature = "transfer-store-mysql")]
            Self::Mysql(store) => store.purge_terminal_jobs(older_than_ms, limit),
            #[cfg(feature = "transfer-store-postgres")]
            Self::Postgres(store) => store.purge_terminal_jobs(older_than_ms, limit),
        })
    }

    fn purge_terminal_jobs_with_cleanup_lease(
        &self,
        older_than_ms: i64,
        limit: usize,
        owner: &str,
        now_ms: i64,
    ) -> FsResult<usize> {
        self.record_store_operation("purge_terminal_jobs_with_cleanup_lease", || match self {
            Self::Memory(store) => {
                store.purge_terminal_jobs_with_cleanup_lease(older_than_ms, limit, owner, now_ms)
            }
            #[cfg(feature = "transfer-store-sqlite")]
            Self::Sqlite(store) => {
                store.purge_terminal_jobs_with_cleanup_lease(older_than_ms, limit, owner, now_ms)
            }
            #[cfg(feature = "transfer-store-mysql")]
            Self::Mysql(store) => {
                store.purge_terminal_jobs_with_cleanup_lease(older_than_ms, limit, owner, now_ms)
            }
            #[cfg(feature = "transfer-store-postgres")]
            Self::Postgres(store) => {
                store.purge_terminal_jobs_with_cleanup_lease(older_than_ms, limit, owner, now_ms)
            }
        })
    }

    fn try_acquire_cleanup_lease(&self, owner: &str, now_ms: i64, lease_ms: i64) -> FsResult<bool> {
        self.record_store_operation("try_acquire_cleanup_lease", || match self {
            Self::Memory(store) => store.try_acquire_cleanup_lease(owner, now_ms, lease_ms),
            #[cfg(feature = "transfer-store-sqlite")]
            Self::Sqlite(store) => store.try_acquire_cleanup_lease(owner, now_ms, lease_ms),
            #[cfg(feature = "transfer-store-mysql")]
            Self::Mysql(store) => store.try_acquire_cleanup_lease(owner, now_ms, lease_ms),
            #[cfg(feature = "transfer-store-postgres")]
            Self::Postgres(store) => store.try_acquire_cleanup_lease(owner, now_ms, lease_ms),
        })
    }

    fn list_transfer_tasks(&self, job_id: &str, run_id: u64) -> FsResult<Vec<TransferTaskRecord>> {
        self.record_store_operation("list_transfer_tasks", || match self {
            Self::Memory(store) => store.list_transfer_tasks(job_id, run_id),
            #[cfg(feature = "transfer-store-sqlite")]
            Self::Sqlite(store) => store.list_transfer_tasks(job_id, run_id),
            #[cfg(feature = "transfer-store-mysql")]
            Self::Mysql(store) => store.list_transfer_tasks(job_id, run_id),
            #[cfg(feature = "transfer-store-postgres")]
            Self::Postgres(store) => store.list_transfer_tasks(job_id, run_id),
        })
    }

    fn get_task_attempt_started_at(
        &self,
        job_id: &str,
        run_id: u64,
        task_id: &str,
    ) -> FsResult<Option<i64>> {
        self.record_store_operation("get_task_attempt_started_at", || match self {
            Self::Memory(store) => store.get_task_attempt_started_at(job_id, run_id, task_id),
            #[cfg(feature = "transfer-store-sqlite")]
            Self::Sqlite(store) => store.get_task_attempt_started_at(job_id, run_id, task_id),
            #[cfg(feature = "transfer-store-mysql")]
            Self::Mysql(store) => store.get_task_attempt_started_at(job_id, run_id, task_id),
            #[cfg(feature = "transfer-store-postgres")]
            Self::Postgres(store) => store.get_task_attempt_started_at(job_id, run_id, task_id),
        })
    }

    fn get_transfer_task_summary(
        &self,
        job_id: &str,
        run_id: u64,
        now_ms: i64,
    ) -> FsResult<TransferTaskSummary> {
        self.record_store_operation("get_transfer_task_summary", || match self {
            Self::Memory(store) => store.get_transfer_task_summary(job_id, run_id, now_ms),
            #[cfg(feature = "transfer-store-sqlite")]
            Self::Sqlite(store) => store.get_transfer_task_summary(job_id, run_id, now_ms),
            #[cfg(feature = "transfer-store-mysql")]
            Self::Mysql(store) => store.get_transfer_task_summary(job_id, run_id, now_ms),
            #[cfg(feature = "transfer-store-postgres")]
            Self::Postgres(store) => store.get_transfer_task_summary(job_id, run_id, now_ms),
        })
    }

    fn list_transfer_task_status_page(
        &self,
        job_id: &str,
        run_id: u64,
        offset: usize,
        limit: usize,
    ) -> FsResult<(Vec<TransferTaskStatus>, bool)> {
        self.record_store_operation("list_transfer_task_status_page", || match self {
            Self::Memory(store) => {
                store.list_transfer_task_status_page(job_id, run_id, offset, limit)
            }
            #[cfg(feature = "transfer-store-sqlite")]
            Self::Sqlite(store) => {
                store.list_transfer_task_status_page(job_id, run_id, offset, limit)
            }
            #[cfg(feature = "transfer-store-mysql")]
            Self::Mysql(store) => {
                store.list_transfer_task_status_page(job_id, run_id, offset, limit)
            }
            #[cfg(feature = "transfer-store-postgres")]
            Self::Postgres(store) => {
                store.list_transfer_task_status_page(job_id, run_id, offset, limit)
            }
        })
    }

    fn request_cancel(&self, job_id: &str, run_id: u64, now_ms: i64) -> FsResult<bool> {
        self.record_store_operation("request_cancel", || match self {
            Self::Memory(store) => store.request_cancel(job_id, run_id, now_ms),
            #[cfg(feature = "transfer-store-sqlite")]
            Self::Sqlite(store) => store.request_cancel(job_id, run_id, now_ms),
            #[cfg(feature = "transfer-store-mysql")]
            Self::Mysql(store) => store.request_cancel(job_id, run_id, now_ms),
            #[cfg(feature = "transfer-store-postgres")]
            Self::Postgres(store) => store.request_cancel(job_id, run_id, now_ms),
        })
    }

    fn list_active_owned_jobs(&self, limit: usize) -> FsResult<Vec<(String, u64, String)>> {
        self.record_store_operation("list_active_owned_jobs", || match self {
            Self::Memory(store) => store.list_active_owned_jobs(limit),
            Self::Sqlite(store) => store.list_active_owned_jobs(limit),
            Self::Mysql(store) => store.list_active_owned_jobs(limit),
            Self::Postgres(store) => store.list_active_owned_jobs(limit),
        })
    }

    fn list_expired_jobs(&self, now_ms: i64, limit: usize) -> FsResult<Vec<(String, u64)>> {
        self.record_store_operation("list_expired_jobs", || match self {
            Self::Memory(store) => store.list_expired_jobs(now_ms, limit),
            Self::Sqlite(store) => store.list_expired_jobs(now_ms, limit),
            Self::Mysql(store) => store.list_expired_jobs(now_ms, limit),
            Self::Postgres(store) => store.list_expired_jobs(now_ms, limit),
        })
    }

    fn claim_next_job(
        &self,
        owner: &str,
        now_ms: i64,
        max_executing_transfers: u64,
        prefer_pending: bool,
        lease_ms: i64,
        after_job_id: &str,
    ) -> FsResult<Option<TransferLease>> {
        self.record_store_operation("claim_next_job", || match self {
            Self::Memory(store) => store.claim_next_job(
                owner,
                now_ms,
                max_executing_transfers,
                prefer_pending,
                lease_ms,
                after_job_id,
            ),
            Self::Sqlite(store) => store.claim_next_job(
                owner,
                now_ms,
                max_executing_transfers,
                prefer_pending,
                lease_ms,
                after_job_id,
            ),
            Self::Mysql(store) => store.claim_next_job(
                owner,
                now_ms,
                max_executing_transfers,
                prefer_pending,
                lease_ms,
                after_job_id,
            ),
            Self::Postgres(store) => store.claim_next_job(
                owner,
                now_ms,
                max_executing_transfers,
                prefer_pending,
                lease_ms,
                after_job_id,
            ),
        })
    }

    fn mark_canceling(
        &self,
        job_id: &str,
        run_id: u64,
        owner: &str,
        reason: &str,
        now_ms: i64,
    ) -> FsResult<bool> {
        self.record_store_operation("mark_canceling", || match self {
            Self::Memory(store) => store.mark_canceling(job_id, run_id, owner, reason, now_ms),
            Self::Sqlite(store) => store.mark_canceling(job_id, run_id, owner, reason, now_ms),
            Self::Mysql(store) => store.mark_canceling(job_id, run_id, owner, reason, now_ms),
            Self::Postgres(store) => store.mark_canceling(job_id, run_id, owner, reason, now_ms),
        })
    }

    fn revive_run(&self, job_id: &str, run_id: u64, now_ms: i64) -> FsResult<bool> {
        self.record_store_operation("revive_run", || match self {
            Self::Memory(store) => store.revive_run(job_id, run_id, now_ms),
            Self::Sqlite(store) => store.revive_run(job_id, run_id, now_ms),
            Self::Mysql(store) => store.revive_run(job_id, run_id, now_ms),
            Self::Postgres(store) => store.revive_run(job_id, run_id, now_ms),
        })
    }

    fn fail_job(&self, job_id: &str, run_id: u64, reason: &str, now_ms: i64) -> FsResult<bool> {
        self.record_store_operation("fail_job", || match self {
            Self::Memory(store) => store.fail_job(job_id, run_id, reason, now_ms),
            Self::Sqlite(store) => store.fail_job(job_id, run_id, reason, now_ms),
            Self::Mysql(store) => store.fail_job(job_id, run_id, reason, now_ms),
            Self::Postgres(store) => store.fail_job(job_id, run_id, reason, now_ms),
        })
    }

    /// Must forward, not inherit the trait's no-op default.
    ///
    /// The default exists for stores that persist the whole record, but this
    /// enum *is* the store the server uses: without the delegation every backend
    /// silently kept an empty `owner_addr`, so status relay fell back to local
    /// reads and the console could never show which instance was running a job.
    fn set_owner_addr(&self, job_id: &str, run_id: u64, owner_addr: &str) -> FsResult<()> {
        self.record_store_operation("set_owner_addr", || match self {
            Self::Memory(store) => store.set_owner_addr(job_id, run_id, owner_addr),
            #[cfg(feature = "transfer-store-sqlite")]
            Self::Sqlite(store) => store.set_owner_addr(job_id, run_id, owner_addr),
            #[cfg(feature = "transfer-store-mysql")]
            Self::Mysql(store) => store.set_owner_addr(job_id, run_id, owner_addr),
            #[cfg(feature = "transfer-store-postgres")]
            Self::Postgres(store) => store.set_owner_addr(job_id, run_id, owner_addr),
        })
    }

    fn update_transfer_state(&self, update: TransferStateUpdate) -> FsResult<bool> {
        self.record_store_operation("update_transfer_state", || match self {
            Self::Memory(store) => store.update_transfer_state(update),
            #[cfg(feature = "transfer-store-sqlite")]
            Self::Sqlite(store) => store.update_transfer_state(update),
            #[cfg(feature = "transfer-store-mysql")]
            Self::Mysql(store) => store.update_transfer_state(update),
            #[cfg(feature = "transfer-store-postgres")]
            Self::Postgres(store) => store.update_transfer_state(update),
        })
    }

    fn requeue_transfer(&self, update: TransferRequeueUpdate) -> FsResult<bool> {
        self.record_store_operation("requeue_transfer", || match self {
            Self::Memory(store) => store.requeue_transfer(update),
            #[cfg(feature = "transfer-store-sqlite")]
            Self::Sqlite(store) => store.requeue_transfer(update),
            #[cfg(feature = "transfer-store-mysql")]
            Self::Mysql(store) => store.requeue_transfer(update),
            #[cfg(feature = "transfer-store-postgres")]
            Self::Postgres(store) => store.requeue_transfer(update),
        })
    }

    fn set_transfer_cv_metadata_epoch(
        &self,
        job_id: &str,
        run_id: u64,
        owner: &str,
        fence: u64,
        cv_metadata_epoch: u64,
        now_ms: i64,
    ) -> FsResult<bool> {
        self.record_store_operation("set_transfer_cv_metadata_epoch", || match self {
            Self::Memory(store) => store.set_transfer_cv_metadata_epoch(
                job_id,
                run_id,
                owner,
                fence,
                cv_metadata_epoch,
                now_ms,
            ),
            #[cfg(feature = "transfer-store-sqlite")]
            Self::Sqlite(store) => store.set_transfer_cv_metadata_epoch(
                job_id,
                run_id,
                owner,
                fence,
                cv_metadata_epoch,
                now_ms,
            ),
            #[cfg(feature = "transfer-store-mysql")]
            Self::Mysql(store) => store.set_transfer_cv_metadata_epoch(
                job_id,
                run_id,
                owner,
                fence,
                cv_metadata_epoch,
                now_ms,
            ),
            #[cfg(feature = "transfer-store-postgres")]
            Self::Postgres(store) => store.set_transfer_cv_metadata_epoch(
                job_id,
                run_id,
                owner,
                fence,
                cv_metadata_epoch,
                now_ms,
            ),
        })
    }

    fn set_transfer_mount_snapshot(&self, update: TransferMountSnapshotUpdate) -> FsResult<bool> {
        self.record_store_operation("set_transfer_mount_snapshot", || match self {
            Self::Memory(store) => store.set_transfer_mount_snapshot(update),
            #[cfg(feature = "transfer-store-sqlite")]
            Self::Sqlite(store) => store.set_transfer_mount_snapshot(update),
            #[cfg(feature = "transfer-store-mysql")]
            Self::Mysql(store) => store.set_transfer_mount_snapshot(update),
            #[cfg(feature = "transfer-store-postgres")]
            Self::Postgres(store) => store.set_transfer_mount_snapshot(update),
        })
    }

    fn insert_tasks(&self, tasks: Vec<TransferTaskRecord>) -> FsResult<()> {
        self.record_store_operation("insert_tasks", || match self {
            Self::Memory(store) => store.insert_tasks(tasks),
            #[cfg(feature = "transfer-store-sqlite")]
            Self::Sqlite(store) => store.insert_tasks(tasks),
            #[cfg(feature = "transfer-store-mysql")]
            Self::Mysql(store) => store.insert_tasks(tasks),
            #[cfg(feature = "transfer-store-postgres")]
            Self::Postgres(store) => store.insert_tasks(tasks),
        })
    }

    fn append_planned_tasks(&self, update: TransferPlannedTasks) -> FsResult<bool> {
        self.record_store_operation("append_planned_tasks", || match self {
            Self::Memory(store) => store.append_planned_tasks(update),
            #[cfg(feature = "transfer-store-sqlite")]
            Self::Sqlite(store) => store.append_planned_tasks(update),
            #[cfg(feature = "transfer-store-mysql")]
            Self::Mysql(store) => store.append_planned_tasks(update),
            #[cfg(feature = "transfer-store-postgres")]
            Self::Postgres(store) => store.append_planned_tasks(update),
        })
    }

    fn persist_planned_tasks(&self, update: TransferPlannedTasks) -> FsResult<bool> {
        self.record_store_operation("persist_planned_tasks", || match self {
            Self::Memory(store) => store.persist_planned_tasks(update),
            #[cfg(feature = "transfer-store-sqlite")]
            Self::Sqlite(store) => store.persist_planned_tasks(update),
            #[cfg(feature = "transfer-store-mysql")]
            Self::Mysql(store) => store.persist_planned_tasks(update),
            #[cfg(feature = "transfer-store-postgres")]
            Self::Postgres(store) => store.persist_planned_tasks(update),
        })
    }

    fn has_transfer_tasks(&self, job_id: &str, run_id: u64) -> FsResult<bool> {
        self.record_store_operation("has_transfer_tasks", || match self {
            Self::Memory(store) => store.has_transfer_tasks(job_id, run_id),
            #[cfg(feature = "transfer-store-sqlite")]
            Self::Sqlite(store) => store.has_transfer_tasks(job_id, run_id),
            #[cfg(feature = "transfer-store-mysql")]
            Self::Mysql(store) => store.has_transfer_tasks(job_id, run_id),
            #[cfg(feature = "transfer-store-postgres")]
            Self::Postgres(store) => store.has_transfer_tasks(job_id, run_id),
        })
    }

    fn update_task_state(&self, update: TransferTaskStateUpdate) -> FsResult<bool> {
        self.record_store_operation("update_task_state", || match self {
            Self::Memory(store) => store.update_task_state(update),
            #[cfg(feature = "transfer-store-sqlite")]
            Self::Sqlite(store) => store.update_task_state(update),
            #[cfg(feature = "transfer-store-mysql")]
            Self::Mysql(store) => store.update_task_state(update),
            #[cfg(feature = "transfer-store-postgres")]
            Self::Postgres(store) => store.update_task_state(update),
        })
    }

    fn claim_pending_tasks(
        &self,
        job_id: &str,
        run_id: u64,
        limit: usize,
    ) -> FsResult<Vec<TransferTaskRecord>> {
        self.record_store_operation("claim_pending_tasks", || match self {
            Self::Memory(store) => store.claim_pending_tasks(job_id, run_id, limit),
            #[cfg(feature = "transfer-store-sqlite")]
            Self::Sqlite(store) => store.claim_pending_tasks(job_id, run_id, limit),
            #[cfg(feature = "transfer-store-mysql")]
            Self::Mysql(store) => store.claim_pending_tasks(job_id, run_id, limit),
            #[cfg(feature = "transfer-store-postgres")]
            Self::Postgres(store) => store.claim_pending_tasks(job_id, run_id, limit),
        })
    }

    fn mark_stale_attempts(
        &self,
        job_id: &str,
        run_id: u64,
        owner: &str,
        fence: u64,
        now_ms: i64,
        candidates: Vec<TransferTaskRecord>,
    ) -> FsResult<Vec<StaleTaskAttempt>> {
        self.record_store_operation("mark_stale_attempts", || match self {
            Self::Memory(store) => {
                store.mark_stale_attempts(job_id, run_id, owner, fence, now_ms, candidates)
            }
            #[cfg(feature = "transfer-store-sqlite")]
            Self::Sqlite(store) => {
                store.mark_stale_attempts(job_id, run_id, owner, fence, now_ms, candidates)
            }
            #[cfg(feature = "transfer-store-mysql")]
            Self::Mysql(store) => {
                store.mark_stale_attempts(job_id, run_id, owner, fence, now_ms, candidates)
            }
            #[cfg(feature = "transfer-store-postgres")]
            Self::Postgres(store) => {
                store.mark_stale_attempts(job_id, run_id, owner, fence, now_ms, candidates)
            }
        })
    }

    fn list_stale_running_tasks(
        &self,
        job_id: &str,
        run_id: u64,
        stale_before_ms: i64,
        limit: usize,
    ) -> FsResult<Vec<TransferTaskRecord>> {
        self.record_store_operation("list_stale_running_tasks", || match self {
            Self::Memory(store) => {
                store.list_stale_running_tasks(job_id, run_id, stale_before_ms, limit)
            }
            #[cfg(feature = "transfer-store-sqlite")]
            Self::Sqlite(store) => {
                store.list_stale_running_tasks(job_id, run_id, stale_before_ms, limit)
            }
            #[cfg(feature = "transfer-store-mysql")]
            Self::Mysql(store) => {
                store.list_stale_running_tasks(job_id, run_id, stale_before_ms, limit)
            }
            #[cfg(feature = "transfer-store-postgres")]
            Self::Postgres(store) => {
                store.list_stale_running_tasks(job_id, run_id, stale_before_ms, limit)
            }
        })
    }

    fn list_tasks_by_state(
        &self,
        job_id: &str,
        run_id: u64,
        state: TransferTaskState,
        limit: usize,
    ) -> FsResult<Vec<TransferTaskRecord>> {
        self.record_store_operation("list_tasks_by_state", || match self {
            Self::Memory(store) => store.list_tasks_by_state(job_id, run_id, state, limit),
            #[cfg(feature = "transfer-store-sqlite")]
            Self::Sqlite(store) => store.list_tasks_by_state(job_id, run_id, state, limit),
            #[cfg(feature = "transfer-store-mysql")]
            Self::Mysql(store) => store.list_tasks_by_state(job_id, run_id, state, limit),
            #[cfg(feature = "transfer-store-postgres")]
            Self::Postgres(store) => store.list_tasks_by_state(job_id, run_id, state, limit),
        })
    }

    fn list_failed_tasks(&self, job_id: &str, run_id: u64) -> FsResult<Vec<TransferTaskRecord>> {
        self.record_store_operation("list_failed_tasks", || match self {
            Self::Memory(store) => store.list_failed_tasks(job_id, run_id),
            #[cfg(feature = "transfer-store-sqlite")]
            Self::Sqlite(store) => store.list_failed_tasks(job_id, run_id),
            #[cfg(feature = "transfer-store-mysql")]
            Self::Mysql(store) => store.list_failed_tasks(job_id, run_id),
            #[cfg(feature = "transfer-store-postgres")]
            Self::Postgres(store) => store.list_failed_tasks(job_id, run_id),
        })
    }

    fn has_failed_tasks(&self, job_id: &str, run_id: u64) -> FsResult<bool> {
        self.record_store_operation("has_failed_tasks", || match self {
            Self::Memory(store) => store.has_failed_tasks(job_id, run_id),
            #[cfg(feature = "transfer-store-sqlite")]
            Self::Sqlite(store) => store.has_failed_tasks(job_id, run_id),
            #[cfg(feature = "transfer-store-mysql")]
            Self::Mysql(store) => store.has_failed_tasks(job_id, run_id),
            #[cfg(feature = "transfer-store-postgres")]
            Self::Postgres(store) => store.has_failed_tasks(job_id, run_id),
        })
    }

    fn has_recoverable_tasks(&self, job_id: &str, run_id: u64) -> FsResult<bool> {
        self.record_store_operation("has_recoverable_tasks", || match self {
            Self::Memory(store) => store.has_recoverable_tasks(job_id, run_id),
            #[cfg(feature = "transfer-store-sqlite")]
            Self::Sqlite(store) => store.has_recoverable_tasks(job_id, run_id),
            #[cfg(feature = "transfer-store-mysql")]
            Self::Mysql(store) => store.has_recoverable_tasks(job_id, run_id),
            #[cfg(feature = "transfer-store-postgres")]
            Self::Postgres(store) => store.has_recoverable_tasks(job_id, run_id),
        })
    }

    fn start_task_attempt(&self, start: TaskAttemptStart) -> FsResult<bool> {
        self.record_store_operation("start_task_attempt", || match self {
            Self::Memory(store) => store.start_task_attempt(start),
            #[cfg(feature = "transfer-store-sqlite")]
            Self::Sqlite(store) => store.start_task_attempt(start),
            #[cfg(feature = "transfer-store-mysql")]
            Self::Mysql(store) => store.start_task_attempt(start),
            #[cfg(feature = "transfer-store-postgres")]
            Self::Postgres(store) => store.start_task_attempt(start),
        })
    }

    fn update_task_report(&self, report: TransferTaskReport) -> FsResult<bool> {
        self.record_store_operation("update_task_report", || match self {
            Self::Memory(store) => store.update_task_report(report),
            #[cfg(feature = "transfer-store-sqlite")]
            Self::Sqlite(store) => store.update_task_report(report),
            #[cfg(feature = "transfer-store-mysql")]
            Self::Mysql(store) => store.update_task_report(report),
            #[cfg(feature = "transfer-store-postgres")]
            Self::Postgres(store) => store.update_task_report(report),
        })
    }

    fn update_task_reports(&self, reports: Vec<TransferTaskReport>) -> FsResult<Vec<bool>> {
        self.record_store_operation("update_task_reports", || match self {
            Self::Memory(store) => store.update_task_reports(reports),
            #[cfg(feature = "transfer-store-sqlite")]
            Self::Sqlite(store) => store.update_task_reports(reports),
            #[cfg(feature = "transfer-store-mysql")]
            Self::Mysql(store) => store.update_task_reports(reports),
            #[cfg(feature = "transfer-store-postgres")]
            Self::Postgres(store) => store.update_task_reports(reports),
        })
    }

    fn update_job_progress_summary(
        &self,
        job_id: &str,
        run_id: u64,
        summary: TransferProgress,
        now_ms: i64,
    ) -> FsResult<()> {
        self.record_store_operation("update_job_progress_summary", || match self {
            Self::Memory(store) => {
                store.update_job_progress_summary(job_id, run_id, summary, now_ms)
            }
            #[cfg(feature = "transfer-store-sqlite")]
            Self::Sqlite(store) => {
                store.update_job_progress_summary(job_id, run_id, summary, now_ms)
            }
            #[cfg(feature = "transfer-store-mysql")]
            Self::Mysql(store) => {
                store.update_job_progress_summary(job_id, run_id, summary, now_ms)
            }
            #[cfg(feature = "transfer-store-postgres")]
            Self::Postgres(store) => {
                store.update_job_progress_summary(job_id, run_id, summary, now_ms)
            }
        })
    }

    fn update_terminal_snapshot(
        &self,
        job_id: &str,
        run_id: u64,
        summary: TransferProgress,
        counts: TransferTaskCounts,
        now_ms: i64,
    ) -> FsResult<()> {
        self.record_store_operation("update_terminal_snapshot", || match self {
            Self::Memory(store) => {
                store.update_terminal_snapshot(job_id, run_id, summary, counts, now_ms)
            }
            #[cfg(feature = "transfer-store-sqlite")]
            Self::Sqlite(store) => {
                store.update_terminal_snapshot(job_id, run_id, summary, counts, now_ms)
            }
            #[cfg(feature = "transfer-store-mysql")]
            Self::Mysql(store) => {
                store.update_terminal_snapshot(job_id, run_id, summary, counts, now_ms)
            }
            #[cfg(feature = "transfer-store-postgres")]
            Self::Postgres(store) => {
                store.update_terminal_snapshot(job_id, run_id, summary, counts, now_ms)
            }
        })
    }
}

impl TransferStoreBackend {
    pub fn backend_label(&self) -> &'static str {
        match self {
            Self::Memory(_) => "memory",
            #[cfg(feature = "transfer-store-sqlite")]
            Self::Sqlite(_) => "sqlite",
            #[cfg(feature = "transfer-store-mysql")]
            Self::Mysql(_) => "mysql",
            #[cfg(feature = "transfer-store-postgres")]
            Self::Postgres(_) => "postgres",
        }
    }

    fn record_store_operation<T>(
        &self,
        operation: &'static str,
        f: impl FnOnce() -> FsResult<T>,
    ) -> FsResult<T> {
        let start = Instant::now();
        let result = f();
        if let Ok(metrics) = TransferMetrics::get() {
            let backend = self.backend_label();
            metrics.observe_store_operation(
                backend,
                operation,
                if result.is_ok() { "success" } else { "error" },
                start.elapsed().as_micros(),
            );
            if is_store_unavailable_result(&result) {
                metrics.record_store_unavailable(backend, operation);
            } else {
                metrics.record_store_available(backend);
            }
        }
        result
    }
}

fn is_store_unavailable_result<T>(result: &FsResult<T>) -> bool {
    let Err(err) = result else {
        return false;
    };
    is_store_unavailable_error(err)
}

pub(crate) fn is_store_unavailable_error(err: &FsError) -> bool {
    if matches!(
        err.kind(),
        curvine_error::ErrorKind::IO | curvine_error::ErrorKind::TransferStoreUnavailable
    ) {
        return true;
    }

    let message = err.to_string();
    message.contains("sqlite transfer store error:")
        || message.contains("mysql transfer store error:")
        || message.contains("postgresql transfer store error:")
        || message.contains("Unknown database")
        || message.contains("No database selected")
        || message.contains("doesn't exist")
        || message.contains("Can't connect")
        || message.contains("Connection refused")
        || message.contains("Lost connection")
        || message.contains("server has gone away")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transfer::MemoryTransferStore;
    use curvine_model::{TransferJobRecord, TransferKind, TransferProgress, TransferState};

    fn job(job_id: &str) -> TransferJobRecord {
        TransferJobRecord {
            job_key: format!("key-{job_id}"),
            job_id: job_id.to_string(),
            run_id: 1,
            kind: TransferKind::Load,
            source_path: "file:///src".to_string(),
            target_path: format!("/dst/{job_id}"),
            command_json: "{}".to_string(),
            mount_snapshot_json: "{}".to_string(),
            secret_ref_json: "{}".to_string(),
            cluster_snapshot_version: 1,
            mount_snapshot_attempts: 0,
            planning_attempts: 0,
            takeover_count: 0,
            placement_addr: "cv-transfer-0:9010".to_string(),
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
            client_request_id: format!("req-{job_id}"),
            submitter: "test".to_string(),
            tenant: "test".to_string(),
            created_at: 1,
            updated_at: 1,
        }
    }

    /// The enum is the store the server actually holds, so every method it fails
    /// to forward silently falls back to the trait default. `set_owner_addr`'s
    /// default is a no-op, which is how every job kept an empty owner address.
    #[test]
    fn backend_forwards_set_owner_addr_to_the_inner_store() {
        let backend = TransferStoreBackend::Memory(MemoryTransferStore::new());
        backend
            .create_or_get_by_request_id_checked(job("forwarded"))
            .unwrap();

        backend
            .set_owner_addr("forwarded", 1, "cv-transfer-1:9010")
            .unwrap();

        let stored = backend.get_transfer("forwarded").unwrap().unwrap();
        assert_eq!(stored.owner_addr, "cv-transfer-1:9010");
    }
}
