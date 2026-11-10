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

#![allow(clippy::needless_range_loop)]

use crate::master::fs::MasterFilesystem;
use crate::master::journal::recovery::{classify_apply_error, ApplyDisposition, RecoveryGate};
use crate::master::journal::*;
use crate::master::meta::inode::InodeView::File;
use crate::master::meta::inode::{InodePath, InodeView};
use crate::master::meta::InodeId;
use crate::master::{
    JobManager, Master, MasterMetrics, MountManager, SyncFsDir, SyncWorkerManager,
};
use curvine_config::JournalConf;
use curvine_core_error::{err_box, ternary, CommonResult, ErrorExt};
use curvine_error::FsError;
use curvine_model::RenameFlags;
use curvine_raft::conf::JournalConfExt;
use curvine_raft::proto::raft::{AppliedIndex, FsmState, SnapshotData};
use curvine_raft::raft::storage::{AppStorage, ApplyMsg, LogStorage, RocksLogStorage};
use curvine_raft::raft::{RaftClient, RaftError, RaftResult, RaftUtils};
use curvine_runtime::common::{FileUtils, TimeSpent};
use curvine_runtime::runtime::{RpcRuntime, Runtime};
use curvine_runtime::sync::channel::{AsyncChannel, AsyncReceiver, AsyncSender, CallChannel};
use log::{debug, error, info, warn};
use raft::eraftpb::{Entry, EntryType};
use raft::StateRole;
use std::collections::VecDeque;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;
use std::{fs, mem};

// Replay the master metadata operation log.
#[derive(Clone)]
pub struct JournalLoader {
    node_id: u64,
    fs_dir: SyncFsDir,
    mnt_mgr: Arc<MountManager>,
    journal_writer: Arc<JournalWriter>,
    ufs_loader: UfsLoader,
    worker_manager: SyncWorkerManager,
    recovery: RecoveryGate,
    log_store: RocksLogStorage,
    sender: AsyncSender<ApplyMsg>,
    fsm_state: Arc<Mutex<FsmState>>,
    retain_checkpoint_num: usize,
    ignore_reply_error: bool,
    max_retry_num: u64,
    skip_failed_ufs_replay_after_retry: bool,
    batch_size: u64,
    retry_interval: Duration,
    metrics: &'static MasterMetrics,
    has_apply_worker: bool,
    /// Shared raft-term handle, so the session lease epoch follows election
    /// instead of process start (#1883 review).
    term: Arc<AtomicU64>,
}

impl JournalLoader {
    pub fn new_replay_loader(
        fs_dir: SyncFsDir,
        mnt_mgr: Arc<MountManager>,
        conf: &JournalConf,
        job_manager: Arc<JobManager>,
    ) -> CommonResult<Self> {
        let rt = conf.create_runtime();
        let client = RaftClient::from_conf(rt.clone(), conf);
        let recovery = RecoveryGate::new();
        let journal_writer = Arc::new(JournalWriter::new(true, client, conf, recovery.clone())?);
        let log_store = RocksLogStorage::from_conf(conf, false);
        let worker_manager = SyncWorkerManager::new(crate::master::fs::WorkerManager::new(
            &curvine_config::ClusterConf::default(),
        )?);
        Self::build(
            rt,
            fs_dir,
            mnt_mgr,
            conf,
            job_manager,
            log_store,
            journal_writer,
            worker_manager,
            recovery,
            true,
            Arc::new(AtomicU64::new(0)),
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        rt: Arc<Runtime>,
        fs_dir: SyncFsDir,
        mnt_mgr: Arc<MountManager>,
        conf: &JournalConf,
        job_manager: Arc<JobManager>,
        log_store: RocksLogStorage,
        journal_writer: Arc<JournalWriter>,
        worker_manager: SyncWorkerManager,
        recovery: RecoveryGate,
        term: Arc<AtomicU64>,
    ) -> CommonResult<Self> {
        Self::build(
            rt,
            fs_dir,
            mnt_mgr,
            conf,
            job_manager,
            log_store,
            journal_writer,
            worker_manager,
            recovery,
            false,
            term,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn build(
        rt: Arc<Runtime>,
        fs_dir: SyncFsDir,
        mnt_mgr: Arc<MountManager>,
        conf: &JournalConf,
        job_manager: Arc<JobManager>,
        log_store: RocksLogStorage,
        journal_writer: Arc<JournalWriter>,
        worker_manager: SyncWorkerManager,
        recovery: RecoveryGate,
        testing: bool,
        term: Arc<AtomicU64>,
    ) -> CommonResult<Self> {
        // Install the fs_mode release gate before anything can serve a release:
        // with transfer enabled the replayer no longer waits for exports, so an
        // unverified Free would drop Curvine data UFS never received.
        if job_manager.transfer_enabled() {
            let releaser = crate::master::journal::FreeReconciler::start(job_manager.clone());
            job_manager.master_fs().set_fs_mode_releaser(releaser);
        }

        let ufs_loader = UfsLoader::new(job_manager, fs_dir.clone(), conf);
        let (sender, receiver) = AsyncChannel::new(conf.writer_channel_size).split();
        let loader = Self {
            node_id: conf.node_id()?,
            fs_dir,
            mnt_mgr,
            journal_writer,
            ufs_loader,
            worker_manager,
            recovery,
            log_store,
            sender,
            fsm_state: Arc::new(Mutex::new(FsmState::default())),
            retain_checkpoint_num: conf.retain_checkpoint_num,
            ignore_reply_error: conf.ignore_reply_error,
            max_retry_num: conf.max_retry_num,
            skip_failed_ufs_replay_after_retry: conf.skip_failed_ufs_replay_after_retry,
            batch_size: conf.scan_batch_size,
            retry_interval: Duration::from_secs(conf.retry_interval_secs),
            metrics: Master::get_metrics()?,
            has_apply_worker: !testing,
            term,
        };

        loader.refresh_metrics()?;
        if !testing {
            let apply_loader = loader.clone();
            rt.spawn(async move {
                Self::run_apply(apply_loader, receiver).await;
            });
        }

        Ok(loader)
    }

    fn fsm_state(&self) -> CommonResult<MutexGuard<'_, FsmState>> {
        match self.fsm_state.lock() {
            Ok(state) => Ok(state),
            Err(e) => err_box!("fsm_state lock poisoned: {}", e),
        }
    }

    fn fsm_state_snapshot(&self) -> CommonResult<FsmState> {
        Ok(self.fsm_state()?.clone())
    }

    fn set_applied(
        &self,
        is_leader: bool,
        applied: AppliedIndex,
        has_ufs_affecting: bool,
    ) -> CommonResult<()> {
        // The leader runs the UFS side effect inline with the metadata op, so a
        // completed apply moves both watermarks together and records the marker
        // followers replay.
        if is_leader && has_ufs_affecting {
            self.journal_writer
                .log_ufs_applied(applied.op_id, applied.term, applied.index)?;
        }

        let mut state = self.fsm_state()?;
        if is_leader {
            state.ufs_applied = applied.clone();
            state.applied = applied;
        } else {
            state.applied = applied;
        }

        if is_leader {
            let caught_up = state.ufs_applied.index >= state.applied.index;
            self.recovery.set_ufs_caught_up(caught_up);
            self.recovery
                .set_pending(state.applied.index.saturating_sub(state.ufs_applied.index));
        }

        self.refresh_fsm_metrics(&state);
        drop(state);
        self.hard_state_changed(&self.log_store.hard_state());
        Ok(())
    }

    fn refresh_fsm_metrics(&self, state: &FsmState) {
        self.metrics.journal_applied.set(state.applied.index as i64);
        self.metrics
            .journal_ufs_applied
            .set(state.ufs_applied.index as i64);
    }

    /// Refresh gauges only: never advance metadata/UFS replay watermarks just
    /// to make monitoring appear caught up.
    fn refresh_metrics(&self) -> CommonResult<()> {
        self.refresh_fsm_metrics(&*self.fsm_state()?);
        self.hard_state_changed(&self.log_store.hard_state());
        Ok(())
    }

    fn build_applied(entry: &Entry) -> AppliedIndex {
        AppliedIndex {
            term: entry.term,
            index: entry.index,
            ..Default::default()
        }
    }

    /// Advance the committed apply high-water mark without replaying any
    /// metadata op: the no-op/config branch and batches deliberately skipped
    /// under `ignore_reply_error` both need the watermark to move so that
    /// replay can continue past them.
    fn advance_watermark(&self, is_leader: bool, cur: FsmState, entry: &Entry) -> CommonResult<()> {
        let mut applied = if is_leader {
            cur.ufs_applied
        } else {
            cur.applied
        };
        applied.term = entry.term;
        applied.index = entry.index;
        self.set_applied(is_leader, applied, false)
    }

    async fn replay_entry(&self, is_leader: bool, entry: &JournalEntry) -> CommonResult<()> {
        {
            let fs_dir = self.fs_dir.read();
            fs_dir.update_op_id(entry.op_id());
            if let Some(inode_id) = entry.inode_id() {
                fs_dir.update_last_inode_id(inode_id)?;
            }
        }

        if is_leader {
            // The leader already applied this metadata in the RPC handler; the
            // UFS side effect is the part replay owes here.
            self.ufs_loader.apply_entry(entry).await
        } else {
            self.apply_entry(entry.clone())
        }
    }

    async fn apply0(
        &self,
        is_leader: bool,
        entry: &Entry,
        skip_ufs_error: bool,
    ) -> CommonResult<()> {
        let cur = self.fsm_state_snapshot()?;
        let role_applied = ternary!(is_leader, cur.ufs_applied.index, cur.applied.index);
        if entry.index <= role_applied {
            info!(
                "skip entry index {}, term {}, fsm_state {:?}",
                entry.index, entry.term, cur
            );
            return Ok(());
        }

        // Empty leader no-ops and configuration entries have no metadata mutation,
        // but still advance the committed apply high-water mark.
        if entry.get_entry_type() != EntryType::EntryNormal || entry.data.is_empty() {
            return self.advance_watermark(is_leader, cur, entry);
        }

        let batch = match JournalBatch::decode(&entry.data) {
            Ok(batch) => batch,
            Err(e) if self.ignore_reply_error => {
                error!(
                    "skip failed journal batch deserialization, entry index={}, term={}, error={}",
                    entry.index, entry.term, e
                );
                return self.advance_watermark(is_leader, cur, entry);
            }
            Err(e) => return err_box!("failed to decode journal batch: {}", e),
        };

        let batch_len = batch.len();
        let mut snapshot = None;
        let mut applied = Self::build_applied(entry);
        let mut has_ufs_affecting = false;

        for (seq, op_entry) in batch.batch.into_iter().enumerate() {
            applied.op_id = op_entry.op_id();
            applied.rpc_id = op_entry.rpc_id();

            match op_entry {
                JournalEntry::Snapshot(e) if is_leader && e.node_id == self.node_id => {
                    if seq + 1 != batch_len {
                        return err_box!("snapshot should be the last entry");
                    }
                    snapshot.replace(e);
                    continue;
                }

                JournalEntry::CacheInvalidation(_)
                | JournalEntry::UfsApplied(_)
                | JournalEntry::UfsDeferred(_) => (),

                // A purge only touches the local trash namespace, so it owes no
                // UFS acknowledgement record.
                JournalEntry::TrashPurge(_) => (),

                _ => has_ufs_affecting = true,
            }

            if let Err(e) = self.replay_entry(is_leader, &op_entry).await {
                if self.ignore_reply_error {
                    error!(
                        "skip failed journal replay, entry index={}, term={}, journal={:?}, error={}",
                        entry.index, entry.term, op_entry, e
                    );
                    continue;
                }

                if is_leader && skip_ufs_error {
                    error!(
                        "skip failed UFS replay after retries, entry index={}, term={}, journal={:?}, error={}",
                        entry.index, entry.term, op_entry, e
                    );
                    continue;
                }

                return err_box!("failed to apply journal: {:?}: {}", op_entry, e);
            }
        }

        self.set_applied(is_leader, applied, has_ufs_affecting)?;

        if let Some(e) = snapshot {
            let spent = TimeSpent::new();
            let snap_data = self.create_snapshot0(Some(e.dir.to_string()))?;

            self.log_store.create_snapshot(snap_data.clone())?;
            self.log_store.compact(snap_data.fsm_state.compact())?;

            info!(
                "create leader snapshot, dir={}, cost={}ms, fsm_state={:?}",
                e.dir,
                spent.used_ms(),
                snap_data.fsm_state
            );
        }

        Ok(())
    }

    async fn apply_msg(
        &self,
        is_leader: bool,
        msg: &ApplyMsg,
        skip_ufs_error: bool,
    ) -> CommonResult<()> {
        match msg {
            ApplyMsg::Entry(entry) | ApplyMsg::EntryWithAck((entry, _)) => {
                self.apply0(is_leader, entry, skip_ufs_error).await?;
                Ok(())
            }

            ApplyMsg::Scan(applied_index) => {
                let mut last_applied = applied_index.index;
                if is_leader && skip_ufs_error {
                    last_applied = last_applied.max(self.fsm_state_snapshot()?.ufs_applied.index);
                }

                let commit_index = self.log_store.hard_state().commit;
                loop {
                    if last_applied >= commit_index {
                        return Ok(());
                    }

                    let high = (last_applied + self.batch_size).min(commit_index + 1);
                    let list = self.log_store.scan_entries(last_applied + 1, high)?;

                    if list.is_empty() {
                        return Ok(());
                    };

                    info!(
                        "replay-scan, start_index: {}, entries: {}, commit_index: {}",
                        last_applied + 1,
                        list.len(),
                        commit_index
                    );

                    for entry in list {
                        self.apply0(is_leader, &entry, skip_ufs_error).await?;
                        last_applied = entry.index;
                        if skip_ufs_error {
                            return Ok(());
                        }
                    }
                }
            }

            _ => err_box!("unsupported apply message in journal loader apply_msg"),
        }
    }

    fn complete_entry_ack(msg: ApplyMsg, result: RaftResult<()>) {
        if let ApplyMsg::EntryWithAck((_, tx)) = msg {
            if let Err(e) = tx.send(result) {
                warn!("send journal entry apply acknowledgement failed: {}", e);
            }
        }
    }

    async fn catch_up_committed_metadata(&self) -> RaftResult<()> {
        let committed = self.log_store.hard_state().commit;
        let applied = self.fsm_state_snapshot()?.applied;
        self.apply_msg(false, &ApplyMsg::new_scan(applied), false)
            .await?;

        let metadata_applied = self.fsm_state_snapshot()?.applied.index;
        if metadata_applied < committed {
            return err_box!(
                "metadata catch-up stopped before committed raft index: applied={}, commit={}",
                metadata_applied,
                committed
            );
        }
        Ok(())
    }
    fn get_ufs_applied(&self) -> CommonResult<AppliedIndex> {
        Ok(self.fsm_state()?.ufs_applied.clone())
    }

    async fn run_apply(self, mut receiver: AsyncReceiver<ApplyMsg>) {
        let mut is_leader = false;
        let mut retry_msg: Option<ApplyMsg> = None;
        let mut retry_deadline: Option<tokio::time::Instant> = None;
        let mut buffered: VecDeque<ApplyMsg> = VecDeque::new();
        let mut logged_skip_ignored = false;

        loop {
            let apply_msg = if retry_msg.is_some() {
                let deadline = match retry_deadline {
                    Some(deadline) => deadline,
                    None => {
                        let deadline = tokio::time::Instant::now() + self.retry_interval;
                        retry_deadline = Some(deadline);
                        deadline
                    }
                };
                if tokio::time::Instant::now() >= deadline {
                    retry_deadline = None;
                    match retry_msg.take() {
                        Some(msg) => msg,
                        None => continue,
                    }
                } else {
                    tokio::select! {
                        biased;
                        _ = tokio::time::sleep_until(deadline) => {
                            retry_deadline = None;
                            match retry_msg.take() {
                                Some(msg) => msg,
                                None => continue,
                            }
                        }
                        incoming = receiver.recv() => {
                            match incoming {
                                Some(ApplyMsg::RoleChange(change)) => ApplyMsg::RoleChange(change),
                                Some(ApplyMsg::Shutdown(tx)) => ApplyMsg::Shutdown(tx),
                                Some(ApplyMsg::CreateSnapshot(tx)) => ApplyMsg::CreateSnapshot(tx),
                                Some(other) => {
                                    buffered.push_back(other);
                                    continue;
                                }
                                None => break,
                            }
                        }
                    }
                }
            } else if let Some(msg) = buffered.pop_front() {
                msg
            } else {
                match receiver.recv().await {
                    Some(msg) => msg,
                    None => break,
                }
            };

            match apply_msg {
                ApplyMsg::CreateSnapshot(tx) => {
                    if let Err(e) = tx.send(self.create_snapshot0(None)) {
                        warn!("send create snapshot result failed: {}", e);
                    }
                }

                ApplyMsg::ApplySnapshot((tx, snapshot)) => {
                    if let Err(e) = tx.send(self.apply_snapshot0(snapshot)) {
                        warn!("send apply snapshot result failed: {}", e);
                    }
                }

                ApplyMsg::RoleChange((role, tx)) => {
                    let result = async {
                        if role == StateRole::Leader {
                            // Publish leadership only after committed namespace
                            // mutations have advanced local metadata high-water marks.
                            self.catch_up_committed_metadata().await?;
                            is_leader = true;
                            let ufs_applied = self.get_ufs_applied()?;
                            info!(
                                "metadata caught up for leader promotion, scheduling UFS replay from {:?}",
                                ufs_applied
                            );
                            retry_msg.replace(ApplyMsg::new_scan(ufs_applied));
                        } else {
                            is_leader = false;
                        }
                        Ok(())
                    }
                    .await;

                    if tx.send(result).is_err() {
                        warn!("leader role-change acknowledgement receiver dropped");
                    }
                }

                ApplyMsg::Shutdown(tx) => {
                    let _ = tx.send(());
                    break;
                }

                msg => {
                    if self.recovery.is_safe_mode() {
                        Self::complete_entry_ack(msg, err_box!("master is in safe mode"));
                        continue;
                    }
                    match self.apply_msg(is_leader, &msg, false).await {
                        Ok(()) => Self::complete_entry_ack(msg, Ok(())),
                        Err(error) => {
                            let message = error.to_string();
                            match classify_apply_error(&message) {
                                ApplyDisposition::EnterSafeMode => {
                                    self.recovery.enter_safe_mode(&message);
                                    Self::complete_entry_ack(msg, Err(error.into()));
                                }
                                ApplyDisposition::Retry => {
                                    if !logged_skip_ignored
                                        && self.skip_failed_ufs_replay_after_retry
                                    {
                                        error!(
                                            "skip_failed_ufs_replay_after_retry is ignored; transient apply errors keep retrying (max_retry_num={})",
                                            self.max_retry_num
                                        );
                                        logged_skip_ignored = true;
                                    }
                                    warn!("apply entry failed, retrying without blocking the apply channel: {message}");
                                    retry_deadline =
                                        Some(tokio::time::Instant::now() + self.retry_interval);
                                    retry_msg = Some(msg);
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    fn create_snapshot0(&self, dir_option: Option<String>) -> RaftResult<SnapshotData> {
        Self::check_meta_data_dir(&self.fs_dir, "create_snapshot")?;
        let fsm_state = self.fsm_state_snapshot()?;
        let dir = match dir_option {
            Some(dir) => dir,
            None => {
                let fs_dir = self.fs_dir.read();
                fs_dir
                    .create_checkpoint(fsm_state.applied.index)
                    .map_err(|e| {
                        // create_checkpoint returns a boxed CommonError; convert
                        // through the string form so ctx() is available and the
                        // failure stays a typed RaftError::Storage candidate.
                        let msg = format!(
                            "failed to create metadata checkpoint (id {}) under meta dir {}: {}",
                            fsm_state.applied.index,
                            Self::meta_data_dir(&self.fs_dir),
                            e
                        );
                        if Self::meta_dir_missing(e.as_ref()) {
                            RaftError::storage(msg)
                        } else {
                            RaftError::from(msg)
                        }
                    })?
            }
        };

        let data = RaftUtils::create_file_snapshot(&dir, self.node_id, fsm_state.clone()).map_err(
            |e| {
                e.ctx(format!(
                    "failed to build file snapshot from checkpoint dir {}",
                    dir
                ))
            },
        )?;
        if let Err(e) = self.purge_checkpoint(&dir) {
            warn!("purge checkpoint: {}", e);
        }

        Ok(data)
    }

    /// Fail fast when the master meta data directory is missing or no longer a
    /// valid RocksDB store (e.g. it was deleted while the process was running).
    /// Without this check the failure surfaces later as a bare
    /// "No such file or directory" deep inside checkpoint/restore, which gives
    /// no clue about which directory or what remediation is required.
    fn check_meta_data_dir(fs_dir: &SyncFsDir, op: &str) -> RaftResult<()> {
        let data_dir = Self::meta_data_dir(fs_dir);
        let path = Path::new(&data_dir);
        let valid = path.is_dir() && path.join("CURRENT").is_file();
        if !valid {
            return Err(RaftError::storage(format!(
                "master meta data_dir is missing or is not a valid RocksDB store: {} (operation: {}). \
                 The local metadata was deleted, moved, or only partially restored. \
                 Restore a consistent meta+journal pair from a healthy voter (member recovery, journal.recover_from_peers) before continuing; \
                 restarting this node without restoring cannot recover the data.",
                data_dir, op
            )));
        }
        Ok(())
    }

    fn meta_data_dir(fs_dir: &SyncFsDir) -> String {
        let fs_dir = fs_dir.read();
        fs_dir.store.store().db.conf().data_dir.clone()
    }

    /// Heuristic: the checkpoint/restore layer reports a vanished underlying
    /// directory as a bare boxed io error with no path context. Match it so we
    /// can upgrade it to the typed storage-fatal error.
    fn meta_dir_missing(e: &dyn std::error::Error) -> bool {
        let msg = e.to_string();
        msg.contains("No such file or directory") || msg.contains("os error 2")
    }

    fn apply_snapshot0(&self, snapshot: SnapshotData) -> RaftResult<()> {
        let mut spend = TimeSpent::new();

        // Raft uses the default value as a no-snapshot placeholder. It carries no
        // filesystem state and must not create a restore directory or replace metadata.
        let is_empty_placeholder_snapshot = snapshot.snapshot_id == 0
            && snapshot.files_data.is_none()
            && snapshot.bytes_data.is_none();
        if is_empty_placeholder_snapshot {
            warn!("skip zero-index empty placeholder snapshot");
            return Ok(());
        }

        let restore_path = match &snapshot.files_data {
            Some(data) => data.dir.clone(),
            None => {
                return err_box!(
                    "refusing to apply empty snapshot {} without checkpoint files",
                    snapshot.snapshot_id
                )
            }
        };
        let actual_size = FileUtils::dir_size(&restore_path).unwrap_or_else(|e| {
            warn!(
                "failed to compute checkpoint size for {}: {}",
                restore_path, e
            );
            0
        });
        let checkpoint_size = if log::log_enabled!(log::Level::Info) {
            actual_size
        } else {
            0
        };

        // Never wipe populated metadata with an empty checkpoint. Harness
        // daily failures showed FileNotFound after restore from checkpoint_size=0
        // following raft quorum loss (CurvineIO/curvine#1207).
        // get_file_counts() returns (dir_count, file_count).
        {
            let fs_dir = self.fs_dir.read();
            let (dir_count, file_count) = fs_dir.get_file_counts();
            if actual_size == 0 && (dir_count > 0 || file_count > 0) {
                return err_box!(
                    "refusing to apply empty snapshot {} at {} ({} bytes) over filesystem with {} files and {} dirs",
                    snapshot.snapshot_id,
                    restore_path,
                    actual_size,
                    file_count,
                    dir_count
                );
            }
        }

        let mut fs_dir = self.fs_dir.write();
        fs_dir
            .restore(&restore_path, checkpoint_size)
            .map_err(|e| {
                let msg = format!(
                    "failed to restore metadata from checkpoint {} (checkpoint_size {} bytes); \
                 local meta data_dir is {}: {}",
                    restore_path,
                    checkpoint_size,
                    Self::meta_data_dir(&self.fs_dir),
                    e
                );
                if Self::meta_dir_missing(e.as_ref()) {
                    RaftError::storage(msg)
                } else {
                    RaftError::from(msg)
                }
            })?;
        fs_dir.update_op_id(snapshot.fsm_state.op_id());
        drop(fs_dir);
        let restore_ms = spend.used_ms();
        spend.reset();

        self.mnt_mgr.restore_best_effort();
        let mount_ms = spend.used_ms();

        *self.fsm_state()? = snapshot.fsm_state;
        self.refresh_metrics()?;

        info!(
            "apply_snapshot: fs_dir_restore={} ms, mount_restore={} ms, total={} ms",
            restore_ms,
            mount_ms,
            restore_ms + mount_ms
        );

        Ok(())
    }

    pub fn apply_entry(&self, entry: JournalEntry) -> CommonResult<()> {
        debug!("replay entry: {:?}", entry);

        match entry {
            JournalEntry::Mkdir(e) => self.mkdir(e),

            JournalEntry::CreateFile(e) => self.create_file(e),

            JournalEntry::OverWriteFile(e) => self.overwrite_file(e),

            JournalEntry::AddBlock(e) => self.add_block(e),

            JournalEntry::CompleteFile(e) => self.complete_file(e),

            JournalEntry::Rename(e) => self.rename(e),

            JournalEntry::Delete(e) => self.delete(e),

            JournalEntry::Free(e) => self.free(e),

            JournalEntry::CacheInvalidation(e) => self.cache_invalidation(e),

            JournalEntry::ReopenFile(e) => self.reopen_file(e),

            JournalEntry::Mount(e) => self.mount(e),

            JournalEntry::UnMount(e) => self.unmount(e),

            JournalEntry::SetAttr(e) => self.set_attr(e),

            JournalEntry::Symlink(e) => self.symlink(e),

            JournalEntry::Link(e) => self.link(e),

            JournalEntry::SetLocks(e) => self.set_locks(e),

            JournalEntry::UfsApplied(e) => self.ufs_applied(e),

            // Deferred UFS records are no longer produced. One can still arrive
            // from a log written by the version that copied UFS ops out of the
            // raft log, so replay it as a plain watermark marker instead of
            // failing the batch.
            JournalEntry::UfsDeferred(e) => self.ufs_applied(UfsAppliedEntry {
                op_id: e.op_id,
                rpc_id: e.rpc_id,
                term: e.term,
                index: e.index,
            }),
            JournalEntry::TrashPurge(e) => self.trash_purge(e),

            JournalEntry::WorkerIntent(e) => self.worker_intent(e),

            _ => Ok(()),
        }
    }

    fn worker_intent(&self, entry: WorkerIntentEntry) -> CommonResult<()> {
        self.worker_manager
            .write()
            .worker_map
            .intents
            .upsert(&entry.host, entry.intent);
        Ok(())
    }

    fn trash_purge(&self, entry: TrashPurgeEntry) -> CommonResult<()> {
        debug!(
            "TrashPurge: entry={} paths={}",
            entry.entry,
            entry.paths.len()
        );

        let mut fs_dir = self.fs_dir.write();
        let res = fs_dir.purge_paths(&entry.paths, entry.mtime)?;
        debug!(
            "TrashPurge: entry={} removed inodes={} bytes={}",
            entry.entry, res.inodes, res.bytes
        );
        Ok(())
    }

    fn cache_invalidation(&self, entry: CacheInvalidationEntry) -> CommonResult<()> {
        let fs_dir = self.fs_dir.write();
        fs_dir.store.apply_cache_invalidations(entry.inodes)
    }

    fn mkdir(&self, entry: MkdirEntry) -> CommonResult<()> {
        let mut fs_dir = self.fs_dir.write();
        let inp = InodePath::resolve(fs_dir.root_ptr(), entry.path, &fs_dir.store)?;
        let name = inp.name().to_string();
        let _ = fs_dir.add_last_inode(inp, InodeView::new_dir(name, entry.dir))?;
        Ok(())
    }

    fn create_file(&self, entry: CreateFileEntry) -> CommonResult<()> {
        let mut fs_dir = self.fs_dir.write();
        let inp = InodePath::resolve(fs_dir.root_ptr(), &entry.path, &fs_dir.store)?;

        if inp.is_full() {
            warn!("create_file: file already exists: {:?}", entry);
            return Ok(());
        }
        let name = inp.name().to_string();
        let _ = fs_dir.add_last_inode(inp, InodeView::new_file(name, entry.file))?;
        Ok(())
    }

    fn reopen_file(&self, entry: ReopenFileEntry) -> CommonResult<()> {
        let fs_dir = self.fs_dir.write();
        let inp = InodePath::resolve(fs_dir.root_ptr(), &entry.path, &fs_dir.store)?;

        let mut inode = match inp.get_last_inode() {
            Some(v) => v,
            None => {
                warn!("reopen_file: file not found: {:?}", entry);
                return Ok(());
            }
        };
        let file = inode.as_file_mut()?;
        let _ = mem::replace(file, entry.file);

        fs_dir.store.apply_reopen_file(inode.as_ref())?;

        Ok(())
    }

    fn overwrite_file(&self, entry: OverWriteFileEntry) -> CommonResult<()> {
        let fs_dir = self.fs_dir.write();
        let inp = InodePath::resolve(fs_dir.root_ptr(), &entry.path, &fs_dir.store)?;

        let mut inode = match inp.get_last_inode() {
            Some(v) => v,
            None => {
                warn!("overwrite_file: file not found: {:?}", entry);
                return Ok(());
            }
        };
        let file = inode.as_file_mut()?;
        let _ = mem::replace(file, entry.file);

        fs_dir.store.apply_overwrite_file(inode.as_ref())?;

        Ok(())
    }

    fn add_block(&self, entry: AddBlockEntry) -> CommonResult<()> {
        let fs_dir = self.fs_dir.write();

        let inode_id = entry.blocks.first().map(|v| InodeId::get_id(v.id));

        let mut inode = match MasterFilesystem::resolve_file_inode(&fs_dir, &entry.path, inode_id) {
            Ok(v) => v,
            Err(e) => {
                warn!("add_block: file not found: {:?} {}", entry, e);
                return Ok(());
            }
        };
        let file = inode.as_file_mut()?;
        let _ = mem::replace(&mut file.blocks, entry.blocks);
        fs_dir
            .store
            .apply_new_block(inode.as_ref(), &entry.commit_block)?;

        Ok(())
    }

    fn complete_file(&self, entry: CompleteFileEntry) -> CommonResult<()> {
        let fs_dir = self.fs_dir.write();

        let mut inode =
            match MasterFilesystem::resolve_file_inode(&fs_dir, &entry.path, Some(entry.file.id)) {
                Ok(v) => v,
                Err(e) => {
                    warn!("complete_file: file not found: {:?} {}", entry, e);
                    return Ok(());
                }
            };
        let file = inode.as_file_mut()?;

        let _ = mem::replace(file, entry.file);
        // Update block location
        fs_dir
            .store
            .apply_complete_file(inode.as_ref(), &entry.commit_blocks)?;

        Ok(())
    }
    pub fn rename(&self, entry: RenameEntry) -> CommonResult<()> {
        let mut fs_dir = self.fs_dir.write();
        let entry_src = entry.src;
        let src_inp = InodePath::resolve(fs_dir.root_ptr(), &entry_src, &fs_dir.store)?;
        let dst_inp = InodePath::resolve(fs_dir.root_ptr(), entry.dst, &fs_dir.store)?;
        if src_inp.get_last_inode().is_none() {
            warn!("Rename: source path not found: {}", entry_src);
            return Ok(());
        }
        fs_dir.unprotected_rename(
            &src_inp,
            &dst_inp,
            entry.mtime,
            RenameFlags::new(entry.flags),
            if RenameFlags::new(entry.flags).exchange_mode()
                && entry.src_inode_id != 0
                && entry.dst_inode_id != 0
            {
                Some((entry.src_inode_id, entry.dst_inode_id))
            } else {
                None
            },
        )?;

        Ok(())
    }

    pub fn delete(&self, entry: DeleteEntry) -> CommonResult<()> {
        let mut fs_dir = self.fs_dir.write();
        let entry_path = entry.path;
        let inp = InodePath::resolve(fs_dir.root_ptr(), &entry_path, &fs_dir.store)?;
        if inp.get_last_inode().is_none() {
            warn!("Delete: path not found: {}", entry_path);
            return Ok(());
        }
        fs_dir.unprotected_delete(&inp, entry.mtime)?;
        Ok(())
    }

    pub fn free(&self, entry: FreeEntry) -> CommonResult<()> {
        let mut fs_dir = self.fs_dir.write();
        let inp = InodePath::resolve(fs_dir.root_ptr(), &entry.path, &fs_dir.store)?;
        let Some(inode) = inp.get_last_inode() else {
            warn!("Free: path not found: {:?}", entry);
            return Ok(());
        };
        fs_dir.unprotected_free(inode, entry.mtime, entry.recursive)?;
        Ok(())
    }

    pub fn mount(&self, entry: MountEntry) -> CommonResult<()> {
        let version = self.mnt_mgr.unprotected_add_mount(entry.info.clone())?;

        let mut fs_dir = self.fs_dir.write();
        fs_dir.unprotected_store_mount(entry.info)?;
        fs_dir.save_mount_table_version(version)?;
        Ok(())
    }

    pub fn unmount(&self, entry: UnMountEntry) -> CommonResult<()> {
        if !self.mnt_mgr.has_mounted(entry.id)? {
            warn!("Unmount: id already unmounted: {:?}", entry);
            return Ok(());
        }
        let version = self.mnt_mgr.unprotected_umount_by_id(entry.id)?;
        let mut fs_dir = self.fs_dir.write();
        fs_dir.unprotected_unmount(entry.id)?;
        fs_dir.save_mount_table_version(version)?;
        Ok(())
    }

    pub fn set_attr(&self, entry: SetAttrEntry) -> CommonResult<()> {
        let mut fs_dir = self.fs_dir.write();
        let inp = InodePath::resolve(fs_dir.root_ptr(), &entry.path, &fs_dir.store)?;
        let last_inode = match inp.get_last_inode() {
            Some(v) => v,
            None => {
                warn!("SetAttr: path not found: {:?}", entry);
                return Ok(());
            }
        };

        fs_dir.unprotected_set_attr(last_inode, entry.opts)?;
        Ok(())
    }

    pub fn symlink(&self, entry: SymlinkEntry) -> CommonResult<()> {
        let link_path = entry.link;
        let mut fs_dir = self.fs_dir.write();
        let inp = InodePath::resolve(fs_dir.root_ptr(), &link_path, &fs_dir.store)?;
        match fs_dir.unprotected_symlink(inp, entry.new_inode, entry.force) {
            Ok(_) => Ok(()),
            Err(FsError::FileAlreadyExists(_)) => {
                warn!("Symlink: file already exists: {:?}", link_path);
                Ok(())
            }
            Err(e) => Err(e.into()),
        }
    }

    pub fn link(&self, entry: LinkEntry) -> CommonResult<()> {
        let mut fs_dir = self.fs_dir.write();
        let old_path = InodePath::resolve(fs_dir.root_ptr(), &entry.src_path, &fs_dir.store)?;
        let new_path = InodePath::resolve(fs_dir.root_ptr(), &entry.dst_path, &fs_dir.store)?;

        // Get the original inode ID
        let original_inode_id = match old_path.get_last_inode() {
            Some(inode) => inode.id(),
            None => {
                warn!("Link: source path not found: {:?}", entry);
                return Ok(());
            }
        };

        if let Some(mut inode_ptr) = old_path.get_last_inode() {
            if let File(_) = inode_ptr.as_mut() {
                inode_ptr.incr_nlink(entry.mtime);
            }
        }

        match fs_dir.unprotected_link(new_path, original_inode_id, entry.mtime as u64) {
            Ok(_) => Ok(()),
            Err(FsError::FileAlreadyExists(_)) => {
                warn!("Link: dst_path already exists: {:?}", entry);
                Ok(())
            }
            Err(e) => Err(e.into()),
        }
    }

    pub fn set_locks(&self, entry: SetLocksEntry) -> CommonResult<()> {
        let fs_dir = self.fs_dir.write();
        fs_dir.store.apply_set_locks(entry.ino, &entry.locks)
    }

    pub fn ufs_applied(&self, entry: UfsAppliedEntry) -> CommonResult<()> {
        let mut lock = self.fsm_state()?;
        lock.ufs_applied = AppliedIndex {
            op_id: entry.op_id,
            rpc_id: entry.rpc_id,
            term: entry.term,
            index: entry.index,
        };
        Ok(())
    }

    // Clean up expired checkpoints.
    pub fn purge_checkpoint(&self, current_ck: impl AsRef<str>) -> CommonResult<()> {
        let current_ck = current_ck.as_ref();
        let ck_dir = match Path::new(current_ck).parent() {
            None => return Ok(()),
            Some(v) => v,
        };

        let current_mtime = match Path::new(current_ck).metadata() {
            Ok(meta) => FileUtils::mtime(&meta)?,
            Err(_) => return Ok(()),
        };

        let mut vec = vec![];
        for entry in fs::read_dir(ck_dir)? {
            let entry = entry?;
            let meta = entry.metadata()?;
            let mtime = FileUtils::mtime(&meta)?;
            if mtime < current_mtime {
                vec.push((mtime, entry.path()));
            }
        }

        // Sort oldest-first and keep at most (retain_checkpoint_num - 1) older
        // checkpoints so that together with current_ck the total is retain_checkpoint_num.
        vec.sort_by_key(|x| x.0);
        let keep = self.retain_checkpoint_num.saturating_sub(1);
        let del_num = vec.len().saturating_sub(keep);

        for (_, path) in vec.iter().take(del_num) {
            let path = path.as_path();
            FileUtils::delete_path(path, true)?;
            info!("delete expired checkpoint: {}", path.to_string_lossy());
        }

        Ok(())
    }

    pub async fn shutdown(&self) -> RaftResult<()> {
        let (tx, rx) = CallChannel::channel();
        self.sender.send(ApplyMsg::Shutdown(tx)).await?;
        rx.receive().await?;
        Ok(())
    }

    async fn apply_direct(&self, msg: ApplyMsg) -> RaftResult<()> {
        let result = self.apply_msg(false, &msg, false).await.map_err(Into::into);

        if matches!(&msg, ApplyMsg::EntryWithAck(_)) {
            Self::complete_entry_ack(msg, result);
            Ok(())
        } else {
            result
        }
    }
}

impl AppStorage for JournalLoader {
    async fn apply(&self, wait: bool, msg: ApplyMsg) -> RaftResult<()> {
        if !self.has_apply_worker {
            return self.apply_direct(msg).await;
        }

        if matches!(&msg, ApplyMsg::EntryWithAck(_)) {
            self.sender.send(msg).await?;
            return Ok(());
        }

        if wait {
            return self.apply_direct(msg).await;
        }

        self.sender.send(msg).await?;
        Ok(())
    }

    fn get_fsm_state(&self) -> FsmState {
        match self.fsm_state.lock() {
            Ok(state) => state.clone(),
            Err(e) => {
                error!("fatal fsm_state lock poisoned: {}", e);
                std::process::abort();
            }
        }
    }

    fn hard_state_changed(&self, state: &raft::eraftpb::HardState) {
        self.metrics.journal_committed.set(state.commit as i64);
        self.metrics.journal_term.set(state.term as i64);
        // Every persisted term moves the lease epoch forward; a promotion always
        // carries a higher term than the leadership it replaces.
        self.term.store(state.term, Ordering::Release);
    }

    async fn role_change(&self, role: StateRole) -> RaftResult<()> {
        if !self.has_apply_worker {
            if role == StateRole::Leader {
                self.catch_up_committed_metadata().await?;
            }
            return Ok(());
        }
        let (tx, rx) = CallChannel::channel();
        self.sender.send(ApplyMsg::RoleChange((role, tx))).await?;
        rx.receive().await?
    }

    async fn create_snapshot(&self) -> RaftResult<SnapshotData> {
        if !self.has_apply_worker {
            return self.create_snapshot0(None);
        }
        let (tx, rx) = CallChannel::channel();
        let msg = ApplyMsg::CreateSnapshot(tx);

        self.sender.send(msg).await?;
        rx.receive().await?
    }

    async fn apply_snapshot(&self, snapshot: SnapshotData) -> RaftResult<()> {
        if !self.has_apply_worker {
            return self.apply_snapshot0(snapshot);
        }
        let (tx, rx) = CallChannel::channel();
        let msg = ApplyMsg::ApplySnapshot((tx, snapshot));

        self.sender.send(msg).await?;
        rx.receive().await?
    }

    fn snapshot_dir(&self, snapshot_id: u64) -> RaftResult<String> {
        let fs_dir = self.fs_dir.read();
        Ok(fs_dir.get_checkpoint_path(snapshot_id))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use curvine_config::{ClusterConf, MasterConf};
    use curvine_error::FsResult;
    use curvine_runtime::common::Utils;

    // Regression tests for the batch-deserialization skip branch in `apply0`:
    // with `ignore_reply_error` enabled an undecodable batch (typically written
    // by a newer-schema leader during a rolling upgrade) is skipped and the
    // apply watermark advances past it; with the flag disabled the replay error
    // is propagated and the watermark must not move.
    fn ignore_reply_error_conf(
        name: &str,
        ignore_reply_error: bool,
    ) -> (ClusterConf, std::path::PathBuf) {
        let root = std::env::temp_dir().join(format!(
            "curvine-journal-loader-{}-{}",
            name,
            Utils::rand_id()
        ));
        let conf = ClusterConf {
            testing: true,
            format_master: true,
            master: MasterConf {
                meta_dir: root.join("meta").to_str().unwrap().into(),
                ..Default::default()
            },
            journal: JournalConf {
                journal_dir: root.join("journal").to_str().unwrap().into(),
                io_threads: 1,
                worker_threads: 2,
                ignore_reply_error,
                ..JournalConf::with_test()
            },
            ..Default::default()
        };
        (conf, root)
    }

    // Not a valid JournalBatchProto payload.
    fn corrupt_entry(index: u64, term: u64) -> Entry {
        Entry {
            index,
            term,
            data: b"corrupt".to_vec(),
            ..Default::default()
        }
    }

    #[test]
    fn trash_purge_entry_removes_its_paths_idempotently() -> FsResult<()> {
        let (conf, root) = ignore_reply_error_conf("trash-purge", true);
        let js = JournalSystem::from_conf(&conf)?;
        let fs = js.fs();
        let loader = js.journal_loader();

        fs.create("/victim/a/file-a", true)?;
        fs.create("/victim/a/file-b", true)?;
        fs.create("/victim/b/file-c", true)?;

        let batch = JournalEntry::TrashPurge(TrashPurgeEntry {
            op_id: 77,
            rpc_id: 0,
            entry: "/victim".to_string(),
            mtime: 5,
            paths: vec![
                "/victim/a/file-a".to_string(),
                "/victim/a/file-b".to_string(),
            ],
        });

        loader.apply_entry(batch.clone())?;
        assert!(!fs.exists("/victim/a/file-a")?);
        assert!(!fs.exists("/victim/a/file-b")?);
        // Unlisted siblings and the entry root survive the batch.
        assert!(fs.exists("/victim/b/file-c")?);
        assert!(fs.exists("/victim")?);

        // A replayed batch is a no-op instead of failing on removed paths.
        loader.apply_entry(batch)?;
        assert!(fs.exists("/victim/b/file-c")?);

        drop(loader);
        drop(fs);
        js.shutdown();
        let _ = fs::remove_dir_all(&root);
        Ok(())
    }

    fn valid_entry(index: u64, term: u64) -> Entry {
        let mut batch = JournalBatch::new(1);
        batch.push(JournalEntry::CacheInvalidation(CacheInvalidationEntry {
            op_id: 41,
            rpc_id: 9,
            inodes: vec![],
        }));
        let data = batch.encode().unwrap();

        Entry {
            index,
            term,
            data,
            ..Default::default()
        }
    }

    #[test]
    fn follower_skips_corrupt_batch_and_keeps_replaying() -> FsResult<()> {
        let (conf, root) = ignore_reply_error_conf("follower", true);
        let js = JournalSystem::from_conf(&conf)?;
        let loader = js.journal_loader();
        let rt = conf.journal.create_runtime();

        rt.block_on(loader.apply(true, ApplyMsg::new_entry(corrupt_entry(1, 5))))?;
        let state = loader.get_fsm_state();
        assert_eq!(
            state.applied.index, 1,
            "skipped batch must advance the follower watermark"
        );
        assert_eq!(state.applied.term, 5);
        assert_eq!(
            state.ufs_applied.index, 0,
            "follower must not advance the UFS watermark"
        );

        rt.block_on(loader.apply(true, ApplyMsg::new_entry(valid_entry(2, 5))))?;
        let state = loader.get_fsm_state();
        assert_eq!(state.applied.index, 2, "next valid entry must still apply");
        assert_eq!(state.applied.op_id, 41);
        assert_eq!(state.applied.rpc_id, 9);

        drop(loader);
        js.shutdown();
        let _ = fs::remove_dir_all(&root);
        Ok(())
    }

    #[test]
    fn leader_advances_ufs_watermark_when_skipping_corrupt_batch() -> FsResult<()> {
        let (conf, root) = ignore_reply_error_conf("leader", true);
        let js = JournalSystem::from_conf(&conf)?;
        let loader = js.journal_loader();
        let rt = conf.journal.create_runtime();

        for index in 1..=2 {
            rt.block_on(loader.apply0(true, &corrupt_entry(index, 7), false))?;
        }
        let state = loader.get_fsm_state();
        assert_eq!(
            state.ufs_applied.index, 2,
            "skipped batch must advance the leader UFS watermark"
        );
        assert_eq!(state.ufs_applied.term, 7);
        assert_eq!(state.applied.index, 2, "leader advances both watermarks");
        assert!(
            loader.recovery.ufs_caught_up(),
            "a synchronous UFS apply leaves nothing pending"
        );
        assert_eq!(loader.recovery.pending(), 0);

        drop(loader);
        js.shutdown();
        let _ = fs::remove_dir_all(&root);
        Ok(())
    }

    #[test]
    fn corrupt_batch_fails_replay_without_ignore_reply_error() -> FsResult<()> {
        let (conf, root) = ignore_reply_error_conf("strict", false);
        let js = JournalSystem::from_conf(&conf)?;
        let loader = js.journal_loader();
        let rt = conf.journal.create_runtime();

        let error = rt
            .block_on(loader.apply(true, ApplyMsg::new_entry(corrupt_entry(1, 5))))
            .expect_err("corrupt batch must fail replay when ignore_reply_error is false");
        assert!(
            error.to_string().contains("failed to decode journal batch"),
            "unexpected error: {}",
            error
        );
        assert_eq!(
            loader.get_fsm_state().applied.index,
            0,
            "failed replay must not move the watermark"
        );

        drop(loader);
        js.shutdown();
        let _ = fs::remove_dir_all(&root);
        Ok(())
    }
}
