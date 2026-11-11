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

//! Block 位置元数据的唯一写入口（CV-70 方案 B 的抽象层）。
//!
//! ## 背景
//!
//! 块位置的增删历史上散落在五条路径里，全部只写 leader 本地 RocksDB、不进
//! raft journal，这是切主后位置视图回退（CoMind CV-70）的根因：
//!
//! | 迁移事件 | 原直写路径 |
//! | --- | --- |
//! | 复制完成 | `MasterReplicationManager::commit_replication_location` → `FsDir::add_replication_location_if_needed` |
//! | drain 搬迁 | `MasterFilesystem::commit_drain_move` → `FsDir::move_block_location` |
//! | 失联 / DECOMMISSIONED 清理 | `delete_locations_unchecked` → `FsDir::delete_locations` |
//! | 增量上报对账 | `apply_block_report_batch_locked` → `FsDir::block_report` |
//! | 全量上报对账 | `reconcile_full_block_report` → 同上 |
//!
//! ## 抽象约定（本模块即边界，不允许绕路）
//!
//! 1. 所有位置写入必须经 `MasterFilesystem::commit_block_location_delta`
//!    汇聚；语义化的公开入口（`commit_replica_added` 等）只是它的类型安全封装。
//! 2. journal 落点：方案 B 落地时，journal send 将在
//!    `commit_block_location_delta` 内与 `fs_dir` apply 处于**同一临界区**，
//!    保证 leader 本地应用顺序与 follower 回放顺序一致；`BlockLocationDelta`
//!    即未来 journal 条目（`BlockLocationDeltaEntry` / `WorkerLocationPurgeEntry`）
//!    的 payload 形状。
//! 3. 条带锁（`worker_location_lock`）由入口的调用方持有，本模块不重复加锁
//!    （`std::sync::Mutex` 不可重入）。

use crate::master::fs::master_filesystem::MasterFilesystem;
use curvine_error::FsResult;
use curvine_model::BlockLocation;
use log::debug;

/// 一次块位置变更的语义描述，同时是方案 B journal 条目的 payload 形状。
#[derive(Debug, Clone)]
pub(crate) enum BlockLocationDelta {
    /// 复制完成：target worker 补上一个副本（单块 add）。
    ReplicaAdded {
        block_id: i64,
        location: BlockLocation,
    },
    /// drain 搬迁：新位置先写入、旧位置再摘除，同一批次原子完成，
    /// 元数据在任何时刻都不会比搬迁前少副本。
    DrainMoved {
        block_id: i64,
        add: BlockLocation,
        from_worker: u32,
    },
    /// 失联 / 强制完成 / DECOMMISSIONED 清理：摘除某 worker 的全部位置。
    WorkerPurged { worker_id: u32 },
    /// 上报对账：worker 心跳 / 全量上报的增量 add/del 批（判定后的 accepted
    /// 列表；超副本数等判定在本入口的同一临界区内完成）。
    ReportBatch {
        worker_id: u32,
        batch: Vec<(bool, i64, BlockLocation)>,
        limit_replicas: bool,
    },
}

/// [`BlockLocationDelta`] 应用后真实生效的结果。
#[derive(Debug)]
pub(crate) enum LocationDeltaOutcome {
    /// true = 实际新增；false = 幂等拒绝（块不存在 / 位置已存在 / 已达副本数）。
    ReplicaAdded(bool),
    /// true = 完成搬迁；false = 幂等拒绝（块不存在 / 源位置已不在）。
    DrainMoved(bool),
    /// 摘除的 block id 列表（调用方据此触发欠副本修复与缓存失效）。
    Purged(Vec<i64>),
    /// 请求 worker 删除多余字节数据的 block id 列表（不涉及位置元数据）。
    ReportBatch { delete_blocks: Vec<i64> },
}

impl MasterFilesystem {
    /// ★ 唯一写入口：所有块位置元数据变更在此汇聚。
    ///
    /// CV-70 方案 B 落地时，journal send 在此与 `fs_dir` apply 同一临界区完成。
    pub(crate) fn commit_block_location_delta(
        &self,
        delta: BlockLocationDelta,
    ) -> FsResult<LocationDeltaOutcome> {
        match delta {
            BlockLocationDelta::ReplicaAdded { block_id, location } => self
                .apply_replica_added(block_id, location)
                .map(LocationDeltaOutcome::ReplicaAdded),
            BlockLocationDelta::DrainMoved {
                block_id,
                add,
                from_worker,
            } => self
                .apply_drain_moved(block_id, add, from_worker)
                .map(LocationDeltaOutcome::DrainMoved),
            BlockLocationDelta::WorkerPurged { worker_id } => self
                .apply_worker_purged(worker_id)
                .map(LocationDeltaOutcome::Purged),
            BlockLocationDelta::ReportBatch {
                worker_id,
                batch,
                limit_replicas,
            } => self
                .apply_report_batch(worker_id, batch, limit_replicas)
                .map(|delete_blocks| LocationDeltaOutcome::ReportBatch { delete_blocks }),
        }
    }

    /// 复制完成提交（调用方：`MasterReplicationManager::finish_replicated_block`）。
    pub(crate) fn commit_replica_added(
        &self,
        block_id: i64,
        location: BlockLocation,
    ) -> FsResult<bool> {
        match self
            .commit_block_location_delta(BlockLocationDelta::ReplicaAdded { block_id, location })?
        {
            LocationDeltaOutcome::ReplicaAdded(added) => Ok(added),
            _ => unreachable!("ReplicaAdded delta cannot yield another outcome"),
        }
    }

    /// drain 搬迁提交（调用方：`MasterFilesystem::commit_drain_move`）。
    pub(crate) fn commit_drain_move_location(
        &self,
        block_id: i64,
        add: BlockLocation,
        from_worker: u32,
    ) -> FsResult<bool> {
        match self.commit_block_location_delta(BlockLocationDelta::DrainMoved {
            block_id,
            add,
            from_worker,
        })? {
            LocationDeltaOutcome::DrainMoved(moved) => Ok(moved),
            _ => unreachable!("DrainMoved delta cannot yield another outcome"),
        }
    }

    /// 摘除某 worker 的全部位置（调用方：`delete_locations_unchecked`）。
    pub(crate) fn purge_worker_locations(&self, worker_id: u32) -> FsResult<Vec<i64>> {
        match self.commit_block_location_delta(BlockLocationDelta::WorkerPurged { worker_id })? {
            LocationDeltaOutcome::Purged(removed) => Ok(removed),
            _ => unreachable!("WorkerPurged delta cannot yield another outcome"),
        }
    }

    /// 上报对账（调用方：`block_report_locked`、`reconcile_full_block_report`，
    /// 二者均已持有 worker 条带锁）。返回请求 worker 删除的 block id 列表。
    pub(crate) fn apply_report_locations(
        &self,
        worker_id: u32,
        batch: Vec<(bool, i64, BlockLocation)>,
        limit_replicas: bool,
    ) -> FsResult<Vec<i64>> {
        match self.commit_block_location_delta(BlockLocationDelta::ReportBatch {
            worker_id,
            batch,
            limit_replicas,
        })? {
            LocationDeltaOutcome::ReportBatch { delete_blocks } => Ok(delete_blocks),
            _ => unreachable!("ReportBatch delta cannot yield another outcome"),
        }
    }

    fn apply_replica_added(&self, block_id: i64, location: BlockLocation) -> FsResult<bool> {
        self.fs_dir
            .write()
            .add_replication_location_if_needed(block_id, location)
    }

    fn apply_drain_moved(
        &self,
        block_id: i64,
        add: BlockLocation,
        from_worker: u32,
    ) -> FsResult<bool> {
        self.fs_dir
            .write()
            .move_block_location(block_id, add, from_worker)
    }

    fn apply_worker_purged(&self, worker_id: u32) -> FsResult<Vec<i64>> {
        self.fs_dir.write().delete_locations(worker_id)
    }

    /// 上报对账的判定 + 应用：每个 chunk 在同一把 `fs_dir` 写锁内完成
    /// 条件判定（块是否存在 / 位置是否已记录 / 是否已达副本数）与批量 apply，
    /// 判定为“超副本 / 无 inode”的块进入 `delete_blocks`（请求 worker 删字节，
    /// 不触碰位置元数据）。
    fn apply_report_batch(
        &self,
        worker_id: u32,
        batch: Vec<(bool, i64, BlockLocation)>,
        limit_replicas: bool,
    ) -> FsResult<Vec<i64>> {
        let mut delete_blocks = Vec::new();
        let mut iter = batch.into_iter();
        loop {
            let chunk: Vec<_> = iter.by_ref().take(Self::BLOCK_REPORT_WRITE_CHUNK).collect();
            if chunk.is_empty() {
                break;
            }

            let mut fs_dir = self.fs_dir.write();
            let mut accepted = Vec::with_capacity(chunk.len());
            for (add, block_id, location) in chunk {
                if !add {
                    accepted.push((false, block_id, location));
                    continue;
                }

                let Some(state) = fs_dir.replication_block_state(block_id)? else {
                    delete_blocks.push(block_id);
                    continue;
                };
                if state
                    .locations
                    .iter()
                    .any(|current| current.worker_id == worker_id)
                {
                    debug!(
                        "block report: block {} already recorded on worker {}",
                        block_id, worker_id
                    );
                    continue;
                }
                if limit_replicas && state.locations.len() >= state.replicas as usize {
                    debug!(
                        "block report: block {} ask worker {} to delete, already at {} of {} replicas ({:?})",
                        block_id,
                        worker_id,
                        state.locations.len(),
                        state.replicas,
                        state.locations
                    );
                    delete_blocks.push(block_id);
                    continue;
                }
                debug!(
                    "block report: add block {} location for worker {} ({:?}, replicas {})",
                    block_id, worker_id, state.locations, state.replicas
                );
                accepted.push((true, block_id, location));
            }
            fs_dir.block_report(accepted)?;
        }
        Ok(delete_blocks)
    }
}
