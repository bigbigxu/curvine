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

//! Worker 状态机核心（设计见 `docs/design/worker-state-management.md`）。
//!
//! 契约：
//!
//! 1. 状态 = `(AdminState, Liveness)`。`AdminState` 来自运维意图（持久化），
//!    `Liveness` 由心跳时间戳派生（不持久化）。**心跳触发永远不修改 `AdminState`**，
//!    所以节点失联恢复后自动回到原来的运维身份，不需要额外的 `resume` 字段。
//! 2. 只有一个入口 [`apply`]：纯函数，不碰 IO、不加锁、不是 async。所有副作用以
//!    [`Effect`] 列表返回，由调用方执行。
//! 3. 守卫失败返回具体的 [`Refusal`]，用于审计和页面展示"为什么不能这么做"。
//!
//! 术语来源见设计文档 §3（CORDONED / DECOMMISSIONING / DECOMMISSIONED / STALE / LOST
//! 分别对应 Kubernetes、HDFS/YARN/TiKV、HDFS 的既有词汇）。

use serde::{Deserialize, Serialize};

/// 心跳派生的事实。不持久化：master 重启或主备切换后由时间戳重算。
#[repr(u8)]
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, PartialOrd, Ord, Default,
)]
pub enum Liveness {
    /// 心跳正常。
    #[default]
    Running,
    /// 心跳迟到（超过 `stale_interval`）。位置保留，读降级，搬迁暂停。
    Stale,
    /// 心跳丢失（超过 `lost_interval`）。位置作废，触发补副本。
    Lost,
    /// 进程主动退出（`HeartbeatStatus::End`）。宽限期内保留位置。
    Shutdown,
}

impl Liveness {
    pub fn as_str_name(&self) -> &'static str {
        match self {
            Liveness::Running => "running",
            Liveness::Stale => "stale",
            Liveness::Lost => "lost",
            Liveness::Shutdown => "shutdown",
        }
    }

    /// Inverse of [`Liveness::as_str_name`], used when reading the wire format.
    pub fn from_str_name(name: &str) -> Option<Liveness> {
        [
            Liveness::Running,
            Liveness::Stale,
            Liveness::Lost,
            Liveness::Shutdown,
        ]
        .into_iter()
        .find(|state| state.as_str_name() == name)
    }
}

/// 面向人的单一 worker 状态。
///
/// 一个 worker 在任一时刻恰好处于这 7 个值之一。它由 [`WorkerState::summary`]
/// 从两轴归并而来：心跳异常（`LOST` / `SHUTDOWN` / `STALE`）优先于运维意图，
/// 心跳正常时才看各管理状态。控制台、`cv node list` 和日志都展示这个值；
/// 需要区分「意图」与「心跳」的场合（主备、搬迁、审计）仍然读两条轴。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum WorkerSummary {
    /// `NORMAL + RUNNING`：正常服务。
    Normal,
    /// `CORDONED + RUNNING`：只读（不再分配新数据，已有数据可读）。
    ReadOnly,
    /// `DECOMMISSIONING + RUNNING`：搬空中。
    Decommissioning,
    /// `DECOMMISSIONED + RUNNING`：已下线。
    Decommissioned,
    /// `STALE`：心跳迟到，位置保留、读降级、搬迁暂停。
    Stale,
    /// `LOST`：心跳丢失，位置作废、触发补副本。
    Lost,
    /// `SHUTDOWN`：进程主动退出，宽限期内保留位置。
    Shutdown,
}

impl WorkerSummary {
    /// 全部 7 个取值，按展示顺序（从需要人关注到健康）。
    pub const ALL: [WorkerSummary; 7] = [
        WorkerSummary::Lost,
        WorkerSummary::Shutdown,
        WorkerSummary::Stale,
        WorkerSummary::Decommissioning,
        WorkerSummary::Decommissioned,
        WorkerSummary::ReadOnly,
        WorkerSummary::Normal,
    ];

    pub fn as_str_name(&self) -> &'static str {
        match self {
            WorkerSummary::Normal => "normal",
            WorkerSummary::ReadOnly => "readonly",
            WorkerSummary::Decommissioning => "decommissioning",
            WorkerSummary::Decommissioned => "decommissioned",
            WorkerSummary::Stale => "stale",
            WorkerSummary::Lost => "lost",
            WorkerSummary::Shutdown => "shutdown",
        }
    }

    /// Inverse of [`WorkerSummary::as_str_name`], used when reading the wire format.
    pub fn from_str_name(name: &str) -> Option<WorkerSummary> {
        WorkerSummary::ALL
            .into_iter()
            .find(|value| value.as_str_name() == name)
    }

    /// 这个状态是否意味着心跳异常（需要人排查节点本身）。
    pub fn is_liveness_failure(&self) -> bool {
        matches!(
            self,
            WorkerSummary::Stale | WorkerSummary::Lost | WorkerSummary::Shutdown
        )
    }
}

/// 运维意图。持久化在 intent 表（journal），跨重启、跨主备切换保持。
///
/// 心跳永远不会改变它；只有运维触发和 drain 完成会改变它。
#[repr(u8)]
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, PartialOrd, Ord, Default,
)]
pub enum AdminState {
    /// 正常服务。
    #[default]
    Normal,
    /// 只出不进：不再分配新数据，已有数据不动、不搬、可读。
    Cordoned,
    /// 搬空中：不再分配新数据，数据往别处搬，可读、可作复制源。
    Decommissioning,
    /// 已下线：不再分配、不可读、不参与副本计数；盘上数据保留。
    Decommissioned,
}

impl AdminState {
    pub fn as_str_name(&self) -> &'static str {
        match self {
            AdminState::Normal => "normal",
            AdminState::Cordoned => "cordoned",
            AdminState::Decommissioning => "decommissioning",
            AdminState::Decommissioned => "decommissioned",
        }
    }
}

/// 一次状态转移。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Trigger {
    /// 运维：只出不进。
    Cordon,
    /// 运维：取消只出不进。
    Uncordon,
    /// 运维：开始搬空。
    Decommission,
    /// 运维：取消搬空（已搬走的不回滚）。
    CancelDecommission,
    /// 系统：块数归零。
    DrainCompleted,
    /// 运维：超时强制完成。
    ForceDecommission,
    /// 运维：接回集群（同时清掉意图记录）。
    Recommission,
    /// 心跳：恢复。
    HeartbeatOk,
    /// 心跳：超过 `stale_interval`。
    StaleTimeout,
    /// 心跳：超过 `lost_interval`。
    LostTimeout,
    /// 心跳：进程主动退出。
    GracefulShutdown,
    /// 心跳：退出宽限期到期。
    ShutdownGraceExpired,
}

impl Trigger {
    /// 是否由心跳/时间派生，而非运维动作。用于断言"事实不影响意图"。
    pub fn is_heartbeat_driven(&self) -> bool {
        matches!(
            self,
            Trigger::HeartbeatOk
                | Trigger::StaleTimeout
                | Trigger::LostTimeout
                | Trigger::GracefulShutdown
                | Trigger::ShutdownGraceExpired
        )
    }
}

/// 拒绝原因。生产路径要把这些原因带回给运维，而不是只写一条日志。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Refusal {
    /// 已经处于该状态。
    AlreadyInState,
    /// 当前状态不允许该操作。
    NotAllowed,
    /// 还有块没搬完，不能判定完成。
    BlocksRemain { blocks: u64 },
    /// 还没到 drain 超时，不允许强制完成。
    DeadlineNotReached,
}

impl Refusal {
    pub fn as_str_name(&self) -> &'static str {
        match self {
            Refusal::AlreadyInState => "already in state",
            Refusal::NotAllowed => "not allowed in this state",
            Refusal::BlocksRemain { .. } => "blocks remain",
            Refusal::DeadlineNotReached => "drain deadline not reached",
        }
    }
}

impl std::fmt::Display for Refusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Refusal::BlocksRemain { blocks } => {
                write!(
                    f,
                    "{}: {} block(s) still located here",
                    self.as_str_name(),
                    blocks
                )
            }
            _ => f.write_str(self.as_str_name()),
        }
    }
}

impl std::error::Error for Refusal {}

/// 转移产生的副作用。状态机只返回清单，由调用方执行。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Effect {
    /// 写入意图记录。
    PersistIntent(AdminState),
    /// 删除意图记录（回到 Normal，不再有运维约束）。
    ClearIntent,
    /// 启动搬迁循环。
    StartDrain,
    /// 停止搬迁循环。
    StopDrain,
    /// 摘除该 worker 的全部块位置。
    DropLocations,
    /// 对因此副本不足的块发起复制。
    TriggerReplication,
    /// 记录被强制摘除的孤儿块清单。
    RecordOrphans,
    /// 进入退出宽限期：位置保留，不触发复制。
    StartShutdownGrace,
}

/// 守卫依赖的外部事实。由调用方（A6 / 心跳检查器）在每次转移前计算。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Facts {
    /// 该 worker 当前在元数据里的块数。
    pub blocks: u64,
    /// 是否已过 `drain_deadline`。
    pub deadline_passed: bool,
}

impl Facts {
    pub fn with_blocks(blocks: u64) -> Self {
        Self {
            blocks,
            ..Default::default()
        }
    }
}

/// 节点状态。字段私有，模块外无法直接赋值，只能经由 [`apply`] 改变。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct WorkerState {
    admin: AdminState,
    liveness: Liveness,
}

impl WorkerState {
    pub fn new(admin: AdminState, liveness: Liveness) -> Self {
        Self { admin, liveness }
    }

    /// 新注册节点的初始状态：意图表有记录按记录，否则 `Normal`。
    pub fn registered(admin: AdminState) -> Self {
        Self::new(admin, Liveness::Running)
    }

    pub fn admin(&self) -> AdminState {
        self.admin
    }

    pub fn liveness(&self) -> Liveness {
        self.liveness
    }

    fn with_admin(self, admin: AdminState) -> Self {
        Self { admin, ..self }
    }

    fn with_liveness(self, liveness: Liveness) -> Self {
        Self { liveness, ..self }
    }

    /// 是否接受新的数据放置。只有 `Normal + Running` 接受。
    pub fn writable(&self) -> bool {
        self.admin == AdminState::Normal && self.liveness == Liveness::Running
    }

    /// 读定位是否可用。`DECOMMISSIONED`（位置已摘除）、`LOST`（不可达）和
    /// `SHUTDOWN`（进程已退出）不可读；`STALE` 降级但允许。
    pub fn readable(&self) -> bool {
        self.admin != AdminState::Decommissioned
            && matches!(self.liveness, Liveness::Running | Liveness::Stale)
    }

    /// 能否作为复制/搬迁的源。搬空中的节点必须可作源，否则搬不动。
    pub fn replicable_source(&self) -> bool {
        self.liveness == Liveness::Running
            && matches!(
                self.admin,
                AdminState::Normal | AdminState::Cordoned | AdminState::Decommissioning
            )
    }

    /// 能否作为复制目标。等价于可写。
    pub fn replicable_target(&self) -> bool {
        self.writable()
    }

    /// 单标签，用于日志、指标标签与控制台展示。
    pub fn as_label(&self) -> String {
        if self.liveness == Liveness::Running {
            self.admin.as_str_name().to_string()
        } else {
            format!(
                "{}+{}",
                self.admin.as_str_name(),
                self.liveness.as_str_name()
            )
        }
    }

    /// 面向人的单一状态：任一时刻只有一个值。
    ///
    /// 两轴是存储与转移的实现细节（心跳触发只改 `liveness`，运维触发只改
    /// `admin`），但运维看的是「这个节点现在是什么」。所以这里给出一条固定的
    /// 归并规则，而不是把两条轴拼成 `cordoned+stale` 让人自己算：
    ///
    /// - 心跳异常优先：`LOST` > `SHUTDOWN` > `STALE`。节点收不到心跳时，
    ///   「它怎么样了」比「我之前让它干什么」更重要。
    /// - 心跳正常时才看运维意图：`DECOMMISSIONING` > `DECOMMISSIONED` >
    ///   `CORDONED`（只读）> `NORMAL`。
    ///
    /// 结果是 7 个取值（见 [`WorkerSummary::ALL`]），互斥且穷尽。
    pub fn summary(&self) -> WorkerSummary {
        match self.liveness {
            Liveness::Lost => WorkerSummary::Lost,
            Liveness::Shutdown => WorkerSummary::Shutdown,
            Liveness::Stale => WorkerSummary::Stale,
            Liveness::Running => match self.admin {
                AdminState::Normal => WorkerSummary::Normal,
                AdminState::Cordoned => WorkerSummary::ReadOnly,
                AdminState::Decommissioning => WorkerSummary::Decommissioning,
                AdminState::Decommissioned => WorkerSummary::Decommissioned,
            },
        }
    }

    /// 块上报是否计入副本。`DECOMMISSIONED` 必须忽略，且不得下发删除。
    pub fn accepts_block_report_add(&self) -> bool {
        self.admin != AdminState::Decommissioned
            && !matches!(self.liveness, Liveness::Lost | Liveness::Shutdown)
    }
}

impl std::fmt::Display for WorkerState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}/{}",
            self.admin.as_str_name(),
            self.liveness.as_str_name()
        )
    }
}

/// 唯一的状态转移入口。
///
/// 返回 `Ok((新状态, 副作用清单))` 或具体的拒绝原因。
///
/// 实现约束（与 `tests::transitions_match_the_table` 一一对应）：
///
/// - 外层 `match` 对 [`Trigger`] 必须穷举，不写 `_`：新增触发源时编译报错。
/// - 运维触发只改 `AdminState`，心跳触发只改 `Liveness`。
impl AdminState {
    /// Inverse of [`AdminState::as_str_name`], used when reading persisted records.
    pub fn from_str_name(name: &str) -> Option<AdminState> {
        [
            AdminState::Normal,
            AdminState::Cordoned,
            AdminState::Decommissioning,
            AdminState::Decommissioned,
        ]
        .into_iter()
        .find(|state| state.as_str_name() == name)
    }
}

/// - 幂等的心跳超时（重复 STALE / 重复 LOST）一律返回空副作用，避免重复补副本。
pub fn apply(
    state: WorkerState,
    trigger: Trigger,
    facts: &Facts,
) -> Result<(WorkerState, Vec<Effect>), Refusal> {
    use AdminState as A;
    use Liveness as L;

    match trigger {
        Trigger::Cordon => match state.admin() {
            A::Normal => Ok((
                state.with_admin(A::Cordoned),
                vec![Effect::PersistIntent(A::Cordoned)],
            )),
            A::Cordoned => Err(Refusal::AlreadyInState),
            A::Decommissioning | A::Decommissioned => Err(Refusal::NotAllowed),
        },

        Trigger::Uncordon => match state.admin() {
            A::Cordoned => Ok((state.with_admin(A::Normal), vec![Effect::ClearIntent])),
            A::Normal => Err(Refusal::AlreadyInState),
            A::Decommissioning | A::Decommissioned => Err(Refusal::NotAllowed),
        },

        Trigger::Decommission => match state.admin() {
            A::Normal | A::Cordoned => {
                let mut effects = vec![Effect::PersistIntent(A::Decommissioning)];
                // 失联节点先落意图，搬迁等心跳恢复后再启动。
                if state.liveness() == L::Running {
                    effects.push(Effect::StartDrain);
                }
                Ok((state.with_admin(A::Decommissioning), effects))
            }
            A::Decommissioning => Err(Refusal::AlreadyInState),
            A::Decommissioned => Err(Refusal::NotAllowed),
        },

        Trigger::CancelDecommission => match state.admin() {
            // 已搬走的块不回滚，所以退回“只出不进”而不是 Normal。
            A::Decommissioning => Ok((
                state.with_admin(A::Cordoned),
                vec![Effect::PersistIntent(A::Cordoned), Effect::StopDrain],
            )),
            A::Cordoned => Err(Refusal::AlreadyInState),
            A::Normal | A::Decommissioned => Err(Refusal::NotAllowed),
        },

        Trigger::DrainCompleted => match state.admin() {
            A::Decommissioning if state.liveness() == L::Running => {
                if facts.blocks > 0 {
                    return Err(Refusal::BlocksRemain {
                        blocks: facts.blocks,
                    });
                }
                Ok((
                    state.with_admin(A::Decommissioned),
                    vec![Effect::PersistIntent(A::Decommissioned), Effect::StopDrain],
                ))
            }
            A::Decommissioning => Err(Refusal::NotAllowed),
            A::Normal | A::Cordoned => Err(Refusal::NotAllowed),
            A::Decommissioned => Err(Refusal::AlreadyInState),
        },

        Trigger::ForceDecommission => match state.admin() {
            A::Decommissioning => {
                if !facts.deadline_passed {
                    return Err(Refusal::DeadlineNotReached);
                }
                let mut effects = vec![Effect::PersistIntent(A::Decommissioned), Effect::StopDrain];
                if facts.blocks > 0 {
                    effects.push(Effect::DropLocations);
                    effects.push(Effect::RecordOrphans);
                }
                Ok((state.with_admin(A::Decommissioned), effects))
            }
            A::Decommissioned => Err(Refusal::AlreadyInState),
            A::Normal | A::Cordoned => Err(Refusal::NotAllowed),
        },

        Trigger::Recommission => match state.admin() {
            A::Decommissioned => Ok((state.with_admin(A::Normal), vec![Effect::ClearIntent])),
            A::Normal => Err(Refusal::AlreadyInState),
            A::Cordoned | A::Decommissioning => Err(Refusal::NotAllowed),
        },

        Trigger::HeartbeatOk => {
            let effects = if state.liveness() != L::Running && state.admin() == A::Decommissioning {
                vec![Effect::StartDrain]
            } else {
                vec![]
            };
            Ok((state.with_liveness(L::Running), effects))
        }

        Trigger::StaleTimeout => match state.liveness() {
            L::Running => Ok((state.with_liveness(L::Stale), vec![])),
            // 幂等：重复的 STALE 不重复产生副作用。
            L::Stale => Ok((state, vec![])),
            L::Lost | L::Shutdown => Err(Refusal::NotAllowed),
        },

        Trigger::LostTimeout => match state.liveness() {
            L::Lost => Ok((state, vec![])),
            L::Running | L::Stale | L::Shutdown => Ok((
                state.with_liveness(L::Lost),
                vec![Effect::DropLocations, Effect::TriggerReplication],
            )),
        },

        Trigger::GracefulShutdown => match state.liveness() {
            L::Running | L::Stale => Ok((
                state.with_liveness(L::Shutdown),
                vec![Effect::StartShutdownGrace],
            )),
            L::Shutdown => Err(Refusal::AlreadyInState),
            L::Lost => Err(Refusal::NotAllowed),
        },

        Trigger::ShutdownGraceExpired => match state.liveness() {
            L::Shutdown => Ok((
                state.with_liveness(L::Lost),
                vec![Effect::DropLocations, Effect::TriggerReplication],
            )),
            L::Running | L::Stale | L::Lost => Err(Refusal::NotAllowed),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const LIVENESS: [Liveness; 4] = [
        Liveness::Running,
        Liveness::Stale,
        Liveness::Lost,
        Liveness::Shutdown,
    ];

    const ADMINS: [AdminState; 4] = [
        AdminState::Normal,
        AdminState::Cordoned,
        AdminState::Decommissioning,
        AdminState::Decommissioned,
    ];

    /// 两轴 16 种组合都必须归并成 7 个互斥状态之一，而且心跳异常优先。
    #[test]
    fn summary_collapses_the_two_axes_into_one_state() {
        let expect =
            |admin: AdminState, liveness: Liveness| WorkerState::new(admin, liveness).summary();

        // 心跳正常时由运维意图决定。
        assert_eq!(
            expect(AdminState::Normal, Liveness::Running),
            WorkerSummary::Normal
        );
        assert_eq!(
            expect(AdminState::Cordoned, Liveness::Running),
            WorkerSummary::ReadOnly
        );
        assert_eq!(
            expect(AdminState::Decommissioning, Liveness::Running),
            WorkerSummary::Decommissioning
        );
        assert_eq!(
            expect(AdminState::Decommissioned, Liveness::Running),
            WorkerSummary::Decommissioned
        );

        // 心跳异常时，意图让位：节点「怎么了」比「我让它干什么」更重要。
        for liveness in [Liveness::Stale, Liveness::Lost, Liveness::Shutdown] {
            let expected = match liveness {
                Liveness::Stale => WorkerSummary::Stale,
                Liveness::Lost => WorkerSummary::Lost,
                _ => WorkerSummary::Shutdown,
            };
            for admin in ADMINS {
                assert_eq!(
                    expect(admin, liveness),
                    expected,
                    "{admin:?} + {liveness:?} must read as the heartbeat fact"
                );
            }
        }

        // 16 种组合全部落在 7 个取值里，且每个取值都可达。
        let mut seen = Vec::new();
        for admin in ADMINS {
            for liveness in LIVENESS {
                let summary = expect(admin, liveness);
                if !seen.contains(&summary) {
                    seen.push(summary);
                }
            }
        }
        assert_eq!(seen.len(), WorkerSummary::ALL.len());
        for value in WorkerSummary::ALL {
            assert!(seen.contains(&value), "{value:?} is unreachable");
        }
    }

    #[test]
    fn summary_names_round_trip() {
        for value in WorkerSummary::ALL {
            assert_eq!(
                WorkerSummary::from_str_name(value.as_str_name()),
                Some(value)
            );
        }
        assert_eq!(WorkerSummary::from_str_name("cordoned"), None);
        assert_eq!(
            WorkerState::new(AdminState::Cordoned, Liveness::Running)
                .summary()
                .as_str_name(),
            "readonly"
        );
    }

    const TRIGGERS: [Trigger; 12] = [
        Trigger::Cordon,
        Trigger::Uncordon,
        Trigger::Decommission,
        Trigger::CancelDecommission,
        Trigger::DrainCompleted,
        Trigger::ForceDecommission,
        Trigger::Recommission,
        Trigger::HeartbeatOk,
        Trigger::StaleTimeout,
        Trigger::LostTimeout,
        Trigger::GracefulShutdown,
        Trigger::ShutdownGraceExpired,
    ];

    struct Case {
        from: WorkerState,
        trigger: Trigger,
        to: WorkerState,
        effects: Vec<Effect>,
    }

    fn case(
        admin: AdminState,
        liveness: Liveness,
        trigger: Trigger,
        to_admin: AdminState,
        to_liveness: Liveness,
        effects: Vec<Effect>,
    ) -> Case {
        Case {
            from: WorkerState::new(admin, liveness),
            trigger,
            to: WorkerState::new(to_admin, to_liveness),
            effects,
        }
    }

    /// 矩阵测试使用的统一事实：有 5 个块、在线、未擦盘、已过 drain 超时。
    fn canonical_facts() -> Facts {
        Facts {
            deadline_passed: true,
            ..Facts::with_blocks(5)
        }
    }

    /// 转移表：列出所有**允许**的格子。表外的格子必须被拒绝。
    fn allowed_cases() -> Vec<Case> {
        let mut cases = Vec::new();

        for liveness in LIVENESS {
            // 运维：只出不进 / 取消。
            cases.push(case(
                AdminState::Normal,
                liveness,
                Trigger::Cordon,
                AdminState::Cordoned,
                liveness,
                vec![Effect::PersistIntent(AdminState::Cordoned)],
            ));
            cases.push(case(
                AdminState::Cordoned,
                liveness,
                Trigger::Uncordon,
                AdminState::Normal,
                liveness,
                vec![Effect::ClearIntent],
            ));

            // 运维：搬空。失联时不启动搬迁，等心跳恢复再启动。
            for admin in [AdminState::Normal, AdminState::Cordoned] {
                let mut effects = vec![Effect::PersistIntent(AdminState::Decommissioning)];
                if liveness == Liveness::Running {
                    effects.push(Effect::StartDrain);
                }
                cases.push(case(
                    admin,
                    liveness,
                    Trigger::Decommission,
                    AdminState::Decommissioning,
                    liveness,
                    effects,
                ));
            }

            // 运维：取消搬空，退到只出不进。
            cases.push(case(
                AdminState::Decommissioning,
                liveness,
                Trigger::CancelDecommission,
                AdminState::Cordoned,
                liveness,
                vec![
                    Effect::PersistIntent(AdminState::Cordoned),
                    Effect::StopDrain,
                ],
            ));

            // 运维：强制完成（已过超时、仍有块）→ 记录孤儿块。
            cases.push(case(
                AdminState::Decommissioning,
                liveness,
                Trigger::ForceDecommission,
                AdminState::Decommissioned,
                liveness,
                vec![
                    Effect::PersistIntent(AdminState::Decommissioned),
                    Effect::StopDrain,
                    Effect::DropLocations,
                    Effect::RecordOrphans,
                ],
            ));

            // 运维：接回集群。
            cases.push(case(
                AdminState::Decommissioned,
                liveness,
                Trigger::Recommission,
                AdminState::Normal,
                liveness,
                vec![Effect::ClearIntent],
            ));
        }

        // 心跳：恢复。搬空中的节点恢复后继续搬。
        for admin in ADMINS {
            for liveness in LIVENESS {
                let effects = if liveness == Liveness::Running {
                    vec![]
                } else if admin == AdminState::Decommissioning {
                    vec![Effect::StartDrain]
                } else {
                    vec![]
                };
                cases.push(case(
                    admin,
                    liveness,
                    Trigger::HeartbeatOk,
                    admin,
                    Liveness::Running,
                    effects,
                ));
            }
        }

        // 心跳：STALE。重复的 STALE 是幂等的，不重复产生副作用。
        for admin in ADMINS {
            cases.push(case(
                admin,
                Liveness::Running,
                Trigger::StaleTimeout,
                admin,
                Liveness::Stale,
                vec![],
            ));
            cases.push(case(
                admin,
                Liveness::Stale,
                Trigger::StaleTimeout,
                admin,
                Liveness::Stale,
                vec![],
            ));
        }

        // 心跳：LOST。已 LOST 时重复超时不得重复产生副作用。
        for admin in ADMINS {
            for liveness in [Liveness::Running, Liveness::Stale, Liveness::Shutdown] {
                cases.push(case(
                    admin,
                    liveness,
                    Trigger::LostTimeout,
                    admin,
                    Liveness::Lost,
                    vec![Effect::DropLocations, Effect::TriggerReplication],
                ));
            }
            cases.push(case(
                admin,
                Liveness::Lost,
                Trigger::LostTimeout,
                admin,
                Liveness::Lost,
                vec![],
            ));
        }

        // 心跳：主动退出（位置保留，起宽限期）。
        for admin in ADMINS {
            for liveness in [Liveness::Running, Liveness::Stale] {
                cases.push(case(
                    admin,
                    liveness,
                    Trigger::GracefulShutdown,
                    admin,
                    Liveness::Shutdown,
                    vec![Effect::StartShutdownGrace],
                ));
            }
            // 心跳：宽限期到期。
            cases.push(case(
                admin,
                Liveness::Shutdown,
                Trigger::ShutdownGraceExpired,
                admin,
                Liveness::Lost,
                vec![Effect::DropLocations, Effect::TriggerReplication],
            ));
        }

        cases
    }

    fn all_pairs() -> Vec<(WorkerState, Trigger)> {
        let mut pairs = Vec::with_capacity(ADMINS.len() * LIVENESS.len() * TRIGGERS.len());
        for admin in ADMINS {
            for liveness in LIVENESS {
                for trigger in TRIGGERS {
                    pairs.push((WorkerState::new(admin, liveness), trigger));
                }
            }
        }
        pairs
    }

    #[test]
    fn allowed_table_has_no_duplicate_cells() {
        let cases = allowed_cases();
        for (i, a) in cases.iter().enumerate() {
            for b in &cases[i + 1..] {
                assert!(
                    !(a.from == b.from && a.trigger == b.trigger),
                    "duplicate cell: {:?} / {:?}",
                    a.from,
                    a.trigger
                );
            }
        }
    }

    #[test]
    fn transitions_match_the_table() {
        let facts = canonical_facts();
        let cases = allowed_cases();
        let mut failures = Vec::new();

        for (from, trigger) in all_pairs() {
            let expected = cases
                .iter()
                .find(|c| c.from == from && c.trigger == trigger);
            let actual = apply(from, trigger, &facts);
            match expected {
                Some(c) => {
                    let want = Ok((c.to, c.effects.clone()));
                    if actual != want {
                        failures.push(format!(
                            "{:?} + {:?}: want {:?}, got {:?}",
                            from, trigger, want, actual
                        ));
                    }
                }
                None => {
                    if actual.is_ok() {
                        failures.push(format!(
                            "{:?} + {:?}: must be refused, got {:?}",
                            from, trigger, actual
                        ));
                    }
                }
            }
        }

        assert!(
            failures.is_empty(),
            "{} cells do not match the transition table:\n{}",
            failures.len(),
            failures.join("\n")
        );
    }

    #[test]
    fn decommission_then_drain_completion_reaches_decommissioned() {
        let facts = canonical_facts();
        let start = WorkerState::registered(AdminState::Normal);

        let (decommissioning, effects) = apply(start, Trigger::Decommission, &facts)
            .expect("decommission must be accepted from NORMAL");
        assert_eq!(decommissioning.admin(), AdminState::Decommissioning);
        assert!(effects.contains(&Effect::StartDrain));

        let drained = Facts {
            blocks: 0,
            ..canonical_facts()
        };
        let (decommissioned, effects) = apply(decommissioning, Trigger::DrainCompleted, &drained)
            .expect("drain completion must be accepted");
        assert_eq!(decommissioned.admin(), AdminState::Decommissioned);
        assert!(
            !decommissioned.readable(),
            "DECOMMISSIONED must not serve reads"
        );
        assert!(
            !decommissioned.accepts_block_report_add(),
            "DECOMMISSIONED must ignore block report adds"
        );
        assert!(effects.contains(&Effect::StopDrain));
    }

    #[test]
    fn drain_completion_is_refused_while_blocks_remain() {
        let state = WorkerState::new(AdminState::Decommissioning, Liveness::Running);
        let res = apply(state, Trigger::DrainCompleted, &canonical_facts());
        assert_eq!(res, Err(Refusal::BlocksRemain { blocks: 5 }));
    }

    #[test]
    fn force_completion_requires_deadline() {
        let state = WorkerState::new(AdminState::Decommissioning, Liveness::Running);
        let facts = Facts {
            deadline_passed: false,
            ..Facts::with_blocks(5)
        };
        assert_eq!(
            apply(state, Trigger::ForceDecommission, &facts),
            Err(Refusal::DeadlineNotReached)
        );
    }

    #[test]
    fn heartbeat_triggers_never_change_admin_state() {
        let facts = canonical_facts();
        for admin in ADMINS {
            for liveness in LIVENESS {
                let state = WorkerState::new(admin, liveness);
                for trigger in TRIGGERS.into_iter().filter(|t| t.is_heartbeat_driven()) {
                    if let Ok((next, _)) = apply(state, trigger, &facts) {
                        assert_eq!(
                            next.admin(),
                            admin,
                            "heartbeat trigger {:?} changed admin of {:?}",
                            trigger,
                            state
                        );
                    }
                }
            }
        }
    }

    #[test]
    fn repeated_lost_timeout_does_not_repeat_side_effects() {
        let facts = canonical_facts();
        let state = WorkerState::new(AdminState::Normal, Liveness::Running);
        let (lost, effects) = apply(state, Trigger::LostTimeout, &facts).expect("first timeout");
        assert!(effects.contains(&Effect::DropLocations));
        assert!(effects.contains(&Effect::TriggerReplication));

        let (still_lost, effects) = apply(lost, Trigger::LostTimeout, &facts).expect("repeat");
        assert_eq!(still_lost, lost);
        assert!(
            effects.is_empty(),
            "repeated LOST timeout must not repeat replication side effects, got {:?}",
            effects
        );
    }

    #[test]
    fn capability_matrix_matches_the_design() {
        // (admin, liveness, writable, readable, replicable_source, report_add)
        let expected = [
            (
                AdminState::Normal,
                Liveness::Running,
                true,
                true,
                true,
                true,
            ),
            (
                AdminState::Normal,
                Liveness::Stale,
                false,
                true,
                false,
                true,
            ),
            (
                AdminState::Normal,
                Liveness::Lost,
                false,
                false,
                false,
                false,
            ),
            (
                AdminState::Normal,
                Liveness::Shutdown,
                false,
                false,
                false,
                false,
            ),
            (
                AdminState::Cordoned,
                Liveness::Running,
                false,
                true,
                true,
                true,
            ),
            (
                AdminState::Cordoned,
                Liveness::Stale,
                false,
                true,
                false,
                true,
            ),
            (
                AdminState::Cordoned,
                Liveness::Lost,
                false,
                false,
                false,
                false,
            ),
            (
                AdminState::Cordoned,
                Liveness::Shutdown,
                false,
                false,
                false,
                false,
            ),
            (
                AdminState::Decommissioning,
                Liveness::Running,
                false,
                true,
                true,
                true,
            ),
            (
                AdminState::Decommissioning,
                Liveness::Stale,
                false,
                true,
                false,
                true,
            ),
            (
                AdminState::Decommissioning,
                Liveness::Lost,
                false,
                false,
                false,
                false,
            ),
            (
                AdminState::Decommissioning,
                Liveness::Shutdown,
                false,
                false,
                false,
                false,
            ),
            (
                AdminState::Decommissioned,
                Liveness::Running,
                false,
                false,
                false,
                false,
            ),
            (
                AdminState::Decommissioned,
                Liveness::Stale,
                false,
                false,
                false,
                false,
            ),
            (
                AdminState::Decommissioned,
                Liveness::Lost,
                false,
                false,
                false,
                false,
            ),
            (
                AdminState::Decommissioned,
                Liveness::Shutdown,
                false,
                false,
                false,
                false,
            ),
        ];

        for (admin, liveness, writable, readable, source, report_add) in expected {
            let state = WorkerState::new(admin, liveness);
            assert_eq!(state.writable(), writable, "{:?} writable", state);
            assert_eq!(state.readable(), readable, "{:?} readable", state);
            assert_eq!(state.replicable_source(), source, "{:?} source", state);
            assert_eq!(
                state.accepts_block_report_add(),
                report_add,
                "{:?} report",
                state
            );
            assert_eq!(
                state.replicable_target(),
                writable,
                "{:?} target must equal writable",
                state
            );
        }
    }

    #[test]
    fn random_walk_keeps_invariants() {
        let facts = canonical_facts();
        let mut state = WorkerState::registered(AdminState::Normal);
        let mut seed: u64 = 0x9E37_79B9_7F4A_7C15;
        let mut reached = std::collections::HashSet::new();

        for step in 0..20_000 {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            let trigger = TRIGGERS[(seed % TRIGGERS.len() as u64) as usize];
            if let Ok((next, _)) = apply(state, trigger, &facts) {
                state = next;
            }
            reached.insert((state.admin(), state.liveness()));

            if state.admin() == AdminState::Decommissioned {
                assert!(
                    !state.readable(),
                    "step {}: DECOMMISSIONED served reads",
                    step
                );
                assert!(
                    !state.writable(),
                    "step {}: DECOMMISSIONED took writes",
                    step
                );
                assert!(
                    !state.accepts_block_report_add(),
                    "step {}: DECOMMISSIONED accepted block report adds",
                    step
                );
            }
            if state.liveness() == Liveness::Lost {
                assert!(!state.readable(), "step {}: LOST served reads", step);
            }
        }

        assert!(
            reached
                .iter()
                .any(|(a, _)| *a == AdminState::Decommissioned),
            "random walk never reached DECOMMISSIONED; only {:?}",
            reached
        );
    }
}
