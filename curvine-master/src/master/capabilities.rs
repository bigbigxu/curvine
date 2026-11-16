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

//! What a master can do in a given phase, and over which channel.
//!
//! Two questions that operators keep conflating and that this table separates:
//!
//! * **authority** -- is the operation allowed in this phase at all?
//! * **reachability** -- over which channel can it be reached?
//!
//! `curvine snapshot create` is the canonical example: a serving leader is
//! allowed to take a checkpoint, and the operation was reachable only through
//! the leader-gated RPC port, so during a recovery the capability existed but
//! the route did not. Recording both keeps "not allowed" and "cannot be
//! reached" distinguishable instead of collapsing into one error (#CV-94).
//!
//! This table is the single source of truth: the HTTP ops endpoints and the CLI
//! render it, they never keep a second copy.

use crate::master::MasterPhase;

/// Operation an operator or a client may want to perform.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Capability {
    MetadataRead,
    MetadataWrite,
    /// Worker liveness and block reports. Accepted on every member, leader or
    /// not, because the worker map is per-process soft state.
    WorkerHeartbeat,
    /// Client session registration and lease renewal.
    ClientSession,
    /// The observability and ops endpoints themselves.
    OpsHttp,
    /// Diagnostic checkpoint published for inspection. Never enters the raft
    /// snapshot registry, so a suspicious state cannot spread.
    SnapshotExport,
    /// Checkpoint registered as the raft snapshot, the artifact peers restore.
    SnapshotRegister,
    /// Ask the current leader to hand leadership to another voter.
    LeaderTransfer,
    /// Runtime configuration update.
    ConfigUpdate,
}

impl Capability {
    pub fn as_str(self) -> &'static str {
        match self {
            Capability::MetadataRead => "metadata-read",
            Capability::MetadataWrite => "metadata-write",
            Capability::WorkerHeartbeat => "worker-heartbeat",
            Capability::ClientSession => "client-session",
            Capability::OpsHttp => "ops-http",
            Capability::SnapshotExport => "snapshot-export",
            Capability::SnapshotRegister => "snapshot-register",
            Capability::LeaderTransfer => "leader-transfer",
            Capability::ConfigUpdate => "config-update",
        }
    }

    /// Every capability, in a stable order, so renderers do not need to know
    /// the enum layout.
    pub const ALL: [Capability; 9] = [
        Capability::MetadataRead,
        Capability::MetadataWrite,
        Capability::WorkerHeartbeat,
        Capability::ClientSession,
        Capability::OpsHttp,
        Capability::SnapshotExport,
        Capability::SnapshotRegister,
        Capability::LeaderTransfer,
        Capability::ConfigUpdate,
    ];
}

/// Channel a capability is served over.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Reachability {
    /// Client data plane on the master RPC port.
    Rpc,
    /// Observability and ops plane on the master web port.
    HttpWeb,
    Both,
    /// No channel is open in this phase.
    None,
}

impl Reachability {
    pub fn as_str(self) -> &'static str {
        match self {
            Reachability::Rpc => "rpc",
            Reachability::HttpWeb => "http",
            Reachability::Both => "both",
            Reachability::None => "none",
        }
    }
}

/// Outcome a caller should expect.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CapabilityValue {
    /// Allowed, semantics complete.
    Allow,
    /// Refused.
    Deny,
    /// Accepted but queued behind current work; the caller must be told it will
    /// not run now. On a leader this is the apply worker's queue.
    Deferred,
    /// Allowed with a known discount that must travel with the result.
    Degraded,
}

impl CapabilityValue {
    pub fn as_str(self) -> &'static str {
        match self {
            CapabilityValue::Allow => "allow",
            CapabilityValue::Deny => "deny",
            CapabilityValue::Deferred => "deferred",
            CapabilityValue::Degraded => "degraded",
        }
    }

    pub fn is_allowed(self) -> bool {
        matches!(
            self,
            CapabilityValue::Allow | CapabilityValue::Deferred | CapabilityValue::Degraded
        )
    }
}

/// One row of the manifest.
#[derive(Debug, Clone, Copy)]
pub struct CapabilityState {
    pub capability: Capability,
    pub reachability: Reachability,
    pub value: CapabilityValue,
    /// Short human explanation, shown by the CLI next to the value.
    pub note: &'static str,
}

impl CapabilityState {
    const fn new(
        capability: Capability,
        reachability: Reachability,
        value: CapabilityValue,
        note: &'static str,
    ) -> Self {
        Self {
            capability,
            reachability,
            value,
            note,
        }
    }
}

const OPS_ALWAYS: &str = "the ops plane is bound before the raft role is known";
const NOTHING_TO_TRANSFER: &str = "leadership transfer needs a live leader";
const REPLAYING: &str = "the namespace is still being replayed, so reads are not trustworthy";
const REBUILDING: &str = "the local state is being rebuilt from peers";
const SAFE_MODE: &str = "local metadata is damaged; the process stays up for diagnosis";
const EXITING: &str = "the raft node stopped; the process is going away";
const FOLLOWING: &str = "a standby does not serve clients";

macro_rules! row {
    ($cap:expr, $reach:expr, $value:expr, $note:expr) => {
        CapabilityState::new($cap, $reach, $value, $note)
    };
}

/// The manifest for one phase. Kept as one table per phase rather than a
/// capability x phase matrix because each phase's answer is read as a whole.
pub fn capabilities(phase: MasterPhase) -> &'static [CapabilityState] {
    match phase {
        MasterPhase::Starting => &STARTING,
        MasterPhase::Replaying => &REPLAYING_PHASE,
        MasterPhase::CatchingUp => &CATCHING_UP,
        MasterPhase::Serving => &SERVING,
        MasterPhase::StandingBy => &STANDING_BY,
        MasterPhase::Quarantined => &QUARANTINED,
        MasterPhase::SafeMode => &SAFE,
        MasterPhase::Exiting => &EXITING_PHASE,
    }
}

/// Look one capability up in a phase.
pub fn capability(phase: MasterPhase, capability: Capability) -> CapabilityState {
    capabilities(phase)
        .iter()
        .copied()
        .find(|state| state.capability == capability)
        .unwrap_or(CapabilityState::new(
            capability,
            Reachability::None,
            CapabilityValue::Deny,
            "not declared for this phase",
        ))
}

static STARTING: [CapabilityState; 9] = [
    row!(
        Capability::MetadataRead,
        Reachability::None,
        CapabilityValue::Deny,
        "the metadata service is not up yet"
    ),
    row!(
        Capability::MetadataWrite,
        Reachability::None,
        CapabilityValue::Deny,
        "the metadata service is not up yet"
    ),
    row!(
        Capability::WorkerHeartbeat,
        Reachability::Rpc,
        CapabilityValue::Allow,
        "worker liveness is per-process soft state"
    ),
    row!(
        Capability::ClientSession,
        Reachability::None,
        CapabilityValue::Deny,
        "no role yet"
    ),
    row!(
        Capability::OpsHttp,
        Reachability::HttpWeb,
        CapabilityValue::Allow,
        OPS_ALWAYS
    ),
    row!(
        Capability::SnapshotExport,
        Reachability::HttpWeb,
        CapabilityValue::Allow,
        "a checkpoint of the local state can be exported for inspection"
    ),
    row!(
        Capability::SnapshotRegister,
        Reachability::HttpWeb,
        CapabilityValue::Allow,
        "replay is finished and no one is writing: the safest moment to snapshot"
    ),
    row!(
        Capability::LeaderTransfer,
        Reachability::HttpWeb,
        CapabilityValue::Deny,
        NOTHING_TO_TRANSFER
    ),
    row!(
        Capability::ConfigUpdate,
        Reachability::HttpWeb,
        CapabilityValue::Allow,
        "runtime configuration does not depend on raft"
    ),
];

static REPLAYING_PHASE: [CapabilityState; 9] = [
    row!(
        Capability::MetadataRead,
        Reachability::None,
        CapabilityValue::Deny,
        REPLAYING
    ),
    row!(
        Capability::MetadataWrite,
        Reachability::None,
        CapabilityValue::Deny,
        REPLAYING
    ),
    row!(
        Capability::WorkerHeartbeat,
        Reachability::Rpc,
        CapabilityValue::Allow,
        "worker liveness is per-process soft state"
    ),
    row!(
        Capability::ClientSession,
        Reachability::None,
        CapabilityValue::Deny,
        "no role yet"
    ),
    row!(
        Capability::OpsHttp,
        Reachability::HttpWeb,
        CapabilityValue::Allow,
        OPS_ALWAYS
    ),
    row!(
        Capability::SnapshotExport,
        Reachability::HttpWeb,
        CapabilityValue::Allow,
        "the applied prefix can be exported, labelled unclean"
    ),
    row!(
        Capability::SnapshotRegister,
        Reachability::None,
        CapabilityValue::Deny,
        "replay runs inline here, so a checkpoint would race the apply loop"
    ),
    row!(
        Capability::LeaderTransfer,
        Reachability::HttpWeb,
        CapabilityValue::Deny,
        NOTHING_TO_TRANSFER
    ),
    row!(
        Capability::ConfigUpdate,
        Reachability::HttpWeb,
        CapabilityValue::Allow,
        "runtime configuration does not depend on raft"
    ),
];

static CATCHING_UP: [CapabilityState; 9] = [
    row!(
        Capability::MetadataRead,
        Reachability::None,
        CapabilityValue::Deny,
        REPLAYING
    ),
    row!(
        Capability::MetadataWrite,
        Reachability::None,
        CapabilityValue::Deny,
        REPLAYING
    ),
    row!(
        Capability::WorkerHeartbeat,
        Reachability::Rpc,
        CapabilityValue::Allow,
        "the leader needs worker liveness to resume exports"
    ),
    row!(
        Capability::ClientSession,
        Reachability::None,
        CapabilityValue::Deny,
        "sessions are served once the metadata caught up"
    ),
    row!(
        Capability::OpsHttp,
        Reachability::HttpWeb,
        CapabilityValue::Allow,
        OPS_ALWAYS
    ),
    row!(
        Capability::SnapshotExport,
        Reachability::HttpWeb,
        CapabilityValue::Allow,
        "the applied prefix can be exported, labelled unclean"
    ),
    row!(
        Capability::SnapshotRegister,
        Reachability::Rpc,
        CapabilityValue::Deferred,
        "queued behind the catch-up scan on the apply worker"
    ),
    row!(
        Capability::LeaderTransfer,
        Reachability::HttpWeb,
        CapabilityValue::Deny,
        "transferring to a leader that cannot serve yet only moves the outage"
    ),
    row!(
        Capability::ConfigUpdate,
        Reachability::HttpWeb,
        CapabilityValue::Allow,
        "runtime configuration does not depend on raft"
    ),
];

static SERVING: [CapabilityState; 9] = [
    row!(
        Capability::MetadataRead,
        Reachability::Rpc,
        CapabilityValue::Allow,
        ""
    ),
    row!(
        Capability::MetadataWrite,
        Reachability::Rpc,
        CapabilityValue::Allow,
        ""
    ),
    row!(
        Capability::WorkerHeartbeat,
        Reachability::Rpc,
        CapabilityValue::Allow,
        ""
    ),
    row!(
        Capability::ClientSession,
        Reachability::Rpc,
        CapabilityValue::Allow,
        ""
    ),
    row!(
        Capability::OpsHttp,
        Reachability::HttpWeb,
        CapabilityValue::Allow,
        ""
    ),
    row!(
        Capability::SnapshotExport,
        Reachability::HttpWeb,
        CapabilityValue::Allow,
        ""
    ),
    row!(
        Capability::SnapshotRegister,
        Reachability::Rpc,
        CapabilityValue::Allow,
        "costs a metadata write stall for the length of the checkpoint"
    ),
    row!(
        Capability::LeaderTransfer,
        Reachability::HttpWeb,
        CapabilityValue::Allow,
        "expresses a preference; raft still refuses a candidate that is behind"
    ),
    row!(
        Capability::ConfigUpdate,
        Reachability::HttpWeb,
        CapabilityValue::Allow,
        ""
    ),
];

static STANDING_BY: [CapabilityState; 9] = [
    row!(
        Capability::MetadataRead,
        Reachability::None,
        CapabilityValue::Deny,
        FOLLOWING
    ),
    row!(
        Capability::MetadataWrite,
        Reachability::None,
        CapabilityValue::Deny,
        FOLLOWING
    ),
    row!(
        Capability::WorkerHeartbeat,
        Reachability::Rpc,
        CapabilityValue::Allow,
        "worker liveness is per-process soft state"
    ),
    row!(
        Capability::ClientSession,
        Reachability::None,
        CapabilityValue::Deny,
        FOLLOWING
    ),
    row!(
        Capability::OpsHttp,
        Reachability::HttpWeb,
        CapabilityValue::Allow,
        OPS_ALWAYS
    ),
    row!(
        Capability::SnapshotExport,
        Reachability::HttpWeb,
        CapabilityValue::Allow,
        ""
    ),
    row!(
        Capability::SnapshotRegister,
        Reachability::Rpc,
        CapabilityValue::Allow,
        "a follower checkpoint never stalls a client write"
    ),
    row!(
        Capability::LeaderTransfer,
        Reachability::HttpWeb,
        CapabilityValue::Deny,
        NOTHING_TO_TRANSFER
    ),
    row!(
        Capability::ConfigUpdate,
        Reachability::HttpWeb,
        CapabilityValue::Allow,
        ""
    ),
];

static QUARANTINED: [CapabilityState; 9] = [
    row!(
        Capability::MetadataRead,
        Reachability::None,
        CapabilityValue::Deny,
        REBUILDING
    ),
    row!(
        Capability::MetadataWrite,
        Reachability::None,
        CapabilityValue::Deny,
        REBUILDING
    ),
    row!(
        Capability::WorkerHeartbeat,
        Reachability::Rpc,
        CapabilityValue::Allow,
        "worker liveness is per-process soft state"
    ),
    row!(
        Capability::ClientSession,
        Reachability::None,
        CapabilityValue::Deny,
        REBUILDING
    ),
    row!(
        Capability::OpsHttp,
        Reachability::HttpWeb,
        CapabilityValue::Allow,
        OPS_ALWAYS
    ),
    row!(
        Capability::SnapshotExport,
        Reachability::HttpWeb,
        CapabilityValue::Deny,
        "exporting a half-rebuilt state would only mislead"
    ),
    row!(
        Capability::SnapshotRegister,
        Reachability::None,
        CapabilityValue::Deny,
        REBUILDING
    ),
    row!(
        Capability::LeaderTransfer,
        Reachability::HttpWeb,
        CapabilityValue::Deny,
        "a quarantined member must not become leader"
    ),
    row!(
        Capability::ConfigUpdate,
        Reachability::HttpWeb,
        CapabilityValue::Allow,
        ""
    ),
];

static SAFE: [CapabilityState; 9] = [
    row!(
        Capability::MetadataRead,
        Reachability::None,
        CapabilityValue::Deny,
        SAFE_MODE
    ),
    row!(
        Capability::MetadataWrite,
        Reachability::None,
        CapabilityValue::Deny,
        SAFE_MODE
    ),
    row!(
        Capability::WorkerHeartbeat,
        Reachability::Rpc,
        CapabilityValue::Allow,
        "keeping workers registered is what makes recovery possible"
    ),
    row!(
        Capability::ClientSession,
        Reachability::None,
        CapabilityValue::Deny,
        SAFE_MODE
    ),
    row!(
        Capability::OpsHttp,
        Reachability::HttpWeb,
        CapabilityValue::Allow,
        OPS_ALWAYS
    ),
    row!(
        Capability::SnapshotExport,
        Reachability::HttpWeb,
        CapabilityValue::Degraded,
        "exported for diagnosis only: the prefix is damaged and must be labelled unclean"
    ),
    row!(
        Capability::SnapshotRegister,
        Reachability::None,
        CapabilityValue::Deny,
        "registering a damaged prefix would spread it to every peer"
    ),
    row!(
        Capability::LeaderTransfer,
        Reachability::HttpWeb,
        CapabilityValue::Deny,
        "a safe-mode master must not hand out leadership"
    ),
    row!(
        Capability::ConfigUpdate,
        Reachability::HttpWeb,
        CapabilityValue::Allow,
        ""
    ),
];

static EXITING_PHASE: [CapabilityState; 9] = [
    row!(
        Capability::MetadataRead,
        Reachability::None,
        CapabilityValue::Deny,
        EXITING
    ),
    row!(
        Capability::MetadataWrite,
        Reachability::None,
        CapabilityValue::Deny,
        EXITING
    ),
    row!(
        Capability::WorkerHeartbeat,
        Reachability::None,
        CapabilityValue::Deny,
        EXITING
    ),
    row!(
        Capability::ClientSession,
        Reachability::None,
        CapabilityValue::Deny,
        EXITING
    ),
    row!(
        Capability::OpsHttp,
        Reachability::None,
        CapabilityValue::Deny,
        EXITING
    ),
    row!(
        Capability::SnapshotExport,
        Reachability::None,
        CapabilityValue::Deny,
        EXITING
    ),
    row!(
        Capability::SnapshotRegister,
        Reachability::None,
        CapabilityValue::Deny,
        EXITING
    ),
    row!(
        Capability::LeaderTransfer,
        Reachability::None,
        CapabilityValue::Deny,
        EXITING
    ),
    row!(
        Capability::ConfigUpdate,
        Reachability::None,
        CapabilityValue::Deny,
        EXITING
    ),
];

#[cfg(test)]
mod tests {
    use super::*;

    const ALL_PHASES: [MasterPhase; 8] = [
        MasterPhase::Starting,
        MasterPhase::Replaying,
        MasterPhase::CatchingUp,
        MasterPhase::Serving,
        MasterPhase::StandingBy,
        MasterPhase::Quarantined,
        MasterPhase::SafeMode,
        MasterPhase::Exiting,
    ];

    #[test]
    fn every_phase_declares_every_capability_exactly_once() {
        for phase in ALL_PHASES {
            let rows = capabilities(phase);
            assert_eq!(rows.len(), Capability::ALL.len(), "{phase:?}");
            for cap in Capability::ALL {
                let count = rows.iter().filter(|row| row.capability == cap).count();
                assert_eq!(count, 1, "{phase:?} must declare {cap:?} once");
            }
        }
    }

    #[test]
    fn only_a_serving_leader_serves_metadata() {
        for phase in ALL_PHASES {
            let read = capability(phase, Capability::MetadataRead);
            let write = capability(phase, Capability::MetadataWrite);
            if phase == MasterPhase::Serving {
                assert_eq!(read.value, CapabilityValue::Allow);
                assert_eq!(write.value, CapabilityValue::Allow);
            } else {
                // Reading a namespace that is still replaying would return a
                // consistent-but-stale prefix, which FUSE then caches.
                assert_eq!(read.value, CapabilityValue::Deny, "{phase:?}");
                assert_eq!(write.value, CapabilityValue::Deny, "{phase:?}");
            }
        }
    }

    #[test]
    fn the_ops_plane_is_reachable_in_every_phase_except_exit() {
        for phase in ALL_PHASES {
            let ops = capability(phase, Capability::OpsHttp);
            if phase == MasterPhase::Exiting {
                assert_eq!(ops.value, CapabilityValue::Deny);
            } else {
                assert_eq!(ops.value, CapabilityValue::Allow, "{phase:?}");
                assert_eq!(ops.reachability, Reachability::HttpWeb, "{phase:?}");
            }
        }
    }

    #[test]
    fn worker_heartbeats_are_accepted_wherever_the_process_is_live() {
        for phase in ALL_PHASES {
            let expected = if phase == MasterPhase::Exiting {
                CapabilityValue::Deny
            } else {
                CapabilityValue::Allow
            };
            assert_eq!(
                capability(phase, Capability::WorkerHeartbeat).value,
                expected,
                "{phase:?}"
            );
        }
    }

    /// Once replay has finished and no one is writing yet, snapshotting is the
    /// safest thing an operator can do -- and the only moment in a recovery
    /// where registering a snapshot is safe.
    #[test]
    fn snapshot_register_is_allowed_once_replay_finished() {
        let starting = capability(MasterPhase::Starting, Capability::SnapshotRegister);
        assert_eq!(starting.value, CapabilityValue::Allow);
        assert_eq!(
            starting.reachability,
            Reachability::HttpWeb,
            "only the web plane is up before the raft role is known"
        );

        // Still replaying: a registered prefix would be re-applied on restore.
        assert_eq!(
            capability(MasterPhase::Replaying, Capability::SnapshotRegister).value,
            CapabilityValue::Deny
        );
        // Queued behind the catch-up scan.
        assert_eq!(
            capability(MasterPhase::CatchingUp, Capability::SnapshotRegister).value,
            CapabilityValue::Deferred
        );
    }

    #[test]
    fn a_damaged_prefix_can_be_exported_but_never_registered() {
        let export = capability(MasterPhase::SafeMode, Capability::SnapshotExport);
        assert_eq!(export.value, CapabilityValue::Degraded);
        assert!(!export.note.is_empty(), "a discount must explain itself");

        let register = capability(MasterPhase::SafeMode, Capability::SnapshotRegister);
        assert_eq!(register.value, CapabilityValue::Deny);
    }

    #[test]
    fn leader_transfer_needs_a_leader_that_can_serve() {
        assert_eq!(
            capability(MasterPhase::Serving, Capability::LeaderTransfer).value,
            CapabilityValue::Allow
        );
        for phase in [
            MasterPhase::Starting,
            MasterPhase::Replaying,
            MasterPhase::CatchingUp,
            MasterPhase::StandingBy,
            MasterPhase::Quarantined,
            MasterPhase::SafeMode,
            MasterPhase::Exiting,
        ] {
            assert_eq!(
                capability(phase, Capability::LeaderTransfer).value,
                CapabilityValue::Deny,
                "{phase:?}"
            );
        }
    }

    #[test]
    fn capability_names_and_values_are_stable_wire_strings() {
        assert_eq!(Capability::MetadataRead.as_str(), "metadata-read");
        assert_eq!(Capability::SnapshotRegister.as_str(), "snapshot-register");
        assert_eq!(CapabilityValue::Deferred.as_str(), "deferred");
        assert_eq!(Reachability::HttpWeb.as_str(), "http");
        assert!(CapabilityValue::Degraded.is_allowed());
        assert!(!CapabilityValue::Deny.is_allowed());
    }
}
