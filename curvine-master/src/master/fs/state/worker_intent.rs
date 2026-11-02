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

//! Operator intent for a worker host.
//!
//! Intent is the only authoritative record of what an operator asked for. It is
//! keyed by host so it survives worker re-registration, session changes and a
//! `worker_id` change; heartbeat handling reads it and never writes it.
//!
//! State semantics live in [`curvine_model::worker_admin`]; this module only
//! stores the record and its provenance.

use curvine_model::AdminState;
use serde::{Deserialize, Serialize};
use curvine_runtime::common::LocalTime;
use std::collections::HashMap;

/// One operator decision about one host.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkerIntent {
    /// What the host must do. `Normal` is never stored: removing the record is
    /// what returns a host to `Normal`.
    pub admin: AdminState,
    /// When the decision was taken (ms since epoch).
    pub since_ms: i64,
    /// Who issued it. Empty for migrations from the legacy hostname set.
    pub operator: String,
    /// Why. Free text, shown in the console and audit log.
    pub reason: String,
    /// Deadline for the current drain, 0 when no deadline is set.
    pub drain_deadline_ms: i64,
    /// Sample of the block ids whose locations were dropped by a forced
    /// completion. The journal stays bounded: `orphan_count` is authoritative.
    pub orphan_blocks: Vec<i64>,
    /// How many locations the forced completion dropped.
    pub orphan_count: u64,
    /// When the forced completion happened (ms since epoch), 0 if never.
    pub forced_at_ms: i64,
    /// Preflight taken when the drain started: block locations to move.
    pub preflight_blocks: u64,
    /// Preflight: of those, blocks whose only location is this worker, i.e. the
    /// ones that would become unreadable if the drain were forced.
    pub preflight_single_replica: u64,
}

impl WorkerIntent {
    pub fn new(
        admin: AdminState,
        operator: impl Into<String>,
        reason: impl Into<String>,
        drain_deadline_ms: i64,
    ) -> Self {
        Self {
            admin,
            since_ms: LocalTime::mills() as i64,
            operator: operator.into(),
            reason: reason.into(),
            drain_deadline_ms,
            orphan_blocks: Vec::new(),
            orphan_count: 0,
            forced_at_ms: 0,
            preflight_blocks: 0,
            preflight_single_replica: 0,
        }
    }

    /// Legacy decommission record: no operator, no reason, no deadline.
    pub fn decommissioned() -> Self {
        Self::new(AdminState::Decommissioned, "", "", 0)
    }

    pub fn is_decommissioned(&self) -> bool {
        self.admin == AdminState::Decommissioned
    }

    /// Records what a forced completion left behind.
    pub fn record_orphans(&mut self, orphan_count: u64, sample: Vec<i64>, forced_at_ms: i64) {
        self.orphan_count = orphan_count;
        self.orphan_blocks = sample;
        self.forced_at_ms = forced_at_ms;
    }
}

/// Host-keyed intent records.
///
/// `Normal` is represented by the absence of a record, so a lookup miss means
/// "no operator constraint". The table is intentionally small (one entry per
/// operator action that has not been cleared) and is journaled by the caller.
#[derive(Debug, Default, Clone)]
pub struct WorkerIntentTable {
    by_host: HashMap<String, WorkerIntent>,
}

impl WorkerIntentTable {
    pub fn new() -> Self {
        Self {
            by_host: HashMap::new(),
        }
    }

    /// Records `intent` for `host`. A `Normal` intent deletes the record.
    pub fn upsert(&mut self, host: &str, intent: WorkerIntent) -> Option<WorkerIntent> {
        if intent.admin == AdminState::Normal {
            return self.by_host.remove(host);
        }
        self.by_host.insert(host.to_string(), intent)
    }

    pub fn remove(&mut self, host: &str) -> Option<WorkerIntent> {
        self.by_host.remove(host)
    }

    pub fn get(&self, host: &str) -> Option<&WorkerIntent> {
        self.by_host.get(host)
    }

    /// Admin state requested for `host`, `Normal` when there is no record.
    pub fn admin_of(&self, host: &str) -> AdminState {
        self.by_host
            .get(host)
            .map(|intent| intent.admin)
            .unwrap_or(AdminState::Normal)
    }

    pub fn is_decommissioned(&self, host: &str) -> bool {
        self.admin_of(host) == AdminState::Decommissioned
    }

    pub fn len(&self) -> usize {
        self.by_host.len()
    }

    pub fn is_empty(&self) -> bool {
        self.by_host.is_empty()
    }

    /// Hosts with a record, sorted so callers (CLI, HTTP) are deterministic.
    pub fn hosts(&self) -> Vec<String> {
        let mut hosts: Vec<String> = self.by_host.keys().cloned().collect();
        hosts.sort();
        hosts
    }

    pub fn iter(&self) -> impl Iterator<Item = (&String, &WorkerIntent)> {
        self.by_host.iter()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn decommissioned(operator: &str) -> WorkerIntent {
        WorkerIntent::new(AdminState::Decommissioned, operator, "hardware return", 0)
    }

    #[test]
    fn upsert_records_and_replaces_intent() {
        let mut table = WorkerIntentTable::new();
        assert!(table.upsert("host-a", decommissioned("alice")).is_none());
        assert_eq!(table.admin_of("host-a"), AdminState::Decommissioned);

        let replaced = table
            .upsert(
                "host-a",
                WorkerIntent::new(AdminState::Cordoned, "bob", "disk slow", 0),
            )
            .expect("previous intent must be returned");
        assert_eq!(replaced.operator, "alice");
        assert_eq!(table.admin_of("host-a"), AdminState::Cordoned);
        assert_eq!(table.len(), 1);
    }

    #[test]
    fn normal_intent_clears_the_record() {
        let mut table = WorkerIntentTable::new();
        table.upsert("host-a", decommissioned("alice"));

        let previous = table.upsert(
            "host-a",
            WorkerIntent::new(AdminState::Normal, "alice", "recommission", 0),
        );
        assert!(previous.is_some());
        assert!(table.is_empty());
        assert_eq!(table.admin_of("host-a"), AdminState::Normal);
    }

    #[test]
    fn missing_host_has_no_operator_constraint() {
        let table = WorkerIntentTable::new();
        assert_eq!(table.admin_of("nobody"), AdminState::Normal);
        assert!(!table.is_decommissioned("nobody"));
        assert!(table.hosts().is_empty());
    }

    #[test]
    fn hosts_are_sorted_for_deterministic_output() {
        let mut table = WorkerIntentTable::new();
        table.upsert("host-c", decommissioned("alice"));
        table.upsert("host-a", decommissioned("alice"));
        table.upsert("host-b", decommissioned("alice"));
        assert_eq!(table.hosts(), vec!["host-a", "host-b", "host-c"]);
    }
}
