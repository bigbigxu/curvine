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

//! Session-scoped write-tool grants for the civi agent.
//!
//! The confirmation flow pauses every write tool call until the user approves.
//! Typing "batch-create 50 files" would mean 50 confirmation cards, so the
//! console offers a per-session approval: the card's "approve all" action
//! grants the *tool* (e.g. `fs_operate`) for the rest of the conversation and
//! subsequent calls dispatch directly.
//!
//! Grants live in memory only, keyed by the conversation's session id (minted
//! by the console), expire after [`GRANT_TTL`] of inactivity, and the table is
//! capped so a leaked session id cannot grow it without bound. MCP clients
//! bypass none of this: they never mint sessions, so they keep the default
//! per-call behaviour of their endpoint.

use std::collections::{HashMap, HashSet};
use std::sync::Mutex;
use std::time::{Duration, Instant};

/// Grants expire after this much inactivity.
const GRANT_TTL: Duration = Duration::from_secs(2 * 60 * 60);
/// Upper bound on tracked sessions; the least recently used entry is dropped
/// when the table is full.
const MAX_SESSIONS: usize = 256;
/// Longest accepted session id (console-minted ids are far shorter).
pub const MAX_SESSION_ID_LEN: usize = 128;

struct GrantEntry {
    tools: HashSet<String>,
    last_used: Instant,
}

/// Shared, in-memory grant table.
#[derive(Clone)]
pub struct SessionGrants {
    sessions: std::sync::Arc<Mutex<HashMap<String, GrantEntry>>>,
}

impl Default for SessionGrants {
    fn default() -> Self {
        Self {
            sessions: std::sync::Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

impl SessionGrants {
    pub fn new() -> Self {
        Self::default()
    }

    /// Whether `session_id` holds a grant for `tool`. Refreshes the entry's
    /// last-used timestamp so an active conversation keeps its grants alive.
    pub fn is_granted(&self, session_id: &str, tool: &str) -> bool {
        if session_id.len() > MAX_SESSION_ID_LEN {
            return false;
        }
        let mut sessions = self.lock();
        Self::sweep(&mut sessions);
        match sessions.get_mut(session_id) {
            Some(entry) if entry.tools.contains(tool) => {
                entry.last_used = Instant::now();
                true
            }
            _ => false,
        }
    }

    /// Grants `tool` for the rest of the session.
    pub fn grant(&self, session_id: &str, tool: &str) {
        if session_id.is_empty() || session_id.len() > MAX_SESSION_ID_LEN || tool.is_empty() {
            return;
        }
        let mut sessions = self.lock();
        let entry = sessions
            .entry(session_id.to_string())
            .or_insert_with(|| GrantEntry {
                tools: HashSet::new(),
                last_used: Instant::now(),
            });
        entry.tools.insert(tool.to_string());
        entry.last_used = Instant::now();
        // Cap after the insert: evict expired entries first, then LRU.
        Self::sweep(&mut sessions);
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<String, GrantEntry>> {
        self.sessions.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Drops expired grants; when the table is over capacity, evicts the
    /// least recently used sessions first.
    fn sweep(sessions: &mut HashMap<String, GrantEntry>) {
        sessions.retain(|_, entry| entry.last_used.elapsed() < GRANT_TTL);
        if sessions.len() <= MAX_SESSIONS {
            return;
        }
        let mut by_age: Vec<(String, Instant)> = sessions
            .iter()
            .map(|(id, entry)| (id.clone(), entry.last_used))
            .collect();
        by_age.sort_by_key(|(_, used)| *used);
        let evict = sessions.len() - MAX_SESSIONS;
        for (id, _) in by_age.into_iter().take(evict) {
            sessions.remove(&id);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn grant_is_scoped_to_session_and_tool() {
        let grants = SessionGrants::new();
        grants.grant("s1", "fs_operate");

        assert!(grants.is_granted("s1", "fs_operate"));
        assert!(!grants.is_granted("s1", "worker_operate"));
        assert!(!grants.is_granted("s2", "fs_operate"));
    }

    #[test]
    fn rejects_oversized_or_empty_session_ids() {
        let grants = SessionGrants::new();
        let long_id = "x".repeat(MAX_SESSION_ID_LEN + 1);
        grants.grant(&long_id, "fs_operate");
        grants.grant("", "fs_operate");
        assert!(!grants.is_granted(&long_id, "fs_operate"));
        assert!(!grants.is_granted("", "fs_operate"));
    }

    #[test]
    fn table_is_capped() {
        let grants = SessionGrants::new();
        for i in 0..(MAX_SESSIONS + 16) {
            grants.grant(&format!("s{i}"), "fs_operate");
        }
        let sessions = grants.lock();
        assert!(sessions.len() <= MAX_SESSIONS);
    }
}
