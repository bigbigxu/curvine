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

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

/// What the apply path does with an error. Transient faults retry in the
/// background. Only damage that would serve a wrong namespace enters safe mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ApplyDisposition {
    Retry,
    EnterSafeMode,
}

pub(crate) fn classify_apply_error(message: &str) -> ApplyDisposition {
    let lower = message.to_ascii_lowercase();
    if is_metadata_damage(&lower) {
        ApplyDisposition::EnterSafeMode
    } else {
        ApplyDisposition::Retry
    }
}

fn is_metadata_damage(lower: &str) -> bool {
    lower.contains("corrupt")
        || lower.contains("checksum")
        || lower.contains("deserialize")
        || lower.contains("invariant")
        || lower.contains("poisoned")
}

/// Shared recovery flags for the journal apply loop, writer, and HTTP probes.
/// Clone shares the same atomics.
#[derive(Clone)]
pub(crate) struct RecoveryGate {
    inner: Arc<RecoveryInner>,
}

struct RecoveryInner {
    safe_mode: AtomicBool,
    writer_failed: AtomicBool,
    ufs_caught_up: AtomicBool,
    pending: AtomicU64,
    last_error: Mutex<String>,
}

impl RecoveryGate {
    pub(crate) fn new() -> Self {
        Self {
            inner: Arc::new(RecoveryInner {
                safe_mode: AtomicBool::new(false),
                writer_failed: AtomicBool::new(false),
                ufs_caught_up: AtomicBool::new(false),
                pending: AtomicU64::new(0),
                last_error: Mutex::new(String::new()),
            }),
        }
    }

    pub(crate) fn is_safe_mode(&self) -> bool {
        self.inner.safe_mode.load(Ordering::Acquire)
    }

    pub(crate) fn enter_safe_mode(&self, reason: &str) {
        self.note_error(reason);
        self.inner.safe_mode.store(true, Ordering::Release);
        self.inner.ufs_caught_up.store(false, Ordering::Release);
        log::error!("master entered safe mode: {reason}");
    }

    pub(crate) fn mark_writer_failed(&self) {
        self.inner.writer_failed.store(true, Ordering::Release);
    }

    pub(crate) fn rejects_writes(&self) -> bool {
        self.is_safe_mode() || self.inner.writer_failed.load(Ordering::Acquire)
    }

    pub(crate) fn set_ufs_caught_up(&self, caught_up: bool) {
        self.inner.ufs_caught_up.store(caught_up, Ordering::Release);
    }

    pub(crate) fn ufs_caught_up(&self) -> bool {
        self.inner.ufs_caught_up.load(Ordering::Acquire)
    }

    pub(crate) fn set_pending(&self, pending: u64) {
        self.inner.pending.store(pending, Ordering::Release);
    }

    pub(crate) fn pending(&self) -> u64 {
        self.inner.pending.load(Ordering::Acquire)
    }

    /// Last recorded failure, empty while nothing has gone wrong. Read by the
    /// ops plane so a safe-mode master can say *why* it stopped serving
    /// instead of only that it did (#CV-94).
    pub(crate) fn last_error(&self) -> Option<String> {
        let slot = match self.inner.last_error.lock() {
            Ok(slot) => slot,
            Err(poisoned) => poisoned.into_inner(),
        };
        if slot.is_empty() {
            None
        } else {
            Some(slot.clone())
        }
    }

    pub(crate) fn note_error(&self, error: &str) {
        if let Ok(mut slot) = self.inner.last_error.lock() {
            slot.clear();
            slot.push_str(error);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn transient_worker_and_raft_errors_retry() {
        for message in [
            "No workers available",
            "No eligible workers for weighted selection",
            "NotLeader",
            "LeaderNotReady",
            "Connection refused",
            "deadline has elapsed",
            "timed out",
        ] {
            assert_eq!(
                classify_apply_error(message),
                ApplyDisposition::Retry,
                "{message}"
            );
        }
    }

    #[test]
    fn metadata_damage_enters_safe_mode() {
        for message in [
            "failed to deserialize journal batch",
            "journal entry corrupt",
            "checksum mismatch",
            "metadata invariant broken",
        ] {
            assert_eq!(
                classify_apply_error(message),
                ApplyDisposition::EnterSafeMode,
                "{message}"
            );
        }
    }

    #[test]
    fn disk_and_frame_errors_stay_retryable() {
        for message in [
            "rocksdb: IO error: No space left on device",
            "failed to decode rpc frame",
        ] {
            assert_eq!(
                classify_apply_error(message),
                ApplyDisposition::Retry,
                "{message}"
            );
        }
    }

    #[test]
    fn safe_mode_rejects_writes_and_clears_caught_up() {
        let gate = RecoveryGate::new();
        gate.set_ufs_caught_up(true);
        assert!(!gate.rejects_writes());
        gate.enter_safe_mode("journal entry checksum mismatch");
        assert!(gate.is_safe_mode());
        assert!(gate.rejects_writes());
        assert!(!gate.ufs_caught_up());
    }
}
