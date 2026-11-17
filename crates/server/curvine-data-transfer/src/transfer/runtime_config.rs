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

//! Live runtime configuration for the transfer role.
//!
//! `job.worker_max_concurrent_tasks` gates dispatch (it is the per-worker
//! in-flight ceiling used by [`crate::transfer::TransferScheduler`]). Copying it
//! into the scheduler at construction time is the exact anti-pattern this module
//! removes: the same knob is already runtime-writable in the worker, so a pushed
//! value would be reported by the config endpoint while the scheduler kept
//! dispatching on the value read at startup. The value therefore lives behind an
//! atomic that every dispatch decision re-reads, and the registry built here is
//! the only write path.

use curvine_config::{ClusterConf, RuntimeConfigRegistry, TransferConf};
use curvine_core_error::{err_box, CommonResult};
use curvine_error::{FsError, FsResult};
use curvine_runtime::common::ByteUnit;
use serde_json::Value;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicUsize, Ordering};
use std::sync::Arc;

/// Runtime key for the per-worker concurrency ceiling.
///
/// The key keeps the historical spelling already published by the worker
/// registry; the on-disk field it is backed by is
/// `job.worker_max_concurrent_tasks`.
pub const WORKER_MAX_CONCURRENT_TASKS_CONFIG: &str = "worker_max_concurrent_tasks";

/// Runtime key for the per-worker in-flight byte budget.
///
/// The transfer role is the only authority for admission: workers report their
/// own load and reject, but the *ceiling* used to decide whether to dispatch is
/// this value, so tuning one place (the transfer) is enough. Like the worker's
/// key of the same name there is no configuration-file entry - the default is
/// `TransferConf::DEFAULT_WORKER_INFLIGHT_BUDGET_BYTES` - so it resets on
/// restart instead of pretending to be persisted.
pub const WORKER_INFLIGHT_BUDGET_CONFIG: &str = "worker_inflight_budget_bytes";

/// Runtime key for the per-job planned-task ceiling.
///
/// Backed by `transfer.max_tasks_per_transfer`. Lowering it cannot invalidate an
/// already-persisted plan: the limit is enforced while planning, so a running
/// job keeps the task rows it already has.
pub const MAX_TASKS_PER_TRANSFER_CONFIG: &str = "max_tasks_per_transfer";

/// Runtime key for the collected-entry ceiling of one planning pass.
///
/// Backed by `transfer.max_plan_entries`. Collection is what dominates planning
/// memory, so this is the knob that decides how wide a source tree one job may
/// walk before failing with "entries exceeds N during listing".
pub const MAX_PLAN_ENTRIES_CONFIG: &str = "max_plan_entries";

/// Runtime key for the submit brake.
///
/// Backed by `transfer.accept_jobs`. It is not the startup switch
/// (`transfer.enabled`): this one only decides whether the already running
/// service accepts *new* work.
pub const ACCEPT_JOBS_CONFIG: &str = "accept_jobs";

/// Runtime key for the per-instance ceiling on concurrently executing transfers.
///
/// Backed by `transfer.max_running_transfers`. Lowering it never aborts a running
/// job: the scheduler reads the value on every acquire, so the excess drains as
/// jobs finish.
pub const MAX_RUNNING_TRANSFERS_CONFIG: &str = "max_running_transfers";

/// Transfer-role knobs that are adjustable without a restart.
///
/// Cloning is not implemented on purpose: the state is shared by `Arc` so that
/// the scheduler, the registry and the writer all observe one atomic.
#[derive(Debug)]
pub struct TransferRuntimeConfig {
    /// Value from the configuration file, restored by `reset_worker_task_limit`.
    file_worker_task_limit: usize,
    /// Effective ceiling. Read on every dispatch decision (never cached).
    worker_task_limit: AtomicUsize,
    /// Value restored by `reset_worker_inflight_budget_bytes`.
    file_worker_inflight_budget_bytes: i64,
    /// Effective per-worker byte ceiling. Read on every dispatch decision.
    worker_inflight_budget_bytes: AtomicI64,
    /// Value from the configuration file, restored by `reset_max_tasks_per_transfer`.
    file_max_tasks_per_transfer: usize,
    /// Effective per-job task cap. Read on every planning cap check.
    max_tasks_per_transfer: AtomicUsize,
    /// Value from the configuration file, restored by `reset_max_plan_entries`.
    file_max_plan_entries: usize,
    /// Effective collected-entry cap. Read when a planning pass starts.
    max_plan_entries: AtomicUsize,
    /// Value from the configuration file, restored by `reset_accept_jobs`.
    file_accept_jobs: bool,
    /// Effective submit brake. Read on every submission, never cached.
    accept_jobs: AtomicBool,
    /// Value from the configuration file, restored by `reset_max_running_transfers`.
    file_max_running_transfers: usize,
    /// Effective executing-transfer ceiling. Read on every scheduler acquire.
    max_running_transfers: AtomicUsize,
}

impl TransferRuntimeConfig {
    /// Captures the configuration file's value as the reset target.
    pub fn from_conf(conf: &ClusterConf) -> Self {
        let mut state = Self::with_limits(
            conf.job.worker_max_concurrent_tasks,
            conf.transfer.max_tasks_per_transfer,
            conf.transfer.max_plan_entries,
        );
        // No configuration-file field: the default is the constant the worker
        // uses too, so the two sides agree until an operator tunes the transfer.
        let budget = TransferConf::DEFAULT_WORKER_INFLIGHT_BUDGET_BYTES;
        state.file_worker_inflight_budget_bytes = budget;
        state.worker_inflight_budget_bytes = AtomicI64::new(budget);
        state.file_accept_jobs = conf.transfer.accepts_jobs();
        state.accept_jobs = AtomicBool::new(conf.transfer.accepts_jobs());
        state.file_max_running_transfers = conf.transfer.max_running_transfers.max(1);
        state.max_running_transfers = AtomicUsize::new(conf.transfer.max_running_transfers.max(1));
        state
    }

    /// Builds the state from a bare limit, for callers that do not hold a
    /// `ClusterConf` (tests). The file value is the same limit.
    pub fn with_limit(limit: usize) -> Self {
        Self::with_limits(
            limit,
            TransferConf::DEFAULT_MAX_TASKS_PER_TRANSFER,
            TransferConf::DEFAULT_MAX_PLAN_ENTRIES,
        )
    }

    /// Builds the state from explicit limits; each is floored to 1 so a zero in
    /// the configuration file cannot park dispatch or block every plan.
    pub fn with_limits(
        worker_task_limit: usize,
        max_tasks_per_transfer: usize,
        max_plan_entries: usize,
    ) -> Self {
        Self {
            file_worker_task_limit: worker_task_limit,
            worker_task_limit: AtomicUsize::new(worker_task_limit.max(1)),
            file_worker_inflight_budget_bytes: TransferConf::DEFAULT_WORKER_INFLIGHT_BUDGET_BYTES,
            worker_inflight_budget_bytes: AtomicI64::new(
                TransferConf::DEFAULT_WORKER_INFLIGHT_BUDGET_BYTES,
            ),
            file_max_tasks_per_transfer: max_tasks_per_transfer,
            max_tasks_per_transfer: AtomicUsize::new(max_tasks_per_transfer.max(1)),
            file_max_plan_entries: max_plan_entries,
            max_plan_entries: AtomicUsize::new(max_plan_entries.max(1)),
            file_accept_jobs: true,
            accept_jobs: AtomicBool::new(true),
            file_max_running_transfers: TransferConf::DEFAULT_MAX_RUNNING_TRANSFERS,
            max_running_transfers: AtomicUsize::new(TransferConf::DEFAULT_MAX_RUNNING_TRANSFERS),
        }
    }

    /// Effective per-worker concurrency ceiling.
    pub fn worker_task_limit(&self) -> usize {
        self.worker_task_limit.load(Ordering::Acquire)
    }

    /// Effective per-worker in-flight byte ceiling.
    pub fn worker_inflight_budget_bytes(&self) -> i64 {
        self.worker_inflight_budget_bytes.load(Ordering::Acquire)
    }

    /// Applies a new per-worker byte ceiling and returns the previous one.
    pub fn update_worker_inflight_budget_bytes(&self, target: i64) -> FsResult<i64> {
        if target <= 0 {
            return Err(FsError::common(format!(
                "{} must be greater than zero",
                WORKER_INFLIGHT_BUDGET_CONFIG
            )));
        }
        Ok(self
            .worker_inflight_budget_bytes
            .swap(target, Ordering::AcqRel))
    }

    /// Restores the default budget and returns it.
    pub fn reset_worker_inflight_budget_bytes(&self) -> i64 {
        let restored = self.file_worker_inflight_budget_bytes.max(1);
        self.worker_inflight_budget_bytes
            .store(restored, Ordering::Release);
        restored
    }

    /// Effective per-job planned-task ceiling.
    pub fn max_tasks_per_transfer(&self) -> usize {
        self.max_tasks_per_transfer.load(Ordering::Acquire)
    }

    /// Applies a new per-job task cap and returns the previous one.
    pub fn update_max_tasks_per_transfer(&self, target: usize) -> FsResult<usize> {
        if target == 0 {
            return Err(FsError::common(format!(
                "{} must be greater than zero",
                MAX_TASKS_PER_TRANSFER_CONFIG
            )));
        }
        Ok(self.max_tasks_per_transfer.swap(target, Ordering::AcqRel))
    }

    /// Restores the configuration file's per-job task cap and returns it.
    pub fn reset_max_tasks_per_transfer(&self) -> usize {
        let restored = self.file_max_tasks_per_transfer.max(1);
        self.max_tasks_per_transfer
            .store(restored, Ordering::Release);
        restored
    }

    /// Effective collected-entry ceiling for one planning pass.
    pub fn max_plan_entries(&self) -> usize {
        self.max_plan_entries.load(Ordering::Acquire)
    }

    /// Applies a new collected-entry ceiling and returns the previous one.
    pub fn update_max_plan_entries(&self, target: usize) -> FsResult<usize> {
        if target == 0 {
            return Err(FsError::common(format!(
                "{} must be greater than zero",
                MAX_PLAN_ENTRIES_CONFIG
            )));
        }
        Ok(self.max_plan_entries.swap(target, Ordering::AcqRel))
    }

    /// Restores the configuration file's collected-entry ceiling and returns it.
    pub fn reset_max_plan_entries(&self) -> usize {
        let restored = self.file_max_plan_entries.max(1);
        self.max_plan_entries.store(restored, Ordering::Release);
        restored
    }

    /// Applies a new ceiling and returns the previous one.
    pub fn update_worker_task_limit(&self, target: usize) -> FsResult<usize> {
        if target == 0 {
            return Err(FsError::common(format!(
                "{} must be greater than zero",
                WORKER_MAX_CONCURRENT_TASKS_CONFIG
            )));
        }
        Ok(self.worker_task_limit.swap(target, Ordering::AcqRel))
    }

    /// Restores the configuration file's value and returns it.
    ///
    /// Returns the *restored* value, not the previous one: the registry hands
    /// this value straight back to the caller as the new effective value, and a
    /// `swap`-style previous value would make the endpoint report the value it
    /// just replaced.
    pub fn reset_worker_task_limit(&self) -> usize {
        let restored = self.file_worker_task_limit.max(1);
        self.worker_task_limit.store(restored, Ordering::Release);
        restored
    }
}

impl TransferRuntimeConfig {
    /// True while this instance accepts new submissions.
    pub fn accepts_jobs(&self) -> bool {
        self.accept_jobs.load(Ordering::Acquire)
    }

    /// Applies the submit brake and returns the previous value.
    pub fn update_accept_jobs(&self, target: bool) -> bool {
        self.accept_jobs.swap(target, Ordering::AcqRel)
    }

    /// Restores the configuration file's brake state.
    pub fn reset_accept_jobs(&self) -> bool {
        self.accept_jobs
            .store(self.file_accept_jobs, Ordering::Release);
        self.file_accept_jobs
    }

    /// Effective ceiling on concurrently executing transfers.
    pub fn max_running_transfers(&self) -> usize {
        self.max_running_transfers.load(Ordering::Acquire)
    }

    /// Applies a new executing-transfer ceiling and returns the previous one.
    pub fn update_max_running_transfers(&self, target: usize) -> FsResult<usize> {
        if target == 0 {
            return Err(FsError::common(format!(
                "{} must be greater than zero",
                MAX_RUNNING_TRANSFERS_CONFIG
            )));
        }
        Ok(self.max_running_transfers.swap(target, Ordering::AcqRel))
    }

    /// Restores the configuration file's executing-transfer ceiling.
    pub fn reset_max_running_transfers(&self) -> usize {
        let restored = self.file_max_running_transfers.max(1);
        self.max_running_transfers
            .store(restored, Ordering::Release);
        restored
    }
}

/// Registry of the transfer role's runtime-writable knobs.
///
/// Matches the worker registry's shape (`register_at` with the dotted config
/// path) so both roles report the same key against the same file field.
pub fn build_transfer_runtime_registry(
    state: Arc<TransferRuntimeConfig>,
    conf: &ClusterConf,
) -> RuntimeConfigRegistry {
    let file_worker_task_limit = conf.job.worker_max_concurrent_tasks;
    let file_max_tasks_per_transfer = conf.transfer.max_tasks_per_transfer;
    let file_max_plan_entries = conf.transfer.max_plan_entries;
    let file_accept_jobs = conf.transfer.accepts_jobs();
    let file_max_running_transfers = conf.transfer.max_running_transfers;

    RuntimeConfigRegistry::builder()
        .register_at(
            WORKER_MAX_CONCURRENT_TASKS_CONFIG,
            // Backed by `JobConf::worker_max_concurrent_tasks`, i.e. `[job].*`.
            // Without this declaration the config view would treat the key as a
            // runtime-only knob and silently drop the field from the view.
            Some("job.worker_max_concurrent_tasks"),
            move || Value::from(file_worker_task_limit),
            {
                let state = state.clone();
                move || Value::from(state.worker_task_limit())
            },
            {
                let state = state.clone();
                move |raw| {
                    let target = parse_usize(WORKER_MAX_CONCURRENT_TASKS_CONFIG, raw)?;
                    let previous = match state.update_worker_task_limit(target) {
                        Ok(v) => v,
                        Err(e) => return err_box!(e.to_string()),
                    };
                    Ok((Value::from(previous), Value::from(target)))
                }
            },
            {
                let state = state.clone();
                move || Ok(Value::from(state.reset_worker_task_limit()))
            },
        )
        .register_at(
            WORKER_INFLIGHT_BUDGET_CONFIG,
            // No configuration-file entry: the file value is the constant the
            // worker also uses, so this stays runtime-writable without claiming
            // a config path (same shape as the worker's own key).
            None,
            {
                let state = state.clone();
                move || Value::from(state.worker_inflight_budget_bytes())
            },
            {
                let state = state.clone();
                move || Value::from(state.worker_inflight_budget_bytes())
            },
            {
                let state = state.clone();
                move |raw| {
                    let target = parse_bytes(WORKER_INFLIGHT_BUDGET_CONFIG, raw)?;
                    let previous = match state.update_worker_inflight_budget_bytes(target) {
                        Ok(v) => v,
                        Err(e) => return err_box!(e.to_string()),
                    };
                    Ok((Value::from(previous), Value::from(target)))
                }
            },
            {
                let state = state.clone();
                move || Ok(Value::from(state.reset_worker_inflight_budget_bytes()))
            },
        )
        .register_at(
            MAX_TASKS_PER_TRANSFER_CONFIG,
            // Backed by `TransferConf::max_tasks_per_transfer`, i.e. `[transfer].*`.
            Some("transfer.max_tasks_per_transfer"),
            move || Value::from(file_max_tasks_per_transfer),
            {
                let state = state.clone();
                move || Value::from(state.max_tasks_per_transfer())
            },
            {
                let state = state.clone();
                move |raw| {
                    let target = parse_usize(MAX_TASKS_PER_TRANSFER_CONFIG, raw)?;
                    let previous = match state.update_max_tasks_per_transfer(target) {
                        Ok(v) => v,
                        Err(e) => return err_box!(e.to_string()),
                    };
                    Ok((Value::from(previous), Value::from(target)))
                }
            },
            {
                let state = state.clone();
                move || Ok(Value::from(state.reset_max_tasks_per_transfer()))
            },
        )
        .register_at(
            MAX_PLAN_ENTRIES_CONFIG,
            // Backed by `TransferConf::max_plan_entries`, i.e. `[transfer].*`.
            Some("transfer.max_plan_entries"),
            move || Value::from(file_max_plan_entries),
            {
                let state = state.clone();
                move || Value::from(state.max_plan_entries())
            },
            {
                let state = state.clone();
                move |raw| {
                    let target = parse_usize(MAX_PLAN_ENTRIES_CONFIG, raw)?;
                    let previous = match state.update_max_plan_entries(target) {
                        Ok(v) => v,
                        Err(e) => return err_box!(e.to_string()),
                    };
                    Ok((Value::from(previous), Value::from(target)))
                }
            },
            {
                let state = state.clone();
                move || Ok(Value::from(state.reset_max_plan_entries()))
            },
        )
        .register_at(
            ACCEPT_JOBS_CONFIG,
            // Backed by `TransferConf::accept_jobs`, i.e. `[transfer].*`.
            Some("transfer.accept_jobs"),
            move || Value::from(file_accept_jobs),
            {
                let state = state.clone();
                move || Value::from(state.accepts_jobs())
            },
            {
                let state = state.clone();
                move |raw| {
                    let target = parse_bool(ACCEPT_JOBS_CONFIG, raw)?;
                    let previous = state.update_accept_jobs(target);
                    Ok((Value::from(previous), Value::from(target)))
                }
            },
            {
                let state = state.clone();
                move || Ok(Value::from(state.reset_accept_jobs()))
            },
        )
        .register_at(
            MAX_RUNNING_TRANSFERS_CONFIG,
            // Backed by `TransferConf::max_running_transfers`, i.e. `[transfer].*`.
            Some("transfer.max_running_transfers"),
            move || Value::from(file_max_running_transfers),
            {
                let state = state.clone();
                move || Value::from(state.max_running_transfers())
            },
            {
                let state = state.clone();
                move |raw| {
                    let target = parse_usize(MAX_RUNNING_TRANSFERS_CONFIG, raw)?;
                    let previous = match state.update_max_running_transfers(target) {
                        Ok(v) => v,
                        Err(e) => return err_box!(e.to_string()),
                    };
                    Ok((Value::from(previous), Value::from(target)))
                }
            },
            {
                let state = state.clone();
                move || Ok(Value::from(state.reset_max_running_transfers()))
            },
        )
        .build()
}

fn parse_bool(config: &str, value: &str) -> CommonResult<bool> {
    match value.trim().to_ascii_lowercase().as_str() {
        "true" | "1" | "on" | "yes" => Ok(true),
        "false" | "0" | "off" | "no" => Ok(false),
        _ => err_box!(
            "invalid {} value {:?}: must be true or false",
            config,
            value
        ),
    }
}

/// Byte ceilings take the same spellings the configuration file uses.
///
/// `10GB`, `512MB`, `1.5GB` and a plain byte count all parse through
/// [`ByteUnit`], because typing `10737418240` is both unwieldy and easy to get
/// wrong by a digit. The stored value is always bytes.
fn parse_bytes(config: &str, value: &str) -> CommonResult<i64> {
    let bytes = match ByteUnit::from_str(value) {
        Ok(v) => v,
        Err(error) => return err_box!("invalid {} value {:?}: {}", config, value, error),
    };
    let bytes = match i64::try_from(bytes.as_byte()) {
        Ok(v) => v,
        Err(_) => return err_box!("{} exceeds the maximum supported byte value", config),
    };
    if bytes <= 0 {
        return err_box!("{} must be greater than zero", config);
    }
    Ok(bytes)
}

fn parse_usize(config: &str, value: &str) -> CommonResult<usize> {
    let parsed: usize = match value.parse() {
        Ok(v) => v,
        Err(_) => {
            return err_box!(
                "invalid {} value {:?}: must be a positive integer",
                config,
                value
            );
        }
    };
    if parsed == 0 {
        return err_box!("{} must be greater than zero", config);
    }
    Ok(parsed)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accept_jobs_is_a_live_brake_that_resets_to_the_file_value() {
        let mut conf = ClusterConf::default();
        conf.transfer.accept_jobs = true;
        let state = Arc::new(TransferRuntimeConfig::from_conf(&conf));
        let registry = build_transfer_runtime_registry(state.clone(), &conf);
        assert!(state.accepts_jobs());

        let closed = registry.set(ACCEPT_JOBS_CONFIG, "false", None).unwrap();
        assert_eq!(closed.previous_value, Value::from(true));
        assert_eq!(closed.value, Value::from(false));
        assert!(!state.accepts_jobs());

        // Anything outside the accepted spellings is refused instead of guessed:
        // a typo must not silently reopen submissions.
        assert!(registry.set(ACCEPT_JOBS_CONFIG, "maybe", None).is_err());
        assert!(!state.accepts_jobs());

        registry.reset(ACCEPT_JOBS_CONFIG, None).unwrap();
        assert!(state.accepts_jobs());
    }

    #[test]
    fn accept_jobs_starts_from_the_file_value() {
        // An instance booted with submissions closed stays closed until an
        // operator opens it, and reset returns to that file value.
        let mut conf = ClusterConf::default();
        conf.transfer.accept_jobs = false;
        let state = Arc::new(TransferRuntimeConfig::from_conf(&conf));
        let registry = build_transfer_runtime_registry(state.clone(), &conf);
        assert!(!state.accepts_jobs());

        registry.set(ACCEPT_JOBS_CONFIG, "true", None).unwrap();
        assert!(state.accepts_jobs());
        registry.reset(ACCEPT_JOBS_CONFIG, None).unwrap();
        assert!(!state.accepts_jobs());
    }

    #[test]
    fn max_running_transfers_is_live_and_cannot_be_zero() {
        let conf = ClusterConf::default();
        let state = Arc::new(TransferRuntimeConfig::from_conf(&conf));
        let registry = build_transfer_runtime_registry(state.clone(), &conf);
        assert_eq!(
            state.max_running_transfers(),
            conf.transfer.max_running_transfers
        );

        registry
            .set(MAX_RUNNING_TRANSFERS_CONFIG, "2", None)
            .unwrap();
        assert_eq!(state.max_running_transfers(), 2);
        // Zero would park every job forever; lowering below the running count is
        // allowed and simply drains as jobs finish.
        assert!(registry
            .set(MAX_RUNNING_TRANSFERS_CONFIG, "0", None)
            .is_err());
        assert_eq!(state.max_running_transfers(), 2);

        registry.reset(MAX_RUNNING_TRANSFERS_CONFIG, None).unwrap();
        assert_eq!(
            state.max_running_transfers(),
            conf.transfer.max_running_transfers
        );
    }

    #[test]
    fn registry_set_and_reset_move_the_live_value() {
        let conf = ClusterConf::default();
        let state = Arc::new(TransferRuntimeConfig::from_conf(&conf));
        let registry = build_transfer_runtime_registry(state.clone(), &conf);
        assert_eq!(
            state.worker_task_limit(),
            conf.job.worker_max_concurrent_tasks
        );

        let updated = registry
            .set(WORKER_MAX_CONCURRENT_TASKS_CONFIG, "7", None)
            .unwrap();
        assert_eq!(state.worker_task_limit(), 7);
        assert_eq!(
            updated.previous_value,
            Value::from(conf.job.worker_max_concurrent_tasks)
        );

        // A zero ceiling would park every worker forever: the write is refused
        // and the effective value stays where it was.
        assert!(registry
            .set(WORKER_MAX_CONCURRENT_TASKS_CONFIG, "0", None)
            .is_err());
        assert!(registry
            .set(WORKER_MAX_CONCURRENT_TASKS_CONFIG, "many", None)
            .is_err());
        assert_eq!(state.worker_task_limit(), 7);

        registry
            .reset(WORKER_MAX_CONCURRENT_TASKS_CONFIG, None)
            .unwrap();
        assert_eq!(
            state.worker_task_limit(),
            conf.job.worker_max_concurrent_tasks
        );
    }

    #[test]
    fn update_rejects_zero_and_reports_the_previous_value() {
        let state = TransferRuntimeConfig::with_limit(3);
        assert!(state.update_worker_task_limit(0).is_err());
        assert_eq!(state.worker_task_limit(), 3);
        assert_eq!(state.update_worker_task_limit(5).unwrap(), 3);
        assert_eq!(state.worker_task_limit(), 5);
        assert_eq!(state.reset_worker_task_limit(), 3);
        assert_eq!(state.worker_task_limit(), 3);
    }

    #[test]
    fn zero_file_value_still_yields_a_usable_ceiling() {
        // `with_limit(0)` mirrors a config file that says 0: the effective
        // ceiling is floored to 1 rather than disabling dispatch entirely.
        let state = TransferRuntimeConfig::with_limit(0);
        assert_eq!(state.worker_task_limit(), 1);
        assert_eq!(state.reset_worker_task_limit(), 1);
    }

    /// The task cap is a separate knob from the worker ceiling: updating one
    /// must not disturb the other.
    #[test]
    fn task_cap_and_worker_ceiling_are_independent() {
        let state = TransferRuntimeConfig::with_limits(4, 100_000, 500_000);
        assert_eq!(state.worker_task_limit(), 4);
        assert_eq!(state.max_tasks_per_transfer(), 100_000);

        assert_eq!(
            state.update_max_tasks_per_transfer(250_000).unwrap(),
            100_000
        );
        assert_eq!(state.max_tasks_per_transfer(), 250_000);
        assert_eq!(state.worker_task_limit(), 4);

        assert_eq!(state.reset_max_tasks_per_transfer(), 100_000);
    }

    /// The collected-entry ceiling is its own knob: it bounds how wide a source
    /// tree one job may walk and must not move the task cap with it.
    #[test]
    fn plan_entry_cap_is_independent_and_rejects_zero() {
        let state = TransferRuntimeConfig::with_limits(1, 100_000, 300_000);
        assert_eq!(state.max_plan_entries(), 300_000);

        assert_eq!(state.update_max_plan_entries(500_000).unwrap(), 300_000);
        assert_eq!(state.max_plan_entries(), 500_000);
        assert_eq!(state.max_tasks_per_transfer(), 100_000);

        assert!(state.update_max_plan_entries(0).is_err());
        assert_eq!(state.max_plan_entries(), 500_000);

        assert_eq!(state.reset_max_plan_entries(), 300_000);
    }

    /// Wide partitions need a raisable entry cap; it is exposed on the same
    /// registry the UpdateConfigs endpoint drives.
    #[test]
    fn registry_updates_the_plan_entry_cap() {
        let conf = ClusterConf::default();
        let state = Arc::new(TransferRuntimeConfig::from_conf(&conf));
        let registry = build_transfer_runtime_registry(state.clone(), &conf);
        let original = conf.transfer.max_plan_entries;

        let updated = registry
            .set(MAX_PLAN_ENTRIES_CONFIG, "800000", None)
            .unwrap();
        assert_eq!(state.max_plan_entries(), 800_000);
        assert_eq!(updated.previous_value, Value::from(original));
        assert_eq!(
            registry.config_path(MAX_PLAN_ENTRIES_CONFIG),
            Some("transfer.max_plan_entries")
        );

        assert!(registry.set(MAX_PLAN_ENTRIES_CONFIG, "0", None).is_err());
        assert_eq!(state.max_plan_entries(), 800_000);

        registry.reset(MAX_PLAN_ENTRIES_CONFIG, None).unwrap();
        assert_eq!(state.max_plan_entries(), original);
    }

    /// A zero cap would reject every non-empty plan, so writes refuse it and the
    /// effective value stays where it was.
    #[test]
    fn task_cap_update_rejects_zero() {
        let state = TransferRuntimeConfig::with_limits(1, 10, 500_000);
        assert!(state.update_max_tasks_per_transfer(0).is_err());
        assert_eq!(state.max_tasks_per_transfer(), 10);
    }

    /// The cap is hot-updatable through the same registry the UpdateConfigs
    /// endpoint drives, and it is reported against its on-disk config path.
    #[test]
    fn registry_updates_the_task_cap() {
        let conf = ClusterConf::default();
        let state = Arc::new(TransferRuntimeConfig::from_conf(&conf));
        let registry = build_transfer_runtime_registry(state.clone(), &conf);
        let original = conf.transfer.max_tasks_per_transfer;

        let updated = registry
            .set(MAX_TASKS_PER_TRANSFER_CONFIG, "200000", None)
            .unwrap();
        assert_eq!(state.max_tasks_per_transfer(), 200_000);
        assert_eq!(updated.previous_value, Value::from(original));
        assert_eq!(
            registry.config_path(MAX_TASKS_PER_TRANSFER_CONFIG),
            Some("transfer.max_tasks_per_transfer")
        );

        assert!(registry
            .set(MAX_TASKS_PER_TRANSFER_CONFIG, "0", None)
            .is_err());
        assert_eq!(state.max_tasks_per_transfer(), 200_000);

        registry.reset(MAX_TASKS_PER_TRANSFER_CONFIG, None).unwrap();
        assert_eq!(state.max_tasks_per_transfer(), original);
    }
    /// The byte ceiling is a transfer-side knob only: the value a worker
    /// reports must not widen or narrow it, and a pushed value has to be
    /// visible to the very next dispatch decision.
    #[test]
    fn worker_inflight_budget_is_a_live_transfer_knob() {
        let conf = ClusterConf::default();
        let state = Arc::new(TransferRuntimeConfig::from_conf(&conf));
        let registry = build_transfer_runtime_registry(state.clone(), &conf);
        assert_eq!(
            state.worker_inflight_budget_bytes(),
            TransferConf::DEFAULT_WORKER_INFLIGHT_BUDGET_BYTES
        );

        // Operators type a size, not ten digits: the stored value is bytes.
        let pushed = registry
            .set(WORKER_INFLIGHT_BUDGET_CONFIG, "1MB", None)
            .unwrap();
        assert_eq!(pushed.value, Value::from(1048576i64));
        assert_eq!(state.worker_inflight_budget_bytes(), 1048576);
        registry
            .set(WORKER_INFLIGHT_BUDGET_CONFIG, "2gb", None)
            .unwrap();
        assert_eq!(state.worker_inflight_budget_bytes(), 2 * 1024 * 1024 * 1024);
        registry
            .set(WORKER_INFLIGHT_BUDGET_CONFIG, "524288", None)
            .unwrap();
        assert_eq!(state.worker_inflight_budget_bytes(), 524288);

        // Zero and garbage are refused instead of silently disabling the gate.
        assert!(registry
            .set(WORKER_INFLIGHT_BUDGET_CONFIG, "0", None)
            .is_err());
        assert!(registry
            .set(WORKER_INFLIGHT_BUDGET_CONFIG, "10 buckets", None)
            .is_err());
        assert_eq!(state.worker_inflight_budget_bytes(), 524288);

        let reset = registry.reset(WORKER_INFLIGHT_BUDGET_CONFIG, None).unwrap();
        assert_eq!(
            reset,
            Value::from(TransferConf::DEFAULT_WORKER_INFLIGHT_BUDGET_BYTES)
        );
        assert_eq!(
            state.worker_inflight_budget_bytes(),
            TransferConf::DEFAULT_WORKER_INFLIGHT_BUDGET_BYTES
        );
    }
}
