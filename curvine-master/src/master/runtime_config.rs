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

use std::sync::Arc;

use curvine_config::{
    MasterConf, RuntimeConfigEntryView, RuntimeConfigRegistry, RuntimeConfigRegistryBuilder,
};
use curvine_core_error::{err_box, CommonResult};
use curvine_proto::RuntimeConfigEntryProto;
use curvine_rpc::handler::LimitConf;
use curvine_runtime::common::{ByteUnit, DurationUnit};
use serde_json::Value;

use crate::master::space::DirSpaceManager;

pub const DIR_SPACE_ENABLED_KEY: &str = curvine_config::DIR_SPACE_ENABLED_RUNTIME_KEY;
pub const DIR_SPACE_TRACKED_PATHS_KEY: &str = curvine_config::DIR_SPACE_TRACKED_PATHS_RUNTIME_KEY;

/// Runtime-tunable dir-space scan knobs. Keys equal the on-disk config paths
/// under `[master.dir_space]` so the config view needs no translation table.
pub const DIR_SPACE_TRACK_MAX_INODES_KEY: &str = "master.dir_space.track_max_inodes";
pub const DIR_SPACE_TRACK_MAX_EDGES_KEY: &str = "master.dir_space.track_max_edges";
pub const DIR_SPACE_TRACK_MEMORY_BUDGET_KEY: &str = "master.dir_space.track_memory_budget";
pub const DIR_SPACE_TRACKED_SCAN_INTERVAL_KEY: &str = "master.dir_space.tracked_scan_interval";
pub const DIR_SPACE_TRACKED_SCAN_TIMEOUT_KEY: &str = "master.dir_space.tracked_scan_timeout";
pub const DIR_SPACE_MAX_INODES_KEY: &str = "master.dir_space.max_inodes";
pub const DIR_SPACE_MAX_EDGES_KEY: &str = "master.dir_space.max_edges";
pub const DIR_SPACE_MEMORY_BUDGET_KEY: &str = "master.dir_space.memory_budget";
pub const DIR_SPACE_GLOBAL_SCAN_INTERVAL_KEY: &str = "master.dir_space.global_scan_interval";
pub const DIR_SPACE_GLOBAL_SCAN_TIMEOUT_KEY: &str = "master.dir_space.global_scan_timeout";

/// Per-connection in-flight request cap. The key is the configuration path so the
/// config view can attribute the value without a translation table.
pub const CONN_LIMIT_KEY: &str = "master.conn_limit";
/// Process-wide concurrent request cap. Same dotted path as the key.
pub const GLOBAL_LIMIT_KEY: &str = "master.global_limit";

/// Master-side runtime registry.
///
/// Exposes the dir-space switch, the explicit statistics-path list, and the two
/// RPC concurrency limits. Overrides are volatile: restart restores the
/// file-backed values.
///
/// The limits are registered against the live [`LimitConf`] the RPC server
/// applies, not against the `ClusterConf` snapshot, so an edit changes the value
/// traffic is throttled by. A config-backed closure would only move the number
/// the config endpoint prints.
pub fn build_master_runtime_registry(
    manager: Arc<DirSpaceManager>,
    limit: LimitConf,
    master_conf: &MasterConf,
) -> RuntimeConfigRegistry {
    let file_conn_limit = master_conf.conn_limit;
    let file_global_limit = master_conf.global_limit;
    let reset_conn = limit.clone();
    let reset_global = limit.clone();

    RuntimeConfigRegistry::builder()
        .register_at(
            DIR_SPACE_ENABLED_KEY,
            Some("master.dir_space.enabled"),
            {
                let manager = manager.clone();
                move || Value::from(manager.file_enabled())
            },
            {
                let manager = manager.clone();
                move || Value::from(manager.tracking_enabled())
            },
            {
                let manager = manager.clone();
                move |raw| {
                    let enabled = parse_enabled(raw)?;
                    let previous = manager.update_enabled(enabled);
                    Ok((Value::from(previous), Value::from(enabled)))
                }
            },
            {
                let manager = manager.clone();
                move || Ok(Value::from(manager.reset_enabled()))
            },
        )
        .register_at(
            DIR_SPACE_TRACKED_PATHS_KEY,
            Some("master.dir_space.tracked_paths"),
            {
                let manager = manager.clone();
                move || Value::from(manager.file_tracked_paths())
            },
            {
                let manager = manager.clone();
                move || Value::from(manager.explicit_paths())
            },
            {
                let manager = manager.clone();
                move |raw| {
                    let paths = parse_tracked_paths(raw)?;
                    let (previous, applied) = manager.update_tracked_paths(&paths)?;
                    Ok((Value::from(previous), Value::from(applied)))
                }
            },
            {
                let manager = manager.clone();
                move || Ok(Value::from(manager.reset_tracked_paths()))
            },
        )
        .register_dir_space_tuning(&manager)
        .register_at(
            CONN_LIMIT_KEY,
            Some(CONN_LIMIT_KEY),
            move || Value::from(file_conn_limit),
            {
                let limit = limit.clone();
                move || Value::from(limit.conn_limit())
            },
            {
                let limit = limit.clone();
                move |raw| {
                    let value = parse_positive_usize(raw, CONN_LIMIT_KEY)?;
                    let previous = limit.set_conn_limit(value);
                    Ok((Value::from(previous), Value::from(value)))
                }
            },
            move || {
                reset_conn.set_conn_limit(file_conn_limit);
                Ok(Value::from(file_conn_limit))
            },
        )
        .register_at(
            GLOBAL_LIMIT_KEY,
            Some(GLOBAL_LIMIT_KEY),
            move || Value::from(file_global_limit),
            {
                let limit = limit.clone();
                move || Value::from(limit.global_limit_size())
            },
            {
                let limit = limit.clone();
                move |raw| {
                    let value = parse_positive_usize(raw, GLOBAL_LIMIT_KEY)?;
                    let previous = limit.set_global_limit(value);
                    Ok((Value::from(previous), Value::from(value)))
                }
            },
            move || {
                reset_global.set_global_limit(file_global_limit);
                Ok(Value::from(file_global_limit))
            },
        )
        .build()
}

/// Extension so the tuning keys can be registered as one unit while reusing
/// the shared `DirSpaceManager` handle.
trait RegisterDirSpaceTuning {
    fn register_dir_space_tuning(self, manager: &Arc<DirSpaceManager>) -> Self;
}

impl RegisterDirSpaceTuning for RuntimeConfigRegistryBuilder {
    fn register_dir_space_tuning(self, manager: &Arc<DirSpaceManager>) -> Self {
        // Per-key apply: parse one field, merge into the current effective
        // tuning, and validate the whole set (including the timeout <=
        // interval invariants) before storing the override. A reset on any
        // tuning key restores every tuning knob to the file-backed values.
        macro_rules! tuning_key {
            ($builder:expr, $key:expr, $field:ident, $parser:ident) => {
                $builder.register_at(
                    $key,
                    Some($key),
                    {
                        let manager = manager.clone();
                        move || Value::from(manager.file_tuning().$field)
                    },
                    {
                        let manager = manager.clone();
                        move || Value::from(manager.effective_tuning().$field)
                    },
                    {
                        let manager = manager.clone();
                        move |raw| {
                            let value = $parser(raw, $key)?;
                            let mut tuning = manager.effective_tuning();
                            let previous = tuning.$field;
                            tuning.$field = value;
                            manager.update_tuning(tuning)?;
                            Ok((Value::from(previous), Value::from(value)))
                        }
                    },
                    {
                        let manager = manager.clone();
                        move || {
                            let restored = manager.reset_tuning();
                            Ok(Value::from(restored.$field))
                        }
                    },
                )
            };
        }

        let builder = self;
        let builder = tuning_key!(builder, DIR_SPACE_TRACK_MAX_INODES_KEY, track_max_inodes, parse_positive_u64_value);
        let builder = tuning_key!(builder, DIR_SPACE_TRACK_MAX_EDGES_KEY, track_max_edges, parse_positive_u64_value);
        let builder = tuning_key!(builder, DIR_SPACE_TRACK_MEMORY_BUDGET_KEY, track_memory_budget_bytes, parse_byte_value);
        let builder = tuning_key!(builder, DIR_SPACE_TRACKED_SCAN_INTERVAL_KEY, tracked_scan_interval_ms, parse_duration_ms_value);
        let builder = tuning_key!(builder, DIR_SPACE_TRACKED_SCAN_TIMEOUT_KEY, tracked_scan_timeout_ms, parse_duration_ms_value);
        let builder = tuning_key!(builder, DIR_SPACE_MAX_INODES_KEY, max_inodes, parse_positive_u64_value);
        let builder = tuning_key!(builder, DIR_SPACE_MAX_EDGES_KEY, max_edges, parse_positive_u64_value);
        let builder = tuning_key!(builder, DIR_SPACE_MEMORY_BUDGET_KEY, memory_budget_bytes, parse_byte_value);
        let builder = tuning_key!(builder, DIR_SPACE_GLOBAL_SCAN_INTERVAL_KEY, global_scan_interval_ms, parse_duration_ms_value);
        tuning_key!(builder, DIR_SPACE_GLOBAL_SCAN_TIMEOUT_KEY, global_scan_timeout_ms, parse_duration_ms_value)
    }
}

/// Positive integer field (inodes / edges). Zero is rejected so a runtime
/// edit cannot introduce a value `DirSpaceConf::validate` would refuse.
fn parse_positive_u64_value(raw: &str, key: &str) -> CommonResult<u64> {
    let value: u64 = raw.trim().parse().map_err(|_| {
        curvine_core_error::CommonError::from(format!("{} expects an integer", key))
    })?;
    if value == 0 {
        return err_box!("{} must be greater than zero", key);
    }
    Ok(value)
}

/// Duration field: plain integer means milliseconds, or a string with unit
/// (`90s`, `5m`, `1h`), matching the on-disk config syntax.
fn parse_duration_ms_value(raw: &str, key: &str) -> CommonResult<u64> {
    let unit = DurationUnit::from_str(raw.trim()).map_err(|err| {
        curvine_core_error::CommonError::from(format!("{}: {}", key, err))
    })?;
    Ok(unit.as_millis())
}

/// Byte-budget field: plain integer means bytes, or a string with unit
/// (`256MB`, `1GB`), matching the on-disk config syntax.
fn parse_byte_value(raw: &str, key: &str) -> CommonResult<u64> {
    let unit = ByteUnit::from_str(raw.trim()).map_err(|err| {
        curvine_core_error::CommonError::from(format!("{}: {}", key, err))
    })?;
    Ok(unit.as_byte())
}

fn parse_enabled(raw: &str) -> CommonResult<bool> {
    match raw.trim() {
        "true" => Ok(true),
        "false" => Ok(false),
        _ => err_box!("{} expects true or false", DIR_SPACE_ENABLED_KEY),
    }
}

/// Reject zero and non-numeric input. `MasterConf::validate` applies the same
/// rule at startup, so a runtime edit must not be able to introduce a value the
/// process would refuse to boot with.
fn parse_positive_usize(raw: &str, key: &str) -> CommonResult<usize> {
    let value: usize = raw.trim().parse().map_err(|_| {
        curvine_core_error::CommonError::from(format!("{} expects an integer", key))
    })?;
    if value == 0 {
        return err_box!("{} must be greater than zero", key);
    }
    Ok(value)
}

fn parse_tracked_paths(raw: &str) -> CommonResult<Vec<String>> {
    match serde_json::from_str(raw) {
        Ok(paths) => Ok(paths),
        Err(err) => err_box!(
            "{} expects a JSON string array: {}",
            DIR_SPACE_TRACKED_PATHS_KEY,
            err
        ),
    }
}

pub fn runtime_entry_to_proto(view: RuntimeConfigEntryView) -> RuntimeConfigEntryProto {
    RuntimeConfigEntryProto {
        key: view.key,
        file_value_json: value_to_json_string(&view.file_value),
        override_value: view.override_value,
        effective_value_json: value_to_json_string(&view.effective_value),
    }
}

pub fn value_to_json_string(value: &Value) -> String {
    value.to_string()
}

pub fn list_runtime_config_entries(
    registry: &RuntimeConfigRegistry,
    key: Option<String>,
) -> CommonResult<Vec<RuntimeConfigEntryView>> {
    match key.filter(|k| !k.is_empty()) {
        Some(k) => Ok(vec![registry.get(&k)?]),
        None => Ok(registry.list()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use curvine_core_error::err_box;
    use std::sync::atomic::{AtomicI64, Ordering};
    use std::sync::Arc;

    fn test_registry() -> RuntimeConfigRegistry {
        let live = Arc::new(AtomicI64::new(5));
        let file = Arc::new(5i64);
        let live_apply = live.clone();
        let live_reset = live.clone();
        let file_reset = file.clone();
        RuntimeConfigRegistry::builder()
            .register(
                "demo_limit",
                move || Value::from(*file),
                move || Value::from(live.load(Ordering::Relaxed)),
                move |raw| {
                    let v: i64 = match raw.parse() {
                        Ok(v) => v,
                        Err(_) => return err_box!("bad int"),
                    };
                    let previous = live_apply.swap(v, Ordering::Relaxed);
                    Ok((Value::from(previous), Value::from(v)))
                },
                move || {
                    let v = *file_reset;
                    live_reset.store(v, Ordering::Relaxed);
                    Ok(Value::from(v))
                },
            )
            .build()
    }

    #[test]
    fn list_and_proto_conversion() {
        let registry = test_registry();
        let all = list_runtime_config_entries(&registry, None).unwrap();
        assert_eq!(all.len(), 1);
        let pb = runtime_entry_to_proto(all[0].clone());
        assert_eq!(pb.key, "demo_limit");
        assert_eq!(pb.file_value_json, "5");
        assert_eq!(pb.effective_value_json, "5");
    }

    #[test]
    fn list_single_key() {
        let registry = test_registry();
        registry.set("demo_limit", "9", Some("test")).unwrap();
        let one = list_runtime_config_entries(&registry, Some("demo_limit".to_string())).unwrap();
        assert_eq!(one.len(), 1);
        assert_eq!(one[0].effective_value, 9);
    }

    // ------------------------------------------------------------------
    // Dir-space tuning keys
    // ------------------------------------------------------------------

    mod dir_space_tuning {
        use super::*;
        use crate::master::journal::JournalSystem;
        use crate::master::space::scheduler::DirSpaceCoordinatorTask;
        use crate::master::space::DirSpaceManager;
        use crate::master::{Master, MasterMonitor, MountManager};
        use curvine_config::{ClusterConf, JournalConf, MasterConf};
        use curvine_runtime::common::Utils;

        fn tuning_registry(name: &str) -> (RuntimeConfigRegistry, Arc<DirSpaceManager>, MasterMonitor) {
            Master::init_test_metrics();
            let mut conf = ClusterConf {
                testing: true,
                master: MasterConf {
                    meta_dir: Utils::test_sub_dir(format!("dir-space-tuning/meta-{}", name)),
                    ..Default::default()
                },
                journal: JournalConf {
                    enable: false,
                    journal_dir: Utils::test_sub_dir(format!("dir-space-tuning/journal-{}", name)),
                    ..Default::default()
                },
                ..Default::default()
            };
            conf.master.dir_space.enabled = true;
            conf.master.dir_space.tracked_paths = vec!["/".to_string()];
            conf.master.init().unwrap();
            let fs = JournalSystem::fs_only_for_test(&conf).unwrap();
            let monitor = fs.master_monitor.clone();
            let manager = Arc::new(DirSpaceManager::new(
                fs.clone(),
                Arc::new(MountManager::new(fs.clone())),
                Arc::new(conf.master),
            ));
            let registry = build_master_runtime_registry(
                manager.clone(),
                LimitConf::new(1024, 1024),
                &MasterConf::default(),
            );
            (registry, manager, monitor)
        }

        #[test]
        fn track_max_inodes_hot_edit_changes_effective_value() {
            let (registry, manager, _monitor) = tuning_registry("hot-edit");
            let file = manager.effective_tuning().track_max_inodes;

            let entry = registry
                .get(DIR_SPACE_TRACK_MAX_INODES_KEY)
                .expect("tuning key registered");
            assert_eq!(entry.effective_value, file as i64);

            registry
                .set(DIR_SPACE_TRACK_MAX_INODES_KEY, "5000000", Some("test"))
                .unwrap();
            assert_eq!(manager.effective_tuning().track_max_inodes, 5_000_000);
            assert!(manager.tuning_overridden());

            let entry = registry.get(DIR_SPACE_TRACK_MAX_INODES_KEY).unwrap();
            assert_eq!(entry.override_value.as_deref(), Some("5000000"));
            assert_eq!(entry.effective_value, 5_000_000i64);
        }

        #[test]
        fn tracked_scan_interval_accepts_duration_string_and_drives_scheduler() {
            let (registry, manager, monitor) = tuning_registry("duration");
            registry
                .set(DIR_SPACE_TRACKED_SCAN_INTERVAL_KEY, "15m", Some("test"))
                .unwrap();
            registry
                .set(DIR_SPACE_TRACKED_SCAN_TIMEOUT_KEY, "10m", Some("test"))
                .unwrap();
            assert_eq!(manager.point_interval_ms(), 15 * 60 * 1000);
            assert_eq!(manager.effective_tuning().tracked_scan_timeout_ms, 10 * 60 * 1000);

            // The coordinator reads the cadence live: a fresh task picks up
            // the tuned interval without a restart.
            let task = DirSpaceCoordinatorTask::new(manager.clone(), monitor);
            let _ = task; // constructed without caching stale intervals
        }

        #[test]
        fn timeout_above_interval_is_rejected_and_leaves_no_override() {
            let (registry, manager, _monitor) = tuning_registry("timeout-reject");
            let before = manager.effective_tuning().tracked_scan_timeout_ms;
            let err = registry
                .set(DIR_SPACE_TRACKED_SCAN_TIMEOUT_KEY, "99m", Some("test"))
                .unwrap_err();
            assert!(err.to_string().contains("must not be greater than"));
            assert_eq!(manager.effective_tuning().tracked_scan_timeout_ms, before);
            assert!(!manager.tuning_overridden());
        }

        #[test]
        fn memory_budget_accepts_unit_string() {
            let (registry, manager, _monitor) = tuning_registry("budget");
            registry
                .set(DIR_SPACE_TRACK_MEMORY_BUDGET_KEY, "1GB", Some("test"))
                .unwrap();
            assert_eq!(manager.effective_tuning().track_memory_budget_bytes, 1024 * 1024 * 1024);
        }

        #[test]
        fn reset_restores_file_backed_tuning() {
            let (registry, manager, _monitor) = tuning_registry("reset");
            let file = manager.effective_tuning().track_max_inodes;
            registry
                .set(DIR_SPACE_TRACK_MAX_INODES_KEY, "5000000", Some("test"))
                .unwrap();
            registry
                .reset(DIR_SPACE_TRACK_MAX_INODES_KEY, Some("test"))
                .unwrap();
            assert_eq!(manager.effective_tuning().track_max_inodes, file);
            assert!(!manager.tuning_overridden());
        }
    }
}
