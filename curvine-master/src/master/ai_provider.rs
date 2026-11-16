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

//! civi AI assistant data source for the master process.
//!
//! Reuses the same master state that backs the metrics/health endpoints, so
//! the agent never needs a second metadata client. The master serves exactly
//! one cluster (the cluster selector is ignored) and has no mount/job/transfer
//! management surface, so those tool groups fall back to the trait default
//! ("not supported"); the admin console's provider serves them.
//!
//! Write actions (`fs_operate`, `worker_operate`) go through the same
//! journaled state machines as the RPC/CLI paths, and only reach the model
//! after the console confirmation flow.

use std::sync::Arc;

use async_trait::async_trait;
use curvine_error::FsResult;
use curvine_model::{RenameFlags, SetAttrOpts, Trigger, TtlAction, WorkerInfo};
use curvine_web::ai::AiDataProvider;
use serde_json::{json, Value};

use crate::master::fs::MasterFilesystem;

pub struct MasterAiProvider {
    fs: MasterFilesystem,
    conf: Arc<curvine_config::ClusterConf>,
    /// Snapshot plus live registry behind the runtime-config actions.
    config_view: Arc<curvine_web::router::ConfigViewState>,
}

impl MasterAiProvider {
    pub fn new(
        fs: MasterFilesystem,
        conf: Arc<curvine_config::ClusterConf>,
        config_view: Arc<curvine_web::router::ConfigViewState>,
    ) -> Self {
        MasterAiProvider {
            fs,
            conf,
            config_view,
        }
    }

    /// Cluster overview shared with the web API surface.
    fn overview_json(&self) -> FsResult<Value> {
        let fs = &self.fs;
        let filesystem_info = fs.filesystem_info()?;

        let (files_total, dir_total) = {
            let (f, d) = fs.get_file_counts();
            (f as usize, d as usize)
        };

        let expected_capacity = filesystem_info.available + filesystem_info.fs_used;
        if filesystem_info.capacity != expected_capacity {
            log::warn!(
                "Capacity inconsistency detected: capacity={}, available={}, fs_used={}, expected_capacity={}",
                filesystem_info.capacity,
                filesystem_info.available,
                filesystem_info.fs_used,
                expected_capacity
            );
        }

        let master_state = format!("{:?}", fs.master_monitor.journal_state());

        Ok(json!({
            "cluster_id": self.conf.cluster_id,
            "master_addr": self.conf.master_addr().to_string(),
            "live_workers": filesystem_info.live_workers.len(),
            "lost_workers": filesystem_info.lost_workers.len(),
            "available": filesystem_info.available,
            "capacity": filesystem_info.capacity,
            "fs_used": filesystem_info.fs_used,
            "reserved_bytes": filesystem_info.reserved_bytes,
            "files_total": files_total,
            "dir_total": dir_total,
            "master_state": master_state,
        }))
    }

    fn ha_json(&self) -> FsResult<Value> {
        let fs = &self.fs;
        Ok(json!({
            "role": format!("{:?}", fs.master_monitor.journal_state()),
            "safe_mode": fs.master_monitor.is_safe_mode(),
            "ufs_caught_up": fs.master_monitor.ufs_caught_up(),
            "journal_addrs": self.conf.journal.journal_addrs.iter()
                .map(|peer| format!("{}:{}", peer.hostname, peer.port))
                .collect::<Vec<_>>(),
        }))
    }

    fn worker_json(&self, worker: &WorkerInfo, state: &str) -> Value {
        json!({
            "address": format!("{}:{}", worker.address.hostname, worker.address.rpc_port),
            "state": state,
            "capacity": worker.capacity,
            "available": worker.available,
            "fs_used": worker.fs_used,
        })
    }

    fn list_workers(&self) -> FsResult<Value> {
        let wm = self.fs.worker_manager.read();
        let live: Vec<Value> = wm
            .worker_map
            .workers()
            .iter()
            .map(|(_, w)| self.worker_json(w, "Normal"))
            .collect();
        let lost: Vec<Value> = wm
            .worker_map
            .lost_workers
            .iter()
            .map(|(_, w)| self.worker_json(w, "Lost"))
            .collect();
        Ok(json!({"live_workers": live, "lost_workers": lost}))
    }

    fn map_fs_err<T>(&self, result: FsResult<T>) -> Result<T, String> {
        result.map_err(|e| e.to_string())
    }

    fn arg_str<'a>(&self, args: &'a Value, key: &str, action: &str) -> Result<&'a str, String> {
        args.get(key)
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .ok_or_else(|| format!("action '{action}' requires a non-empty '{key}'"))
    }
}

#[async_trait]
impl AiDataProvider for MasterAiProvider {
    async fn cluster_insight(
        &self,
        _cluster: Option<&str>,
        action: &str,
        _args: &Value,
    ) -> Result<Value, String> {
        match action {
            "overview" => self.map_fs_err(self.overview_json()),
            "ha" => self.map_fs_err(self.ha_json()),
            "config" => {
                let conf = serde_json::to_value(self.conf.as_ref()).map_err(|e| e.to_string())?;
                Ok(curvine_web::ai::redact_secrets(&conf))
            }
            "sessions" => self
                .map_fs_err(self.fs.session_summary())
                .and_then(|sessions| serde_json::to_value(sessions).map_err(|e| e.to_string())),
            "clusters" => Ok(json!([{
                "id": self.conf.cluster_id,
                "source": "default",
                "role": "this master process serves this single cluster"
            }])),
            "runtime_config" => {
                let view = curvine_web::router::config_view_for_state(&self.config_view)
                    .map_err(|e| e.message().to_string())?;
                Ok(view["runtime"].clone())
            }
            "dir_space" | "observability" => Err(format!(
                "cluster_insight action '{action}' is served by the admin console provider"
            )),
            other => Err(format!("cluster_insight: unknown action '{}'", other)),
        }
    }

    async fn fs_insight(
        &self,
        _cluster: Option<&str>,
        action: &str,
        args: &Value,
    ) -> Result<Value, String> {
        let path = self.arg_str(args, "path", action)?;
        match action {
            "list_dir" => {
                let files = self.map_fs_err(self.fs.list_status(path))?;
                serde_json::to_value(files).map_err(|e| e.to_string())
            }
            "stat" => {
                let status = self.map_fs_err(self.fs.file_status(path))?;
                serde_json::to_value(status).map_err(|e| e.to_string())
            }
            "blocks" => {
                let blocks = self.map_fs_err(self.fs.get_block_locations(path))?;
                serde_json::to_value(blocks).map_err(|e| e.to_string())
            }
            "trash_list" => {
                let entries = self.map_fs_err(self.fs.list_trash())?;
                serde_json::to_value(entries).map_err(|e| e.to_string())
            }
            "content_summary" | "cache_summary" | "locks" => Err(format!(
                "fs_insight action '{action}' is served by the admin console provider"
            )),
            other => Err(format!("fs_insight: unknown action '{}'", other)),
        }
    }

    async fn fs_operate(
        &self,
        _cluster: Option<&str>,
        action: &str,
        args: &Value,
    ) -> Result<Value, String> {
        let path = self.arg_str(args, "path", action)?;
        match action {
            "create_file" => Err(
                "fs_operate action 'create_file' writes file content, which is served by the \
                 admin console provider"
                    .to_string(),
            ),
            "mkdir" => {
                let parent = args
                    .get("recursive")
                    .and_then(Value::as_bool)
                    .unwrap_or(true);
                self.map_fs_err(self.fs.mkdir(path, parent))?;
                Ok(json!({"created": true, "path": path}))
            }
            "delete" => {
                let recursive = args
                    .get("recursive")
                    .and_then(Value::as_bool)
                    .unwrap_or(false);
                let result = self.map_fs_err(self.fs.delete(path, recursive))?;
                Ok(json!({
                    "deleted": true, "path": path, "recursive": recursive,
                    "inodes_removed": result.inodes, "bytes": result.bytes
                }))
            }
            "rename" => {
                let target = self.arg_str(args, "target", action)?;
                self.map_fs_err(self.fs.rename(path, target, RenameFlags::empty()))?;
                Ok(json!({"renamed": true, "from": path, "to": target}))
            }
            "set_attr" => {
                let mut opts = SetAttrOpts {
                    recursive: args
                        .get("recursive")
                        .and_then(Value::as_bool)
                        .unwrap_or(false),
                    ..SetAttrOpts::default()
                };
                if let Some(v) = args.get("owner").and_then(Value::as_str) {
                    opts.owner = Some(v.to_string());
                }
                if let Some(v) = args.get("group").and_then(Value::as_str) {
                    opts.group = Some(v.to_string());
                }
                if let Some(v) = args.get("mode").and_then(Value::as_u64) {
                    opts.mode = Some(v as u32);
                }
                if let Some(v) = args.get("replicas").and_then(Value::as_i64) {
                    opts.replicas = Some(v as i32);
                }
                if let Some(v) = args.get("ttl_ms").and_then(Value::as_i64) {
                    opts.ttl_ms = Some(v);
                }
                if let Some(v) = args.get("ttl_action").and_then(Value::as_str) {
                    opts.ttl_action = Some(TtlAction::try_from(v).map_err(|e| e.to_string())?);
                }
                self.map_fs_err(self.fs.set_attr(path, opts))?;
                Ok(json!({"updated": true, "path": path}))
            }
            "symlink" => {
                let target = self.arg_str(args, "target", action)?;
                self.map_fs_err(self.fs.symlink(target, path, false, 0o644))?;
                Ok(json!({"created": true, "link": path, "target": target}))
            }
            "link" => {
                let target = self.arg_str(args, "target", action)?;
                self.map_fs_err(self.fs.link(path, target))?;
                Ok(json!({"linked": true, "src": path, "dst": target}))
            }
            "free" => {
                let recursive = args
                    .get("recursive")
                    .and_then(Value::as_bool)
                    .unwrap_or(false);
                let result = self.map_fs_err(self.fs.free(path, recursive))?;
                Ok(
                    json!({"freed": true, "path": path, "inodes": result.inodes, "bytes": result.bytes}),
                )
            }
            "trash_restore" => {
                self.map_fs_err(self.fs.restore_trash(path, None, false))?;
                Ok(json!({"restored": true, "path": path}))
            }
            "trash_empty" => {
                let result = self.map_fs_err(self.fs.empty_trash())?;
                Ok(json!({"emptied": true, "inodes_removed": result.inodes, "bytes": result.bytes}))
            }
            other => Err(format!("fs_operate: unknown action '{}'", other)),
        }
    }

    async fn worker_insight(
        &self,
        _cluster: Option<&str>,
        action: &str,
        args: &Value,
    ) -> Result<Value, String> {
        match action {
            "list" => self.map_fs_err(self.list_workers()),
            "detail" => {
                let worker = self.arg_str(args, "worker", action)?;
                let wm = self.fs.worker_manager.read();
                let all = wm
                    .worker_map
                    .workers()
                    .iter()
                    .map(|(_, w)| (w, "Normal"))
                    .chain(wm.worker_map.lost_workers.iter().map(|(_, w)| (w, "Lost")));
                for (w, state) in all {
                    let addr = format!("{}:{}", w.address.hostname, w.address.rpc_port);
                    if addr == worker || w.address.hostname == worker {
                        return Ok(self.worker_json(w, state));
                    }
                }
                Err(format!("worker not found: {}", worker))
            }
            other => Err(format!("worker_insight: unknown action '{}'", other)),
        }
    }

    async fn worker_operate(
        &self,
        _cluster: Option<&str>,
        action: &str,
        args: &Value,
    ) -> Result<Value, String> {
        let trigger = match action {
            "cordon" => Trigger::Cordon,
            "uncordon" => Trigger::Uncordon,
            "decommission" => Trigger::Decommission,
            "force_decommission" => Trigger::ForceDecommission,
            "recommission" => Trigger::CancelDecommission,
            other => return Err(format!("worker_operate: unknown action '{}'", other)),
        };
        let workers: Vec<&str> = args
            .get("workers")
            .and_then(Value::as_array)
            .map(|items| items.iter().filter_map(Value::as_str).collect())
            .unwrap_or_default();
        if workers.is_empty() {
            return Err("worker_operate requires 'workers' (array of addresses)".to_string());
        }
        let mut updated = Vec::new();
        for worker in workers.iter().map(|w| w.trim()).filter(|w| !w.is_empty()) {
            self.map_fs_err(self.fs.apply_operator_action(
                worker,
                trigger,
                "civi",
                "civi ai assistant",
            ))?;
            updated.push(json!({"worker": worker, "action": action}));
        }
        Ok(json!({"action": action, "updated": updated}))
    }

    async fn cluster_operate(
        &self,
        _cluster: Option<&str>,
        action: &str,
        args: &Value,
    ) -> Result<Value, String> {
        match action {
            "runtime_config_update" => {
                let entries = args
                    .get("entries")
                    .and_then(Value::as_array)
                    .ok_or_else(|| "runtime_config_update requires 'entries'".to_string())?;
                let mut applied = Vec::new();
                for entry in entries {
                    let key = entry
                        .get("key")
                        .and_then(Value::as_str)
                        .ok_or_else(|| "each entry requires 'key'".to_string())?;
                    let value = entry.get("value").cloned().unwrap_or(Value::Null);
                    let value_text = match &value {
                        Value::String(text) => text.clone(),
                        Value::Null => String::new(),
                        other => other.to_string(),
                    };
                    let result = curvine_web::router::post_update_config(
                        self.config_view.registry.clone(),
                        axum::extract::Query(vec![(key.to_string(), value_text)]),
                        Value::Null,
                    )
                    .await
                    .map_err(|e| e.message().to_string())?;
                    applied.push(json!({"key": key, "value": value, "result": result.0}));
                }
                Ok(json!({"updated": applied}))
            }
            "failover" | "dir_space_update" => Err(format!(
                "cluster_operate action '{action}' is served by the admin console provider"
            )),
            other => Err(format!("cluster_operate: unknown action '{}'", other)),
        }
    }
}
