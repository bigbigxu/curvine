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

//! Tool registry for the civi agent.
//!
//! Tools are aggregated by domain: five readonly `*_insight` tools and five
//! confirmed-write `*_operate` tools. Each tool dispatches on an `action`
//! string (see the [`AiDataProvider`](crate::ai::provider::AiDataProvider)
//! docs for the action tables); action-specific arguments are validated in
//! the provider implementations, while this module is the single choke point
//! for the whitelist, the action lists and path validation. Write tools
//! (perm = "write") additionally go through the user-confirmation flow
//! before executing (see design doc §4).

use serde_json::{json, Value};

use crate::ai::conf::AiSettings;
use crate::ai::provider::AiDataProvider;

/// Serialized tool results are truncated to this size before being fed back
/// into the model context.
const MAX_RESULT_BYTES: usize = 48 * 1024;

/// Substrings (lower-cased) whose presence in a config key marks the value
/// as secret for redaction.
const SECRET_KEY_PATTERNS: &[&str] = &[
    "password",
    "passwd",
    "secret",
    "token",
    "api_key",
    "apikey",
    "credential",
    "private_key",
    "access_key",
];

#[derive(Debug, Clone)]
pub struct ToolSpec {
    pub name: &'static str,
    pub description: &'static str,
    pub parameters: Value,
    /// Permission tier, surfaced in the settings UI.
    pub perm: &'static str,
    /// Actions the tool dispatches on; `None` lists them in the schema.
    pub actions: &'static [&'static str],
}

/// Read-only tool groups, backed by `AiDataProvider`.
pub fn builtin_tools() -> Vec<ToolSpec> {
    vec![
        ToolSpec {
            name: "cluster_insight",
            description: "Inspect the Curvine cluster: capacity/usage overview, master HA \
                          state, redacted configuration, client sessions, multi-cluster \
                          registry, effective runtime config, dir-space (quota) status, \
                          and observability metric queries.",
            parameters: cluster_tool_schema(),
            perm: "readonly",
            actions: CLUSTER_INSIGHT_ACTIONS,
        },
        ToolSpec {
            name: "fs_insight",
            description: "Inspect the Curvine filesystem: directory listings, path status, \
                          block locations, content summaries, UFS/CV cache reconciliation, \
                          path locks and trash contents.",
            parameters: fs_tool_schema(),
            perm: "readonly",
            actions: FS_INSIGHT_ACTIONS,
        },
        ToolSpec {
            name: "mount_insight",
            description: "Inspect Curvine mounts (UFS integration): mount table, single \
                          mount info, UFS reachability validation (dry-run) and resync \
                          task status.",
            parameters: mount_tool_schema(),
            perm: "readonly",
            actions: MOUNT_INSIGHT_ACTIONS,
        },
        ToolSpec {
            name: "job_insight",
            description: "Inspect data jobs: load/export jobs (list, status) and transfer \
                          jobs (list, status pages, service instances, service config).",
            parameters: job_tool_schema(),
            perm: "readonly",
            actions: JOB_INSIGHT_ACTIONS,
        },
        ToolSpec {
            name: "worker_insight",
            description: "Inspect storage workers: live/lost worker list with capacity and \
                          usage, or the detail of one worker (blocks, water levels).",
            parameters: worker_tool_schema(),
            perm: "readonly",
            actions: WORKER_INSIGHT_ACTIONS,
        },
        ToolSpec {
            name: "fs_operate",
            description: "Mutate the Curvine filesystem: create text files (max 64 KB per \
                          call), directories, links, attr updates, deletions and trash \
                          operations. Every action is destructive or state-changing and \
                          requires explicit user confirmation in the console before \
                          execution.",
            parameters: fs_operate_schema(),
            perm: "write",
            actions: FS_OPERATE_ACTIONS,
        },
        ToolSpec {
            name: "mount_operate",
            description: "Mutate the Curvine mount table: add/update/delete mounts (UFS \
                          integration points) and start resync. Requires explicit user \
                          confirmation in the console before execution.",
            parameters: mount_operate_schema(),
            perm: "write",
            actions: MOUNT_OPERATE_ACTIONS,
        },
        ToolSpec {
            name: "job_operate",
            description: "Operate on data jobs: submit a load job (UFS->CV warm-up), submit \
                          a transfer job, cancel or retry jobs. Requires explicit user \
                          confirmation in the console before execution.",
            parameters: job_operate_schema(),
            perm: "write",
            actions: JOB_OPERATE_ACTIONS,
        },
        ToolSpec {
            name: "worker_operate",
            description: "Change storage worker states through the journaled state machine: \
                          cordon, uncordon, decommission, force_decommission, recommission. \
                          Requires explicit user confirmation in the console before \
                          execution.",
            parameters: worker_operate_schema(),
            perm: "write",
            actions: WORKER_OPERATE_ACTIONS,
        },
        ToolSpec {
            name: "cluster_operate",
            description: "Cluster-level operations: trigger master failover, update runtime \
                          (hot) configuration, update dir-space governance. These are the \
                          most dangerous actions and require explicit user confirmation in \
                          the console before execution.",
            parameters: cluster_operate_schema(),
            perm: "write",
            actions: CLUSTER_OPERATE_ACTIONS,
        },
    ]
}

pub fn find_spec(name: &str) -> Option<ToolSpec> {
    builtin_tools().into_iter().find(|t| t.name == name)
}

/// Action lists mirrored into the tool schemas (`action` enum).
macro_rules! actions {
    ($($a:literal),+ $(,)?) => {
        &[$($a),+]
    };
}

pub const CLUSTER_INSIGHT_ACTIONS: &[&str] = actions![
    "overview",
    "ha",
    "config",
    "sessions",
    "clusters",
    "runtime_config",
    "dir_space",
    "observability"
];
pub const FS_INSIGHT_ACTIONS: &[&str] = actions![
    "list_dir",
    "stat",
    "blocks",
    "content_summary",
    "cache_summary",
    "locks",
    "trash_list"
];
pub const MOUNT_INSIGHT_ACTIONS: &[&str] = actions!["list", "info", "validate", "resync_status"];
pub const JOB_INSIGHT_ACTIONS: &[&str] = actions![
    "list",
    "status",
    "transfers",
    "transfer_status",
    "transfer_instances",
    "transfer_config"
];
pub const WORKER_INSIGHT_ACTIONS: &[&str] = actions!["list", "detail"];
pub const FS_OPERATE_ACTIONS: &[&str] = actions![
    "create_file",
    "mkdir",
    "rename",
    "delete",
    "set_attr",
    "symlink",
    "link",
    "free",
    "trash_restore",
    "trash_empty"
];

/// Largest file content one tool call may write (the chat context itself is
/// the practical bound; this just keeps runaway payloads out of the RPC).
pub const MAX_CREATE_FILE_BYTES: usize = 64 * 1024;
pub const MOUNT_OPERATE_ACTIONS: &[&str] = actions!["add", "update", "delete", "resync"];
pub const JOB_OPERATE_ACTIONS: &[&str] =
    actions!["submit_load", "submit_transfer", "cancel", "retry"];
pub const WORKER_OPERATE_ACTIONS: &[&str] = actions![
    "cordon",
    "uncordon",
    "decommission",
    "force_decommission",
    "recommission"
];
pub const CLUSTER_OPERATE_ACTIONS: &[&str] =
    actions!["failover", "runtime_config_update", "dir_space_update"];

fn actions_enum(actions: &[&str]) -> Value {
    json!(actions)
}

fn cluster_tool_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "action": {
                "type": "string",
                "enum": actions_enum(CLUSTER_INSIGHT_ACTIONS),
                "description": "Which cluster inspection to run."
            },
            "cluster": {"type": "string", "description": "Optional cluster id (multi-cluster console)."},
            "path": {"type": "string", "description": "dir_space: tracked directory path."},
            "metric": {"type": "string", "description": "observability: metric name, e.g. 'jvm_memory_used_bytes'."},
            "component": {"type": "string", "description": "observability: optional component filter (master/worker/client)."},
            "range": {"type": "string", "description": "observability: optional time range like '1h' (with step)."},
            "step": {"type": "string", "description": "observability: optional step like '60s'."}
        },
        "required": ["action"]
    })
}

fn fs_tool_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "action": {
                "type": "string",
                "enum": actions_enum(FS_INSIGHT_ACTIONS),
                "description": "Which filesystem inspection to run."
            },
            "cluster": {"type": "string", "description": "Optional cluster id (multi-cluster console)."},
            "path": {"type": "string", "description": "Absolute path to inspect. Required by list_dir/stat/blocks/content_summary/locks."},
            "deep": {"type": "boolean", "description": "cache_summary: enable deep (slower) reconciliation."},
            "page": {"type": "integer", "description": "trash_list: page number (default 1)."},
            "page_size": {"type": "integer", "description": "trash_list: entries per page (default 100)."}
        },
        "required": ["action"]
    })
}

fn mount_tool_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "action": {
                "type": "string",
                "enum": actions_enum(MOUNT_INSIGHT_ACTIONS),
                "description": "Which mount inspection to run."
            },
            "cluster": {"type": "string", "description": "Optional cluster id (multi-cluster console)."},
            "path": {"type": "string", "description": "info: the curvine mount path (required)."},
            "ufs_path": {"type": "string", "description": "validate: the UFS path to probe (required)."},
            "task_id": {"type": "string", "description": "resync_status: resync task id (required)."}
        },
        "required": ["action"]
    })
}

fn job_tool_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "action": {
                "type": "string",
                "enum": actions_enum(JOB_INSIGHT_ACTIONS),
                "description": "Which job inspection to run."
            },
            "cluster": {"type": "string", "description": "Optional cluster id (multi-cluster console)."},
            "job_id": {"type": "string", "description": "status/transfer_status: job id (required)."},
            "limit": {"type": "integer", "description": "list/transfers: max entries (default 20)."}
        },
        "required": ["action"]
    })
}

fn worker_tool_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "action": {
                "type": "string",
                "enum": actions_enum(WORKER_INSIGHT_ACTIONS),
                "description": "Which worker inspection to run."
            },
            "cluster": {"type": "string", "description": "Optional cluster id (multi-cluster console)."},
            "worker": {"type": "string", "description": "detail: worker address 'host:port' (required)."}
        },
        "required": ["action"]
    })
}

fn fs_operate_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "action": {
                "type": "string",
                "enum": actions_enum(FS_OPERATE_ACTIONS),
                "description": "Which filesystem mutation to run."
            },
            "cluster": {"type": "string", "description": "Optional cluster id (multi-cluster console)."},
            "path": {"type": "string", "description": "Target path (required by most actions)."},
            "target": {"type": "string", "description": "rename/symlink/link: destination path."},
            "recursive": {"type": "boolean", "description": "delete/free: recurse into subdirectories."},
            "owner": {"type": "string", "description": "set_attr: new owner."},
            "group": {"type": "string", "description": "set_attr: new group."},
            "mode": {"type": "integer", "description": "set_attr: octal permission bits, e.g. 493 for 0755."},
            "replicas": {"type": "integer", "description": "set_attr: replica count."},
            "ttl_ms": {"type": "integer", "description": "set_attr: time-to-live in milliseconds (0 clears TTL)."},
            "ttl_action": {"type": "string", "enum": ["delete", "trash"], "description": "set_attr: what happens on TTL expiry."},
            "content": {"type": "string", "description": "create_file: text content to write (max 64 KB)."},
            "overwrite": {"type": "boolean", "description": "create_file: overwrite an existing file (default false)."},
            "xattr": {"type": "object", "description": "set_attr: additional xattrs as {key: string}."},
            "trash_paths": {"type": "array", "items": {"type": "string"}, "description": "trash_restore: trash entry paths to restore."}
        },
        "required": ["action"]
    })
}

fn mount_operate_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "action": {
                "type": "string",
                "enum": actions_enum(MOUNT_OPERATE_ACTIONS),
                "description": "Which mount mutation to run."
            },
            "cluster": {"type": "string", "description": "Optional cluster id (multi-cluster console)."},
            "path": {"type": "string", "description": "Curvine mount path (required)."},
            "ufs_path": {"type": "string", "description": "add/update: UFS source path."},
            "write_type": {"type": "string", "enum": ["cache", "fs"], "description": "add/update: cache or fs mode."},
            "read_only": {"type": "boolean", "description": "add/update: read-only mount."},
            "properties": {"type": "object", "description": "add/update: UFS connection properties."},
            "consistency_check": {"type": "boolean", "description": "add/update: consistency check strategy."},
            "resync_path": {"type": "string", "description": "resync: fs_mode curvine path to resync (alias of path)."}
        },
        "required": ["action"]
    })
}

fn job_operate_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "action": {
                "type": "string",
                "enum": actions_enum(JOB_OPERATE_ACTIONS),
                "description": "Which job mutation to run."
            },
            "cluster": {"type": "string", "description": "Optional cluster id (multi-cluster console)."},
            "path": {"type": "string", "description": "submit_load: UFS source path."},
            "target_path": {"type": "string", "description": "submit_load: optional curvine target path."},
            "job_id": {"type": "string", "description": "cancel/retry: job id (required)."},
            "kind": {"type": "string", "enum": ["load", "transfer"], "description": "cancel/retry: which job family the id belongs to."},
            "src_path": {"type": "string", "description": "submit_transfer: source path."},
            "dst_path": {"type": "string", "description": "submit_transfer: destination path."}
        },
        "required": ["action"]
    })
}

fn worker_operate_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "action": {
                "type": "string",
                "enum": actions_enum(WORKER_OPERATE_ACTIONS),
                "description": "Which worker state change to apply."
            },
            "cluster": {"type": "string", "description": "Optional cluster id (multi-cluster console)."},
            "workers": {
                "type": "array",
                "items": {"type": "string"},
                "description": "Worker addresses to operate on, e.g. ['host:9002']."
            }
        },
        "required": ["action", "workers"]
    })
}

fn cluster_operate_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "action": {
                "type": "string",
                "enum": actions_enum(CLUSTER_OPERATE_ACTIONS),
                "description": "Which cluster-level operation to run."
            },
            "cluster": {"type": "string", "description": "Optional cluster id (multi-cluster console)."},
            "target_master": {"type": "string", "description": "failover: address of the master to promote."},
            "entries": {
                "type": "array",
                "items": {"type": "object"},
                "description": "runtime_config_update: [{key, value}] overrides; value null clears."
            },
            "enabled": {"type": "boolean", "description": "dir_space_update: enable/disable tracking."},
            "tracked_paths": {"type": "array", "items": {"type": "string"}, "description": "dir_space_update: directories to track."}
        },
        "required": ["action"]
    })
}

/// Tool specs in OpenAI `tools` format, filtered by the settings whitelist.
pub fn openai_specs(settings: &AiSettings) -> Vec<Value> {
    builtin_tools()
        .into_iter()
        .filter(|t| settings.enabled_tools.iter().any(|n| n == t.name))
        .map(|t| {
            json!({
                "type": "function",
                "function": {
                    "name": t.name,
                    "description": t.description,
                    "parameters": t.parameters
                }
            })
        })
        .collect()
}

fn require_str<'a>(args: &'a Value, key: &str, action: &str) -> Result<&'a str, String> {
    args.get(key)
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| format!("action '{action}' requires a non-empty '{key}'"))
}

/// Single entry point for executing a tool call issued by the model.
/// Never trust the model: the whitelist is enforced again here even though
/// disabled tools were never announced to it, and the `action` field is
/// checked against the per-tool action table.
pub async fn dispatch(
    provider: &dyn AiDataProvider,
    settings: &AiSettings,
    name: &str,
    args: &Value,
    cluster: Option<&str>,
) -> Result<Value, String> {
    if !settings.enabled_tools.iter().any(|n| n == name) {
        return Err(format!(
            "tool '{}' is not enabled for the AI assistant",
            name
        ));
    }
    let spec = find_spec(name).ok_or_else(|| format!("unknown tool '{}'", name))?;
    let action = require_str(args, "action", name)?;
    if !spec.actions.contains(&action) {
        return Err(format!(
            "tool '{}' does not support action '{}'",
            name, action
        ));
    }

    // The console cluster selector overrides the payload-level default: an
    // explicit `cluster` argument wins over the conversation default.
    let cluster = args
        .get("cluster")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .or(cluster);

    // Path-carrying arguments are user-controllable data: validate before
    // they reach the provider.
    if let Some(path) = args.get("path").and_then(Value::as_str) {
        validate_path(path)?;
    }
    if let Some(target) = args.get("target").and_then(Value::as_str) {
        validate_path(target)?;
    }

    let value = match name {
        "cluster_insight" => provider.cluster_insight(cluster, action, args).await?,
        "fs_insight" => {
            if matches!(
                action,
                "list_dir" | "stat" | "blocks" | "content_summary" | "locks"
            ) {
                require_str(args, "path", action)?;
            }
            provider.fs_insight(cluster, action, args).await?
        }
        "mount_insight" => {
            match action {
                "info" | "validate" | "resync_status" => {
                    let _ = require_str(
                        args,
                        if action == "validate" {
                            "ufs_path"
                        } else {
                            "path"
                        },
                        action,
                    )?;
                }
                _ => {}
            }
            provider.mount_insight(cluster, action, args).await?
        }
        "job_insight" => {
            if matches!(action, "status" | "transfer_status") {
                require_str(args, "job_id", action)?;
            }
            provider.job_insight(cluster, action, args).await?
        }
        "worker_insight" => {
            if action == "detail" {
                require_str(args, "worker", action)?;
            }
            provider.worker_insight(cluster, action, args).await?
        }
        "fs_operate" => {
            match action {
                "rename" | "symlink" | "link" => {
                    require_str(args, "target", action)?;
                }
                "create_file" => {
                    require_str(args, "content", action)?;
                    let content = args.get("content").and_then(Value::as_str).unwrap_or("");
                    if content.len() > MAX_CREATE_FILE_BYTES {
                        return Err(format!(
                            "content exceeds the {} KB limit for create_file",
                            MAX_CREATE_FILE_BYTES / 1024
                        ));
                    }
                }
                _ => {
                    require_str(args, "path", action)?;
                }
            }
            provider.fs_operate(cluster, action, args).await?
        }
        "mount_operate" => {
            match action {
                "add" | "update" => {
                    require_str(args, "ufs_path", action)?;
                }
                "resync" => {
                    let _ = require_str(
                        args,
                        if args.get("resync_path").is_some() {
                            "resync_path"
                        } else {
                            "path"
                        },
                        action,
                    )?;
                }
                _ => {
                    require_str(args, "path", action)?;
                }
            }
            provider.mount_operate(cluster, action, args).await?
        }
        "job_operate" => {
            match action {
                "submit_load" => {
                    require_str(args, "path", action)?;
                }
                "submit_transfer" => {
                    require_str(args, "src_path", action)?;
                    require_str(args, "dst_path", action)?;
                }
                "cancel" | "retry" => {
                    require_str(args, "job_id", action)?;
                }
                _ => {}
            }
            provider.job_operate(cluster, action, args).await?
        }
        "worker_operate" => {
            let workers = parse_worker_list(args)?;
            if workers.is_empty() {
                return Err("'workers' must contain at least one address".to_string());
            }
            provider.worker_operate(cluster, action, args).await?
        }
        "cluster_operate" => {
            match action {
                "failover" => {
                    require_str(args, "target_master", action)?;
                }
                "runtime_config_update" => {
                    if args.get("entries").and_then(Value::as_array).is_none() {
                        return Err(
                            "action 'runtime_config_update' requires 'entries' (array)".to_string()
                        );
                    }
                }
                "dir_space_update"
                    if args.get("enabled").is_none() && args.get("tracked_paths").is_none() =>
                {
                    return Err(
                        "action 'dir_space_update' requires 'enabled' and/or 'tracked_paths'"
                            .to_string(),
                    );
                }
                _ => {}
            }
            provider.cluster_operate(cluster, action, args).await?
        }
        other => return Err(format!("unknown tool '{}'", other)),
    };

    Ok(truncate(value))
}

fn parse_worker_list(args: &Value) -> Result<Vec<String>, String> {
    let list = args
        .get("workers")
        .and_then(Value::as_array)
        .ok_or_else(|| "missing required parameter: workers (array of strings)".to_string())?;
    let mut out = Vec::new();
    for item in list {
        let s = item
            .as_str()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .ok_or_else(|| "'workers' items must be non-empty strings".to_string())?;
        out.push(s.to_string());
    }
    Ok(out)
}

/// Paths coming from the model are user-controllable data: enforce absolute
/// paths and reject traversal segments.
fn validate_path(path: &str) -> Result<String, String> {
    let path = path.trim();
    if !path.starts_with('/') {
        return Err(format!("path '{}' must be absolute (start with '/')", path));
    }
    if path.contains('\0') || path.contains('\\') {
        return Err(format!("path '{}' contains invalid characters", path));
    }
    if path.split('/').any(|seg| seg == "..") {
        return Err(format!("path '{}' must not contain '..' segments", path));
    }
    Ok(path.to_string())
}

fn truncate(value: Value) -> Value {
    let text = value.to_string();
    if text.len() <= MAX_RESULT_BYTES {
        return value;
    }
    let mut cut = MAX_RESULT_BYTES;
    while !text.is_char_boundary(cut) {
        cut -= 1;
    }
    json!({
        "truncated": true,
        "note": format!(
            "Result exceeded {} bytes and was truncated. Narrow your query (e.g. list a \
             sub-directory) instead of expecting the full payload.",
            MAX_RESULT_BYTES
        ),
        "data_prefix": &text[..cut]
    })
}

/// Recursively replaces secret-looking string values with `***`.
pub fn redact_secrets(value: &Value) -> Value {
    match value {
        Value::Object(map) => Value::Object(
            map.iter()
                .map(|(k, v)| {
                    let key = k.to_lowercase();
                    if SECRET_KEY_PATTERNS.iter().any(|p| key.contains(p)) && !v.is_null() {
                        (k.clone(), Value::String("***".to_string()))
                    } else {
                        (k.clone(), redact_secrets(v))
                    }
                })
                .collect(),
        ),
        Value::Array(items) => Value::Array(items.iter().map(redact_secrets).collect()),
        other => other.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::sync::Mutex;

    struct FakeProvider {
        calls: Mutex<Vec<String>>,
    }

    impl FakeProvider {
        fn new() -> Self {
            FakeProvider {
                calls: Mutex::new(vec![]),
            }
        }
    }

    #[async_trait::async_trait]
    impl AiDataProvider for FakeProvider {
        async fn cluster_insight(
            &self,
            _cluster: Option<&str>,
            action: &str,
            _args: &Value,
        ) -> Result<Value, String> {
            self.calls
                .lock()
                .unwrap()
                .push(format!("cluster:{}", action));
            Ok(json!({"action": action, "capacity": 100}))
        }
        async fn fs_insight(
            &self,
            _cluster: Option<&str>,
            action: &str,
            _args: &Value,
        ) -> Result<Value, String> {
            self.calls.lock().unwrap().push(format!("fs:{}", action));
            Ok(json!({"action": action, "capacity": 100}))
        }
        async fn mount_insight(
            &self,
            _cluster: Option<&str>,
            action: &str,
            _args: &Value,
        ) -> Result<Value, String> {
            self.calls.lock().unwrap().push(format!("mount:{}", action));
            Ok(json!({"action": action, "capacity": 100}))
        }
        async fn job_insight(
            &self,
            _cluster: Option<&str>,
            action: &str,
            _args: &Value,
        ) -> Result<Value, String> {
            self.calls.lock().unwrap().push(format!("job:{}", action));
            Ok(json!({"action": action, "capacity": 100}))
        }
        async fn worker_insight(
            &self,
            _cluster: Option<&str>,
            action: &str,
            _args: &Value,
        ) -> Result<Value, String> {
            self.calls
                .lock()
                .unwrap()
                .push(format!("worker:{}", action));
            Ok(json!({"action": action, "capacity": 100}))
        }
        async fn fs_operate(
            &self,
            _cluster: Option<&str>,
            action: &str,
            _args: &Value,
        ) -> Result<Value, String> {
            self.calls.lock().unwrap().push(format!("fs_op:{}", action));
            Ok(json!({"action": action, "capacity": 100}))
        }
        async fn mount_operate(
            &self,
            _cluster: Option<&str>,
            action: &str,
            _args: &Value,
        ) -> Result<Value, String> {
            self.calls
                .lock()
                .unwrap()
                .push(format!("mount_op:{}", action));
            Ok(json!({"action": action, "capacity": 100}))
        }
        async fn job_operate(
            &self,
            _cluster: Option<&str>,
            action: &str,
            _args: &Value,
        ) -> Result<Value, String> {
            self.calls
                .lock()
                .unwrap()
                .push(format!("job_op:{}", action));
            Ok(json!({"action": action, "capacity": 100}))
        }
        async fn worker_operate(
            &self,
            _cluster: Option<&str>,
            action: &str,
            _args: &Value,
        ) -> Result<Value, String> {
            self.calls
                .lock()
                .unwrap()
                .push(format!("worker_op:{}", action));
            Ok(json!({"action": action, "capacity": 100}))
        }
        async fn cluster_operate(
            &self,
            _cluster: Option<&str>,
            action: &str,
            _args: &Value,
        ) -> Result<Value, String> {
            self.calls
                .lock()
                .unwrap()
                .push(format!("cluster_op:{}", action));
            Ok(json!({"action": action, "capacity": 100}))
        }
    }

    fn all_enabled_settings() -> AiSettings {
        AiSettings {
            enabled_tools: builtin_tools().iter().map(|t| t.name.to_string()).collect(),
            ..AiSettings::default()
        }
    }

    fn default_settings() -> AiSettings {
        AiSettings::default()
    }

    #[tokio::test]
    async fn test_dispatch_routes_and_validates() {
        let provider = FakeProvider::new();
        let settings = all_enabled_settings();

        let v = dispatch(
            &provider,
            &settings,
            "cluster_insight",
            &json!({"action": "overview"}),
            None,
        )
        .await
        .unwrap();
        assert_eq!(v["capacity"], 100);

        let v = dispatch(
            &provider,
            &settings,
            "fs_insight",
            &json!({"action": "list_dir", "path": "/data/logs"}),
            None,
        )
        .await
        .unwrap();
        assert_eq!(v["action"], "list_dir");

        // Path validation.
        assert!(dispatch(
            &provider,
            &settings,
            "fs_insight",
            &json!({"action": "list_dir", "path": "relative"}),
            None
        )
        .await
        .is_err());
        assert!(dispatch(
            &provider,
            &settings,
            "fs_insight",
            &json!({"action": "list_dir", "path": "/a/../b"}),
            None
        )
        .await
        .is_err());
        assert!(dispatch(
            &provider,
            &settings,
            "fs_insight",
            &json!({"action": "list_dir"}),
            None
        )
        .await
        .is_err());

        // Unknown tool and unknown action.
        assert!(dispatch(&provider, &settings, "rm_rf", &json!({}), None)
            .await
            .is_err());
        assert!(dispatch(
            &provider,
            &settings,
            "cluster_insight",
            &json!({"action": "reboot"}),
            None
        )
        .await
        .is_err());
    }

    #[tokio::test]
    async fn test_whitelist_enforced_at_dispatch() {
        let provider = FakeProvider::new();
        let mut settings = all_enabled_settings();
        settings.enabled_tools = vec!["cluster_insight".to_string()];

        assert!(dispatch(
            &provider,
            &settings,
            "worker_insight",
            &json!({"action": "list"}),
            None
        )
        .await
        .is_err());
        assert!(dispatch(
            &provider,
            &settings,
            "cluster_insight",
            &json!({"action": "overview"}),
            None
        )
        .await
        .is_ok());
    }

    #[tokio::test]
    async fn test_write_tools_off_by_default() {
        let provider = FakeProvider::new();
        let settings = default_settings();
        // Default whitelist only contains readonly tools.
        assert!(dispatch(
            &provider,
            &settings,
            "fs_operate",
            &json!({"action": "delete", "path": "/a"}),
            None
        )
        .await
        .is_err());
        assert!(dispatch(
            &provider,
            &settings,
            "cluster_insight",
            &json!({"action": "config"}),
            None
        )
        .await
        .is_ok());

        // Once enabled, arg validation applies.
        let mut enabled = settings.clone();
        enabled.enabled_tools.push("worker_operate".to_string());
        assert!(dispatch(
            &provider,
            &enabled,
            "worker_operate",
            &json!({"action": "decommission", "workers": []}),
            None
        )
        .await
        .is_err());
        let v = dispatch(
            &provider,
            &enabled,
            "worker_operate",
            &json!({"action": "decommission", "workers": ["host:9002"]}),
            None,
        )
        .await
        .unwrap();
        assert_eq!(v["action"], "decommission");
    }

    #[tokio::test]
    async fn test_create_file_content_validation() {
        let provider = FakeProvider::new();
        let mut settings = all_enabled_settings();
        settings.enabled_tools.push("fs_operate".to_string());

        let ok = json!({"action": "create_file", "path": "/a.txt", "content": "hello"});
        assert!(dispatch(&provider, &settings, "fs_operate", &ok, None)
            .await
            .is_ok());

        let too_big = json!({
            "action": "create_file",
            "path": "/a.txt",
            "content": "x".repeat(MAX_CREATE_FILE_BYTES + 1)
        });
        assert!(dispatch(&provider, &settings, "fs_operate", &too_big, None)
            .await
            .is_err());

        // Missing content.
        assert!(dispatch(
            &provider,
            &settings,
            "fs_operate",
            &json!({"action": "create_file", "path": "/a.txt"}),
            None
        )
        .await
        .is_err());
    }

    #[tokio::test]
    async fn test_truncation() {
        let big: Vec<_> = (0..100_000)
            .map(|i| json!({"i": i, "pad": "x".repeat(8)}))
            .collect();
        let out = truncate(json!(big));
        assert_eq!(out["truncated"], true);
        assert!(out["data_prefix"].as_str().unwrap().len() <= MAX_RESULT_BYTES);

        let small = truncate(json!({"ok": true}));
        assert_eq!(small["ok"], true);
    }

    #[tokio::test]
    async fn test_cluster_argument_overrides_conversation_default() {
        let provider = FakeProvider::new();
        let settings = all_enabled_settings();
        let args = json!({"action": "overview", "cluster": "beta"});
        // The FakeProvider ignores the selector; assert only that the call
        // passes validation with both sources present.
        assert!(dispatch(
            &provider,
            &settings,
            "cluster_insight",
            &args,
            Some("alpha")
        )
        .await
        .is_ok());
    }

    #[test]
    fn test_redact_secrets() {
        let conf = json!({
            "cluster_id": "test",
            "master": {"username": "admin", "password": "p@ss", "port": 8995},
            "journal": {"token": "abc"},
            "s3": {"access_key": "ak", "secret_key": "sk", "region": "us"},
            "workers": [{"passwd": "x", "hostname": "h1"}]
        });
        let redacted = redact_secrets(&conf);
        assert_eq!(redacted["cluster_id"], "test");
        assert_eq!(redacted["master"]["password"], "***");
        assert_eq!(redacted["master"]["username"], "admin");
        assert_eq!(redacted["master"]["port"], 8995);
        assert_eq!(redacted["journal"]["token"], "***");
        assert_eq!(redacted["s3"]["access_key"], "***");
        assert_eq!(redacted["s3"]["secret_key"], "***");
        assert_eq!(redacted["s3"]["region"], "us");
        assert_eq!(redacted["workers"][0]["passwd"], "***");
        assert_eq!(redacted["workers"][0]["hostname"], "h1");
    }

    #[test]
    fn test_settings_mask_roundtrip() {
        let settings = AiSettings {
            api_key: "sk-secret".to_string(),
            ..AiSettings::default()
        };
        let masked = settings.masked();
        assert_eq!(masked.api_key, "***");

        // The store keeps the real key when the mask is posted back.
        let store = crate::ai::conf::AiSettingsStore::from_path(
            std::env::temp_dir().join(format!("civi-test-{}.json", std::process::id())),
        );
        store.update(settings);
        assert_eq!(store.api_key(), "sk-secret");
        let mut posted = store.get().masked();
        posted.enabled = true;
        let saved = store.update(posted);
        assert_eq!(saved.api_key, "***");
    }
}
