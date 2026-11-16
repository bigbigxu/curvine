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

//! Data source abstraction for civi tools.
//!
//! `curvine-web` must not depend on `curvine-master`, so the agent talks to
//! the cluster through this trait. Servers implement it on top of the same
//! internal state that backs their existing web API handlers:
//!
//! * the master serves one cluster in-process, so its implementation ignores
//!   the cluster selector;
//! * the admin console fronts several clusters, so its implementation resolves
//!   the selector against the cluster registry (and falls back to the default
//!   cluster when the caller does not name one).
//!
//! The methods are async because the console reaches imported clusters over
//! RPC; the master's implementation simply does its (synchronous) state access
//! inside the async body.
//!
//! Tools are aggregated by domain, not one-per-operation: each method serves
//! one tool and dispatches on an `action` string carried in `args`. Every
//! action's arguments arrive as a free-form JSON object and are validated by
//! the provider implementation (see [`crate::ai::tools`] for the whitelist
//! and argument shape each action accepts).
//!
//! # Tool groups
//!
//! | Method           | Tool            | Perm      | Actions |
//! |------------------|-----------------|-----------|---------|
//! | [`cluster_insight`](Self::cluster_insight)   | `cluster_insight` | readonly | overview, ha, config, sessions, clusters, runtime_config, dir_space, observability |
//! | [`fs_insight`](Self::fs_insight)             | `fs_insight`      | readonly | list_dir, stat, blocks, content_summary, cache_summary, locks, trash_list |
//! | [`mount_insight`](Self::mount_insight)       | `mount_insight`   | readonly | list, info, validate, resync_status |
//! | [`job_insight`](Self::job_insight)           | `job_insight`     | readonly | list, status, transfers, transfer_status, transfer_instances, transfer_config |
//! | [`worker_insight`](Self::worker_insight)     | `worker_insight`  | readonly | list, detail |
//! | [`fs_operate`](Self::fs_operate)             | `fs_operate`      | write    | mkdir, rename, delete, set_attr, symlink, link, free, trash_restore, trash_empty |
//! | [`mount_operate`](Self::mount_operate)       | `mount_operate`   | write    | add, update, delete, resync |
//! | [`job_operate`](Self::job_operate)           | `job_operate`     | write    | submit_load, submit_transfer, cancel, retry |
//! | [`worker_operate`](Self::worker_operate)     | `worker_operate`  | write    | cordon, uncordon, decommission, force_decommission, recommission |
//! | [`cluster_operate`](Self::cluster_operate)   | `cluster_operate` | write    | failover, runtime_config_update, dir_space_update |

use async_trait::async_trait;
use serde_json::Value;

#[async_trait]
pub trait AiDataProvider: Send + Sync + 'static {
    /// Cluster-wide inspection: overview, master HA, redacted config, client
    /// sessions, multi-cluster registry, runtime config view, dir-space
    /// status, and observability metric queries.
    async fn cluster_insight(
        &self,
        _cluster: Option<&str>,
        _action: &str,
        _args: &Value,
    ) -> Result<Value, String> {
        Err("not supported by this provider".to_string())
    }

    /// Filesystem inspection: listings, path status, block locations, usage
    /// summaries, cache reconciliation, locks and trash contents.
    async fn fs_insight(
        &self,
        _cluster: Option<&str>,
        _action: &str,
        _args: &Value,
    ) -> Result<Value, String> {
        Err("not supported by this provider".to_string())
    }

    /// Mount table inspection: listing, single-mount info, UFS reachability
    /// validation (dry-run) and resync task status.
    async fn mount_insight(
        &self,
        _cluster: Option<&str>,
        _action: &str,
        _args: &Value,
    ) -> Result<Value, String> {
        Err("not supported by this provider".to_string())
    }

    /// Data-job inspection: load/export jobs and transfer jobs, their status
    /// pages and the transfer service configuration.
    async fn job_insight(
        &self,
        _cluster: Option<&str>,
        _action: &str,
        _args: &Value,
    ) -> Result<Value, String> {
        Err("not supported by this provider".to_string())
    }

    /// Storage node inspection: live/lost worker list and per-worker detail.
    async fn worker_insight(
        &self,
        _cluster: Option<&str>,
        _action: &str,
        _args: &Value,
    ) -> Result<Value, String> {
        Err("not supported by this provider".to_string())
    }

    /// Filesystem mutation (confirmed write): mkdir, rename, delete, attr
    /// updates, links, space release, trash restore/empty.
    async fn fs_operate(
        &self,
        _cluster: Option<&str>,
        _action: &str,
        _args: &Value,
    ) -> Result<Value, String> {
        Err("not supported by this provider".to_string())
    }

    /// Mount table mutation (confirmed write): add, update, delete, resync.
    async fn mount_operate(
        &self,
        _cluster: Option<&str>,
        _action: &str,
        _args: &Value,
    ) -> Result<Value, String> {
        Err("not supported by this provider".to_string())
    }

    /// Data-job mutation (confirmed write): submit load / transfer, cancel,
    /// retry.
    async fn job_operate(
        &self,
        _cluster: Option<&str>,
        _action: &str,
        _args: &Value,
    ) -> Result<Value, String> {
        Err("not supported by this provider".to_string())
    }

    /// Worker state machine actions (confirmed write): cordon, uncordon,
    /// decommission, force_decommission, recommission.
    async fn worker_operate(
        &self,
        _cluster: Option<&str>,
        _action: &str,
        _args: &Value,
    ) -> Result<Value, String> {
        Err("not supported by this provider".to_string())
    }

    /// Cluster-level mutation (confirmed write): master failover, runtime
    /// config update, dir-space governance update.
    async fn cluster_operate(
        &self,
        _cluster: Option<&str>,
        _action: &str,
        _args: &Value,
    ) -> Result<Value, String> {
        Err("not supported by this provider".to_string())
    }
}
