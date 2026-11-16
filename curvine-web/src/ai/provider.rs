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

//! Data source abstraction for CIVI tools.
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

use async_trait::async_trait;
use serde_json::Value;

#[async_trait]
pub trait AiDataProvider: Send + Sync + 'static {
    /// Cluster capacity/usage/file counts/worker summary.
    async fn cluster_overview(&self, _cluster: Option<&str>) -> Result<Value, String> {
        Err("not supported by this provider".to_string())
    }

    /// List a directory. `path` must be an absolute path.
    async fn list_dir(&self, _cluster: Option<&str>, _path: &str) -> Result<Value, String> {
        Err("not supported by this provider".to_string())
    }

    /// Live/lost workers.
    async fn worker_status(&self, _cluster: Option<&str>) -> Result<Value, String> {
        Err("not supported by this provider".to_string())
    }

    /// Client session leases (zombie mount diagnostics). Only the master
    /// tracks client sessions, so console deployments report "not supported".
    async fn session_summary(&self, _cluster: Option<&str>) -> Result<Value, String> {
        Err("not supported by this provider".to_string())
    }

    /// Cluster configuration with secret fields redacted.
    async fn cluster_config(&self, _cluster: Option<&str>) -> Result<Value, String> {
        Err("not supported by this provider".to_string())
    }

    /// Mark workers as decommissioned (write, requires confirmation).
    async fn add_dcm(&self, _cluster: Option<&str>, _workers: &[String]) -> Result<Value, String> {
        Err("not supported by this provider".to_string())
    }
}
