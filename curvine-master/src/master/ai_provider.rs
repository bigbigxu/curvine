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

//! CIVI AI assistant data source for the master process.
//!
//! Reuses the same master state that backs the metrics/health endpoints, so
//! the agent never needs a second metadata client. Read-only by default;
//! `add_dcm` is a write tool that goes through the confirmation flow in the
//! console (and the same journaled decommission state machine as the CLI).

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use curvine_error::{FsError, FsResult};
use curvine_model::Trigger;
use curvine_web::ai::AiDataProvider;
use serde_json::{json, Value};

use crate::master::fs::MasterFilesystem;

pub struct MasterAiProvider {
    fs: MasterFilesystem,
    conf: Arc<curvine_config::ClusterConf>,
}

impl MasterAiProvider {
    pub fn new(fs: MasterFilesystem, conf: Arc<curvine_config::ClusterConf>) -> Self {
        MasterAiProvider { fs, conf }
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

    /// Directory listing shared with the web API surface.
    fn list_dir_json(&self, path: &str) -> FsResult<Value> {
        let files = self.fs.list_status(path)?;
        serde_json::to_value(files).map_err(|e| FsError::from(e.to_string()))
    }

    /// Worker status shared with the web API surface.
    fn workers_json(&self) -> FsResult<Value> {
        let wm = self.fs.worker_manager.read();
        let mut workers = HashMap::new();
        workers.insert(
            "live_workers".to_string(),
            wm.worker_map
                .workers()
                .iter()
                .map(|(_, w)| w.clone())
                .collect::<Vec<_>>(),
        );
        workers.insert(
            "lost_workers".to_string(),
            wm.worker_map
                .lost_workers
                .iter()
                .map(|(_, w)| w.clone())
                .collect::<Vec<_>>(),
        );
        serde_json::to_value(workers).map_err(|e| FsError::from(e.to_string()))
    }

    /// Session summary shared with the web API surface.
    fn sessions_json(&self) -> FsResult<Value> {
        let sessions = self.fs.session_summary()?;
        serde_json::to_value(sessions).map_err(|e| FsError::from(e.to_string()))
    }

    /// Cluster configuration with secrets redacted.
    fn config_json(&self) -> FsResult<Value> {
        let conf =
            serde_json::to_value(self.conf.as_ref()).map_err(|e| FsError::from(e.to_string()))?;
        Ok(curvine_web::ai::redact_secrets(&conf))
    }

    /// Marks workers as decommissioned through the same journaled state
    /// machine as the RPC/CLI paths.
    fn add_dcm_json(&self, workers: &[String]) -> FsResult<Value> {
        let mut updated = Vec::new();
        for worker in workers.iter().map(|w| w.trim()).filter(|w| !w.is_empty()) {
            self.fs.apply_operator_action(
                worker,
                Trigger::Decommission,
                "rest",
                "civi ai assistant",
            )?;
            updated.push(worker.to_string());
        }
        Ok(json!(updated))
    }
}

// The master serves exactly one cluster: the selector names it or nothing.
#[async_trait]
impl AiDataProvider for MasterAiProvider {
    async fn cluster_overview(&self, _cluster: Option<&str>) -> Result<Value, String> {
        self.overview_json().map_err(|e| e.to_string())
    }

    async fn list_dir(&self, _cluster: Option<&str>, path: &str) -> Result<Value, String> {
        self.list_dir_json(path).map_err(|e| e.to_string())
    }

    async fn worker_status(&self, _cluster: Option<&str>) -> Result<Value, String> {
        self.workers_json().map_err(|e| e.to_string())
    }

    async fn session_summary(&self, _cluster: Option<&str>) -> Result<Value, String> {
        self.sessions_json().map_err(|e| e.to_string())
    }

    async fn cluster_config(&self, _cluster: Option<&str>) -> Result<Value, String> {
        self.config_json().map_err(|e| e.to_string())
    }

    async fn add_dcm(&self, _cluster: Option<&str>, workers: &[String]) -> Result<Value, String> {
        self.add_dcm_json(workers).map_err(|e| e.to_string())
    }
}
