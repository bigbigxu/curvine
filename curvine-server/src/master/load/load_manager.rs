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

#![allow(dead_code)]

use crate::common::ufs_manager::UfsManager;
use crate::master::fs::MasterFilesystem;
use crate::master::{LoadJob, LoadManagerConfig};
use chrono::Utc;
use core::time::Duration;
use curvine_client::file::{FsClient, FsContext};
use curvine_common::conf::ClusterConf;
use curvine_common::fs::CurvineURI;
use curvine_common::fs::RpcCode;
use curvine_common::proto::{
    CancelLoadRequest, CancelLoadResponse, LoadState, LoadTaskReportRequest, LoadTaskRequest,
    LoadTaskResponse,
};
use curvine_common::state::{TtlAction, WorkerAddress};
use dashmap::DashMap;
use log::{error, info, warn};
use orpc::client::ClientFactory;
use orpc::common::DurationUnit;
use orpc::io::net::InetAddr;
use orpc::message::{Builder, RequestStatus};
use orpc::runtime::{RpcRuntime, Runtime};
use orpc::CommonResult;
use std::sync::{Arc, Mutex};
use thiserror::Error;

/// Load the Task Manager
#[derive(Clone)]
pub struct LoadManager {
    /// Task queue
    jobs: Arc<DashMap<String, LoadJob>>,
    /// File system interface
    master_fs: Arc<MasterFilesystem>,
    /// Is it running
    running: Arc<Mutex<bool>>,
    /// Client factory
    client_factory: Arc<ClientFactory>,
    /// Configuration information
    config: LoadManagerConfig,
    /// Runtime
    rt: Arc<Runtime>,
    /// File system
    fs_client: Arc<FsClient>,
}

impl LoadManager {
    pub fn from_cluster_conf(
        fs: Arc<MasterFilesystem>,
        rt: Arc<Runtime>,
        conf: &ClusterConf,
    ) -> Self {
        let fs_context = Arc::new(FsContext::with_rt(conf.clone(), rt.clone()).unwrap());
        let fs_client = Arc::new(FsClient::new(fs_context));
        // let ufs_manager = UfsManager::new(fs_client.clone());
        Self {
            jobs: Arc::new(DashMap::new()),
            master_fs: fs,
            running: Arc::new(Mutex::new(false)),
            client_factory: Arc::new(ClientFactory::default()),
            config: LoadManagerConfig::from_cluster_conf(conf),
            rt,
            fs_client,
        }
    }

    /// Start the load manager
    pub fn start(&self) {
        let mut running = self.running.lock().unwrap();
        if *running {
            info!("LoadManager is already running");
            return;
        }
        *running = true;

        let cleanup_interval = Duration::from_secs(self.config.cleanup_interval_seconds);
        let jobs = self.jobs.clone();
        let fs = self.master_fs.clone();

        self.rt.spawn(async move {
            let mut interval = tokio::time::interval(cleanup_interval);
            loop {
                interval.tick().await;
                Self::cleanup_expired_jobs(fs.clone(), jobs.clone()).await;
            }
        });
        info!("LoadManager started");
    }

    /// Handle cancellation of tasks
    pub(crate) async fn cancel_job(&self, job_id: String) -> CommonResult<bool> {
        // Get task information
        let mut cancel_result = false;
        let assigned_workers = {
            if let Some(mut job) = self.jobs.get_mut(&job_id) {
                // Check whether it can be canceled
                if job.state == LoadState::Completed
                    || job.state == LoadState::Failed
                    || job.state == LoadState::Canceled
                {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        format!("Cannot cancel job in state {}", job.state),
                    )
                    .into());
                }

                // Update status is Cancel
                job.update_state(LoadState::Canceled, Some("Canceling job".to_string()));

                // Get the assigned Worker
                job.assigned_workers.clone()
            } else {
                return Err(LoadManagerError::JobNotFound(job_id).into());
            }
        };

        // Send a cancel request to all assigned Workers
        for worker in assigned_workers {
            info!(
                "Sending cancel request for job {} to worker {}",
                job_id, worker.worker_id
            );
            let worker_addr = InetAddr::new(&worker.ip_addr, worker.rpc_port as u16);
            let worker_client = self.client_factory.create_raw(&worker_addr).await.unwrap();
            let request = CancelLoadRequest {
                job_id: job_id.clone(),
            };

            let msg = Builder::new_rpc(RpcCode::CancelLoadJob)
                .request(RequestStatus::Rpc)
                .proto_header(request)
                .build();

            match worker_client.rpc(msg).await {
                Ok(response) => {
                    let response: CancelLoadResponse = response.parse_header()?;
                    if response.success {
                        cancel_result = response.success;
                        self.update_job_state(&job_id, LoadState::Canceled, None);
                    } else {
                        self.update_job_state(
                            &job_id,
                            LoadState::Failed,
                            Some("Failed to send cancel load request to worker".to_string()),
                        );
                    }
                }
                Err(e) => {
                    error!("Failed to send load task to worker: {}", e);
                    self.update_job_state(
                        &job_id,
                        LoadState::Failed,
                        Some(format!(
                            "Failed to send cancel load request to worker: {}",
                            e
                        )),
                    );
                }
            }
        }
        Ok(cancel_result)
    }

    fn update_job_state(&self, job_id: &str, state: LoadState, message: Option<String>) {
        if let Some(mut job) = self.jobs.get_mut(job_id) {
            job.update_state(state, message);
        }
    }

    /// Handle the task status reported by Worker
    pub async fn handle_task_report(&self, report: LoadTaskReportRequest) -> CommonResult<()> {
        let job_id = report.job_id.clone();
        // Extract indicator information
        let mut task_id = String::new();
        let mut path = String::new();
        let mut target_path = String::new();
        let mut total_size: u64 = 0;
        let mut loaded_size: u64 = 0;
        let message = report.message.unwrap_or_default();
        let state_name = LoadState::from_i32(report.state).unwrap();
        // Extract indicator information
        if let Some(metrics) = report.metrics {
            task_id = metrics.task_id;
            path = metrics.path;
            target_path = metrics.target_path;
            total_size = metrics.total_size.unwrap_or(0) as u64;
            loaded_size = metrics.loaded_size.unwrap_or(0) as u64;
        }

        info!(
            "Received task report for job {}, task {}: state={:?}, loaded={}/{}, message={}.",
            job_id,
            task_id,
            state_name.as_str_name(),
            loaded_size,
            total_size,
            message
        );

        // Update task status
        if let Some(mut job) = self.jobs.get_mut(&job_id) {
            // If you receive a report for this task for the first time, add to the subtask list
            if !job.task_details.contains_key(&task_id) && !task_id.is_empty() {
                job.add_sub_task(
                    task_id.clone(),
                    path.clone(),
                    target_path.clone(),
                    report.worker_id as u32,
                );
            }

            // Update the status of subtasks, as well as progress information + job progress
            job.update_sub_task(
                &task_id,
                LoadState::from_i32(report.state).unwrap(),
                Some(loaded_size),
                Some(total_size),
                Some(message),
            );
        } else {
            warn!("Received status update for unknown job: {}", job_id);
            return Err(LoadManagerError::JobNotFound(job_id).into());
        }

        Ok(())
    }

    /// Submit loading task
    pub async fn submit_job(
        &self,
        path: &str,
        ttl: Option<&str>,
        recursive: bool,
    ) -> CommonResult<(String, String)> {
        info!(
            "Submitting load job for path: {}, recursive: {}",
            path, recursive
        );

        let uri = CurvineURI::new(path)?;
        let mut ufs_manager = UfsManager::new(self.fs_client.clone());
        let target_path = ufs_manager.get_curvine_path(&uri).await?;
        let ufs_client = ufs_manager.get_client(&uri).await?;

        // Check whether the same path already exists
        for entry in self.jobs.iter() {
            let job = entry.value();
            if job.source_path == path
                && job.state != LoadState::Failed
                && job.state != LoadState::Canceled
            {
                warn!(
                    "Found existing job for path {}: job_id={}, state={}",
                    path, job.job_id, job.state
                );
                // return Ok((job.job_id.clone(), job.target_path.clone()));
            }
        }

        let mut job = LoadJob::new(path.to_string(), target_path.clone(), recursive);

        let ttl_duration_unit = if let Some(ttl_str) = ttl {
            match DurationUnit::from_str(ttl_str) {
                Ok(duration_unit) => {
                    let ttl_duration = duration_unit.as_duration();
                    job.expire_time =
                        Some(Utc::now() + chrono::Duration::from_std(ttl_duration).unwrap());
                    Some(duration_unit)
                }
                Err(e) => {
                    warn!("Failed to parse TTL '{}': {}", ttl_str, e);
                    None
                }
            }
        } else {
            None
        };

        let job_id = job.job_id.clone();

        // Add tasks to storage
        self.jobs.insert(job_id.clone(), job);

        let jobs_clone = self.jobs.clone();
        let fs_clone = self.master_fs.clone();
        let fs_clone_1 = self.master_fs.clone();
        let client_factory_clone = self.client_factory.clone();

        let job_id_clone = job_id.clone();
        let target_path_clone = target_path.clone();
        let path = path.to_string();

        tokio::spawn(async move {
            if let Some(mut job_ref) = jobs_clone.get_mut(&job_id_clone) {
                job_ref.update_state(LoadState::Pending, Some("Assigning workers".to_string()));
            }

            // Use UfsClient to determine whether the source path is a directory
            match ufs_client.is_directory(&uri).await {
                Ok(is_dir) => {
                    if is_dir && !recursive {
                        // It is a directory but does not recurse, the update status is failed
                        if let Some(mut job) = jobs_clone.get_mut(&job_id_clone) {
                            job.update_state(
                                LoadState::Failed,
                                Some(
                                    "Path is a directory but recursive flag is not set".to_string(),
                                ),
                            );
                        }
                        return;
                    }

                    let worker = {
                        match Self::choose_worker(fs_clone).await {
                            Ok(w) => w,
                            Err(e) => {
                                error!("Failed to choose workers: {}", e);
                                if let Some(mut job_ref) = jobs_clone.get_mut(&job_id_clone) {
                                    job_ref.update_state(
                                        LoadState::Failed,
                                        Some(format!("Failed to choose workers: {}", e)),
                                    );
                                }
                                return;
                            }
                        }
                    };

                    // Update the Worker allocation of tasks
                    if let Some(mut job_ref) = jobs_clone.get_mut(&job_id_clone) {
                        job_ref.assign_worker(worker.clone());
                        job_ref.update_state(
                            LoadState::Loading,
                            Some(format!(
                                "Assigned to worker {} {}",
                                &worker.worker_id, &worker.hostname
                            )),
                        );
                    }

                    let worker_addr = InetAddr::new(worker.ip_addr.clone(), worker.rpc_port as u16);
                    let worker_client = client_factory_clone
                        .create(&worker_addr, false)
                        .await
                        .unwrap();

                    if !is_dir {
                        let (ttl_ms, ttl_action) = if let Some(duration_unit) = ttl_duration_unit {
                            (
                                Some(duration_unit.as_millis() as i64),
                                Some(TtlAction::Delete.into()),
                            )
                        } else {
                            (None, None)
                        };

                        let request = LoadTaskRequest {
                            job_id: job_id_clone.clone(),
                            source_path: path.to_string(),
                            target_path: target_path.clone(),
                            ttl_ms,
                            ttl_action,
                        };
                        let msg = Builder::new_rpc(RpcCode::SubmitLoadTask)
                            .request(RequestStatus::Rpc)
                            .proto_header(request)
                            .build();

                        // Send a load task request to Worker
                        match worker_client.rpc(msg).await {
                            Ok(response) => {
                                let task_response: LoadTaskResponse =
                                    response.parse_header().unwrap();

                                // Record subtask ID
                                if let Some(mut job) = jobs_clone.get_mut(&job_id_clone) {
                                    job.add_sub_task(
                                        task_response.task_id.clone(),
                                        path.to_string(),
                                        target_path.clone(),
                                        worker.worker_id,
                                    );

                                    info!(
                                        "Added sub-task {} for job {}",
                                        task_response.task_id, job_id_clone
                                    );
                                }
                            }
                            Err(e) => {
                                error!("Failed to send load task to worker: {}", e);
                                if let Some(mut job) = jobs_clone.get_mut(&job_id_clone) {
                                    job.update_state(
                                        LoadState::Failed,
                                        Some(format!("Failed to send load task to worker: {}", e)),
                                    );
                                }
                            }
                        }
                    } else {
                        // Use UfsClient to process the directory directly
                        let files = match ufs_client.list_directory(&uri, recursive).await {
                            Ok(files) => {
                                if files.is_empty() {
                                    if let Some(mut job) = jobs_clone.get_mut(&job_id_clone) {
                                        job.update_state(
                                            LoadState::Failed,
                                            Some(format!(
                                                "Found {} files to process in directory {}",
                                                files.len(),
                                                path
                                            )),
                                        );
                                        return;
                                    }
                                }

                                files
                            }
                            Err(e) => {
                                error!("Failed to process directory: {}", e);
                                if let Some(mut job) = jobs_clone.get_mut(&job_id_clone) {
                                    job.update_state(
                                        LoadState::Failed,
                                        Some(format!("Failed to process directory: {}", e)),
                                    );
                                }
                                return;
                            }
                        };

                        info!(
                            "Found {} files to process in directory {}",
                            files.len(),
                            path
                        );

                        // Send loading tasks for each file
                        for file_path in files {
                            // Extract file name from file path
                            let uri = CurvineURI::new(&file_path).unwrap();
                            let target_path = ufs_manager.get_curvine_path(&uri).await.unwrap();
                            let (ttl_ms, ttl_action) =
                                if let Some(duration_unit) = ttl_duration_unit {
                                    (
                                        Some(duration_unit.as_millis() as i64),
                                        Some(TtlAction::Delete.into()),
                                    )
                                } else {
                                    (None, None)
                                };

                            let request = LoadTaskRequest {
                                job_id: job_id_clone.clone(),
                                source_path: file_path.clone(),
                                target_path: target_path.clone(),
                                ttl_ms,
                                ttl_action,
                            };

                            let msg = Builder::new_rpc(RpcCode::SubmitLoadTask)
                                .request(RequestStatus::Rpc)
                                .proto_header(request)
                                .build()
                                .into_arc();

                            let worker = {
                                match Self::choose_worker(fs_clone_1.clone()).await {
                                    Ok(w) => w,
                                    Err(e) => {
                                        error!("Failed to choose workers: {}", e);
                                        if let Some(mut job_ref) = jobs_clone.get_mut(&job_id_clone)
                                        {
                                            job_ref.update_state(
                                                LoadState::Failed,
                                                Some(format!("Failed to choose workers: {}", e)),
                                            );
                                        }
                                        return;
                                    }
                                }
                            };

                            // Update the Worker allocation of tasks
                            if let Some(mut job_ref) = jobs_clone.get_mut(&job_id_clone) {
                                job_ref.assign_worker(worker.clone());
                                job_ref.update_state(
                                    LoadState::Loading,
                                    Some(format!(
                                        "Assigned to worker {} {}",
                                        &worker.worker_id, &worker.hostname
                                    )),
                                );
                            }

                            let worker_addr =
                                InetAddr::new(worker.ip_addr.clone(), worker.rpc_port as u16);
                            let worker_client = client_factory_clone
                                .create(&worker_addr, false)
                                .await
                                .unwrap();

                            // Send a load task request to Worker
                            match worker_client.rpc(msg).await {
                                Ok(response) => {
                                    let task_response: LoadTaskResponse =
                                        response.parse_header().unwrap();
                                    // Record subtask ID
                                    if let Some(mut job) = jobs_clone.get_mut(&job_id_clone) {
                                        job.add_sub_task(
                                            task_response.task_id.clone(),
                                            file_path.clone(),
                                            uri.to_local_path().unwrap().clone(),
                                            worker.worker_id,
                                        );

                                        info!(
                                            "Added sub-task {} for job {}, sub_path {}",
                                            task_response.task_id,
                                            job_id_clone,
                                            uri.to_local_path().unwrap()
                                        );
                                    }
                                }
                                Err(e) => {
                                    error!("Failed to send load task to worker: {}", e);
                                }
                            }
                        }
                    }
                }
                Err(e) => {
                    error!("Failed to check if directory: {}", e);
                    if let Some(mut job) = jobs_clone.get_mut(&job_id_clone) {
                        job.update_state(
                            LoadState::Failed,
                            Some(format!("Failed to check if directory: {}", e)),
                        );
                    }
                }
            }
        });

        Ok((job_id.clone(), target_path_clone))
    }

    async fn choose_worker(master_fs: Arc<MasterFilesystem>) -> CommonResult<WorkerAddress> {
        let worker_mgr = master_fs.worker_manager.read();
        let workers = worker_mgr
            .choose_workers(1, None)
            .map_err(|e| LoadManagerError::RpcError(format!("Failed to choose workers: {}", e)))?;

        if let Some(worker) = workers.first() {
            Ok(worker.clone())
        } else {
            Err(LoadManagerError::NoAvailableWorker.into())
        }
    }

    /// Get task status
    pub async fn get_load_job_status(&self, job_id: String) -> CommonResult<Option<LoadJob>> {
        match self.jobs.get(&job_id) {
            Some(job_ref) => {
                // DashMap returns Ref, and cloned value is required
                Ok(Some(job_ref.clone()))
            }
            None => {
                //Use the LoadManagerError type in a unified way
                Err(LoadManagerError::JobNotFound(job_id).into())
            }
        }
    }

    async fn cleanup_expired_jobs(
        master_fs: Arc<MasterFilesystem>,
        jobs: Arc<DashMap<String, LoadJob>>,
    ) {
        let now = Utc::now();
        let mut jobs_to_remove = Vec::new();
        info!("Cleaning up expired jobs");

        // Collect tasks that need to be removed first
        for entry in jobs.iter() {
            let job = entry.value();
            if let Some(expire_time) = job.expire_time {
                if now > expire_time {
                    jobs_to_remove.push(job.job_id.clone());
                }
            }
        }

        // Then remove them
        for job_id in jobs_to_remove {
            if let Some(job) = jobs.get_mut(&job_id) {
                info!(
                    "Removing expired job: {}, path: {}",
                    job_id,
                    job.target_path.clone()
                );
                // Delete the target path and handle possible errors
                match master_fs.delete(job.target_path.clone(), true) {
                    Ok(_) => {
                        info!(
                            "Remove expired job: {}, path: {} successful.",
                            job_id,
                            job.target_path.clone()
                        );
                        jobs.remove(&job_id);
                    }
                    Err(e) => {
                        error!("Failed to delete target path for job {}: {}", job_id, e);
                    }
                }
            }
        }
    }
}

/// Load manager error
#[derive(Debug, Error)]
pub enum LoadManagerError {
    #[error("The task does not exist: {0}")]
    JobNotFound(String),
    #[error("The task already exists: {0}")]
    JobAlreadyExists(String),
    #[error("There are no available worker nodes")]
    NoAvailableWorker,
    #[error("RPC error: {0}")]
    RpcError(String),
    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),
}
