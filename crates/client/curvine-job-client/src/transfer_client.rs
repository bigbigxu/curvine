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

use curvine_config::ClusterConf;
use curvine_error::{FsError, FsResult};
use curvine_fs_api::RpcCode;
use curvine_model::{
    JobTaskProgress, JobTaskState, TransferCommand, TransferKind, TransferState,
    TransferTaskReportInfo,
};
use curvine_net::net::NodeAddr;
use curvine_proto::{
    CancelTransferRequest, CancelTransferResponse, GetRuntimeConfigRequest,
    GetRuntimeConfigResponse, GetTransferServiceInfoRequest, GetTransferStatusRequest,
    GetTransferStatusResponse, ListTransferTenantsRequest, ListTransferTenantsResponse,
    ListTransfersRequest, ListTransfersResponse, ResetRuntimeConfigRequest,
    ResetRuntimeConfigResponse, RetryTransferRequest, SubmitTransferRequest,
    SubmitTransferResponse, TaskBlockLocationProto, TransferProgressProto,
    TransferServiceInfoResponse, TransferTaskReportBatchRequest, TransferTaskReportBatchResponse,
    TransferTaskReportRequest, TransferTaskReportResponse, UpdateRuntimeConfigRequest,
    UpdateRuntimeConfigResponse, WatchTransferRequest, WatchTransferResponse,
};
use curvine_rpc::client::ClusterConnector;
use curvine_runtime::runtime::Runtime;

use curvine_client_core::file::FsContext;

const TRANSFER_PROTOCOL_VERSION: u32 = 1;
/// Sent only for fs_mode export batches, so an older server rejects them
/// instead of running them as plain directory exports.
const FS_MODE_TRANSFER_PROTOCOL_VERSION: u32 = 2;

#[derive(Clone)]
pub struct TransferClient {
    connector: Arc<ClusterConnector>,
}

#[derive(Clone)]
pub struct TransferTaskProgressReport {
    pub job_id: String,
    pub task_id: String,
    pub info: TransferTaskReportInfo,
    pub progress: JobTaskProgress,
}

impl TransferClient {
    pub fn with_context(context: &Arc<FsContext>) -> FsResult<Self> {
        Self::with_endpoints(
            context.conf().client_rpc_conf(),
            context.clone_runtime(),
            transfer_endpoints(context),
        )
    }

    pub fn with_endpoint(context: &Arc<FsContext>, endpoint: impl Into<String>) -> FsResult<Self> {
        Self::with_report_endpoints(context, vec![endpoint.into()])
    }

    pub fn with_report_endpoints(
        context: &Arc<FsContext>,
        endpoints: Vec<String>,
    ) -> FsResult<Self> {
        Self::with_endpoints(
            context.conf().client_rpc_conf(),
            context.clone_runtime(),
            endpoints,
        )
    }

    pub fn with_rt(conf: &ClusterConf, rt: Arc<Runtime>) -> FsResult<Self> {
        Self::with_endpoints(
            conf.client_rpc_conf(),
            rt,
            configured_transfer_endpoints(conf),
        )
    }

    /// One client pinned to exactly one instance.
    ///
    /// Used by the console to ask every configured `transfer.endpoints` address
    /// "how are you": a status answer must come from *that* instance, never from
    /// whichever one wins a fan-out race.
    pub fn with_single_endpoint(
        conf: &ClusterConf,
        rt: Arc<Runtime>,
        endpoint: impl Into<String>,
    ) -> FsResult<Self> {
        Self::with_endpoints(conf.client_rpc_conf(), rt, vec![endpoint.into()])
    }

    /// One client pinned to one instance, built from an `FsContext`.
    ///
    /// Same as [`TransferClient::with_single_endpoint`] without needing a
    /// `Runtime` handle: the console's cluster contexts hold a `UnifiedFileSystem`
    /// and nothing else, and resolving the same endpoint for every page load must
    /// not require a second runtime.
    pub fn with_context_single_endpoint(
        context: &Arc<FsContext>,
        endpoint: impl Into<String>,
    ) -> FsResult<Self> {
        Self::with_endpoints(
            context.conf().client_rpc_conf(),
            context.clone_runtime(),
            vec![endpoint.into()],
        )
    }

    /// Reads one instance's live configuration knobs (`None` = every key).
    pub async fn get_runtime_config(
        &self,
        key: Option<&str>,
    ) -> FsResult<GetRuntimeConfigResponse> {
        self.connector
            .proto_rpc::<_, GetRuntimeConfigResponse, FsError>(
                RpcCode::GetRuntimeConfig,
                GetRuntimeConfigRequest {
                    key: key.map(str::to_string),
                },
            )
            .await
    }

    /// Applies one runtime override on this instance. Non-persistent.
    pub async fn update_runtime_config(
        &self,
        key: &str,
        value: &str,
    ) -> FsResult<UpdateRuntimeConfigResponse> {
        self.connector
            .proto_rpc::<_, UpdateRuntimeConfigResponse, FsError>(
                RpcCode::UpdateRuntimeConfig,
                UpdateRuntimeConfigRequest {
                    key: key.to_string(),
                    value: value.to_string(),
                    client_context: None,
                },
            )
            .await
    }

    /// Clears one runtime override on this instance, restoring the file value.
    pub async fn reset_runtime_config(&self, key: &str) -> FsResult<ResetRuntimeConfigResponse> {
        self.connector
            .proto_rpc::<_, ResetRuntimeConfigResponse, FsError>(
                RpcCode::ResetRuntimeConfig,
                ResetRuntimeConfigRequest {
                    key: key.to_string(),
                    client_context: None,
                },
            )
            .await
    }

    /// Asks this instance for its own status.
    pub async fn service_info(&self) -> FsResult<TransferServiceInfoResponse> {
        self.connector
            .proto_rpc::<_, TransferServiceInfoResponse, FsError>(
                RpcCode::GetTransferServiceInfo,
                GetTransferServiceInfoRequest {},
            )
            .await
    }

    fn with_endpoints(
        conf: curvine_rpc::client::ClientConf,
        rt: Arc<Runtime>,
        endpoints: Vec<String>,
    ) -> FsResult<Self> {
        let connector = ClusterConnector::with_rt(conf, rt);
        for endpoint in endpoints {
            connector.add_node(NodeAddr::from_str(endpoint)?)?;
        }
        Ok(Self {
            connector: Arc::new(connector),
        })
    }

    pub async fn submit(&self, command: TransferCommand) -> FsResult<SubmitTransferResponse> {
        let req = SubmitTransferRequest {
            kind: transfer_kind_code(command.kind),
            source_path: command.source_path.clone(),
            target_path: command.target_path.clone(),
            client_request_id: command.client_request_id.clone(),
            submitter: command.submitter.clone(),
            tenant: command.tenant.clone(),
            command: serde_json::to_vec(&command).map_err(|e| FsError::common(e.to_string()))?,
            protocol_version: Some(if command.is_fs_mode_batch() {
                FS_MODE_TRANSFER_PROTOCOL_VERSION
            } else {
                TRANSFER_PROTOCOL_VERSION
            }),
            forwarded: None,
        };
        self.connector
            .proto_rpc::<_, SubmitTransferResponse, FsError>(RpcCode::SubmitTransfer, req)
            .await
    }

    pub async fn status(&self, job_id: impl AsRef<str>) -> FsResult<GetTransferStatusResponse> {
        self.status_page(job_id, None, None).await
    }

    pub async fn retry(&self, job_id: impl AsRef<str>) -> FsResult<SubmitTransferResponse> {
        let req = RetryTransferRequest {
            job_id: job_id.as_ref().to_string(),
        };
        self.connector
            .proto_rpc::<_, SubmitTransferResponse, FsError>(RpcCode::RetryTransfer, req)
            .await
    }

    pub async fn status_page(
        &self,
        job_id: impl AsRef<str>,
        page_size: Option<u32>,
        page_token: Option<String>,
    ) -> FsResult<GetTransferStatusResponse> {
        let req = GetTransferStatusRequest {
            job_id: job_id.as_ref().to_string(),
            page_size,
            page_token,
            forwarded: None,
        };
        self.connector
            .proto_rpc::<_, GetTransferStatusResponse, FsError>(RpcCode::GetTransferStatus, req)
            .await
    }

    /// `states` selects any of several states at once (a console bucket); empty
    /// means all. `page_token` is a numeric offset.
    pub async fn list(
        &self,
        kind: Option<TransferKind>,
        states: Vec<TransferState>,
        submitter: Option<String>,
        tenant: Option<String>,
        page_size: Option<u32>,
        page_token: Option<String>,
    ) -> FsResult<ListTransfersResponse> {
        let req = ListTransfersRequest {
            kind: kind.map(transfer_kind_code),
            state: None,
            states: states.into_iter().map(transfer_state_code).collect(),
            page_size,
            page_token,
            submitter,
            tenant,
        };
        self.connector
            .proto_rpc::<_, ListTransfersResponse, FsError>(RpcCode::ListTransfers, req)
            .await
    }

    pub async fn list_tenants(
        &self,
        page_size: Option<u32>,
        page_token: Option<String>,
    ) -> FsResult<ListTransferTenantsResponse> {
        let req = ListTransferTenantsRequest {
            page_size,
            page_token,
        };
        self.connector
            .proto_rpc::<_, ListTransferTenantsResponse, FsError>(RpcCode::ListTransferTenants, req)
            .await
    }

    pub async fn watch(
        &self,
        job_id: impl AsRef<str>,
        since_updated_at: Option<u64>,
        page_size: Option<u32>,
        page_token: Option<String>,
    ) -> FsResult<WatchTransferResponse> {
        let req = WatchTransferRequest {
            job_id: job_id.as_ref().to_string(),
            since_updated_at,
            page_size,
            page_token,
            forwarded: None,
        };
        self.connector
            .proto_rpc::<_, WatchTransferResponse, FsError>(RpcCode::WatchTransfer, req)
            .await
    }

    pub async fn cancel(
        &self,
        job_id: impl AsRef<str>,
        run_id: Option<u64>,
    ) -> FsResult<CancelTransferResponse> {
        let req = CancelTransferRequest {
            job_id: job_id.as_ref().to_string(),
            run_id,
        };
        self.connector
            .proto_rpc::<_, CancelTransferResponse, FsError>(RpcCode::CancelTransfer, req)
            .await
    }

    pub async fn report_task(
        &self,
        job_id: impl AsRef<str>,
        task_id: impl AsRef<str>,
        info: &TransferTaskReportInfo,
        progress: JobTaskProgress,
    ) -> FsResult<bool> {
        let req = task_report_request(TransferTaskProgressReport {
            job_id: job_id.as_ref().to_string(),
            task_id: task_id.as_ref().to_string(),
            info: info.clone(),
            progress,
        });
        let response = self
            .connector
            .proto_rpc::<_, TransferTaskReportResponse, FsError>(RpcCode::ReportTransferTask, req)
            .await?;
        Ok(response.accepted)
    }

    pub async fn report_task_batch(
        &self,
        reports: Vec<TransferTaskProgressReport>,
    ) -> FsResult<Vec<bool>> {
        if reports.is_empty() {
            return Ok(Vec::new());
        }
        let report_count = reports.len();
        let req = TransferTaskReportBatchRequest {
            reports: reports.into_iter().map(task_report_request).collect(),
            protocol_version: Some(TRANSFER_PROTOCOL_VERSION),
        };
        let response = self
            .connector
            .proto_rpc::<_, TransferTaskReportBatchResponse, FsError>(
                RpcCode::ReportTransferTaskBatch,
                req,
            )
            .await?;
        if response.accepted.len() != report_count {
            return Err(FsError::common(format!(
                "Transfer task report batch response count {} does not match request count {}",
                response.accepted.len(),
                report_count
            )));
        }
        Ok(response.accepted)
    }
}

fn task_report_request(report: TransferTaskProgressReport) -> TransferTaskReportRequest {
    TransferTaskReportRequest {
        job_id: report.job_id,
        run_id: report.info.run_id,
        task_id: report.task_id,
        attempt_id: report.info.attempt_id,
        worker_id: report.info.worker_id,
        worker_session_id: report.info.worker_session_id,
        state: transfer_task_state_code(report.progress.state),
        progress: TransferProgressProto {
            loaded_size: report.progress.loaded_size,
            total_size: report.progress.total_size,
            update_time: report.progress.update_time,
            message: report.progress.message,
        },
        accepted_at_ms: Some(report.progress.accepted_at_ms),
        first_byte_at_ms: Some(report.progress.first_byte_at_ms),
        attempt_started_at_ms: Some(report.info.attempt_started_at_ms),
        block_locations: report
            .progress
            .block_locations
            .into_iter()
            .map(|loc| TaskBlockLocationProto {
                block_id: loc.block_id,
                worker_ids: loc.worker_ids,
            })
            .collect(),
        protocol_version: Some(TRANSFER_PROTOCOL_VERSION),
    }
}

fn transfer_endpoints(context: &Arc<FsContext>) -> Vec<String> {
    transfer_endpoints_from_conf(context.conf())
}

/// The instance list this configuration points at.
///
/// `transfer.endpoints` is authoritative (there is no registry or heartbeat);
/// an empty list means a single-instance deployment that never configured one,
/// which [`TransferConf::init`] already normalizes to its own `hostname:rpc_port`.
pub fn configured_transfer_endpoints(conf: &ClusterConf) -> Vec<String> {
    if conf.transfer.endpoints.is_empty() {
        vec![format!(
            "{}:{}",
            conf.transfer.hostname, conf.transfer.rpc_port
        )]
    } else {
        conf.transfer.endpoints.clone()
    }
}

fn transfer_endpoints_from_conf(conf: &ClusterConf) -> Vec<String> {
    configured_transfer_endpoints(conf)
}

fn transfer_kind_code(kind: TransferKind) -> i32 {
    kind as i32
}

fn transfer_state_code(state: TransferState) -> i32 {
    state as i32
}

fn transfer_task_state_code(state: JobTaskState) -> i32 {
    match state {
        JobTaskState::Pending | JobTaskState::UNKNOWN => 1,
        JobTaskState::Loading => 2,
        JobTaskState::Completed => 3,
        // Job-level PartialSuccess has no task-level counterpart; report as failed.
        JobTaskState::Failed | JobTaskState::PartialSuccess => 4,
        JobTaskState::Canceled => 5,
    }
}
