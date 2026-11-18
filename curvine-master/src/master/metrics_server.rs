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

use std::env;
use std::sync::Arc;
use std::time::Duration;

use axum::extract::Query;
use axum::http::HeaderMap;
use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::{Extension, Json, Router};
use curvine_core_error::{err_box, CommonResult};
use curvine_error::FsResult;
use curvine_net::net::{InetAddr, NetUtils};
use curvine_raft::conf::JournalConfExt;
use curvine_raft::raft::storage::{LogStorage, RocksLogStorage};
use curvine_rpc::server::ServerConf;
use curvine_runtime::common::LocalTime;
use curvine_runtime::runtime::{RpcRuntime, Runtime};
use curvine_web::router::ConfigViewState;
use curvine_web::router::{get_config_view, post_update_config, RuntimeConfigHttpResult};
use log::{error, info};
use serde_json::{json, Value};
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio::time::timeout;

use crate::master::fs::MasterFilesystem;
use crate::master::{
    ApplyRate, Master, MasterAiProvider, MasterMonitor, MasterPhase, RATE_MIN_SAMPLES,
};

#[derive(Clone)]
struct MasterMetricsHandler {
    /// `host:web_port` of this process, echoed by `/phase` so an aggregated
    /// multi-node view can label each sample.
    node: String,
    fs: MasterFilesystem,
    /// Journal RocksDB store, exported with the `journal_` metric prefix.
    journal_store: RocksLogStorage,
    /// Snapshot plus live registry behind the `/config` routes.
    config_view: Arc<ConfigViewState>,
    runtime: MasterRuntimeMetadata,
    /// Profiling gate; the flame-graph route is mounted only when this says so.
    profiling: curvine_config::ProfilingConf,
    /// Shared secret for the ops plane; empty means unauthenticated.
    ops_token: Option<String>,
}

/// Header carrying the ops token.
const OPS_TOKEN_HEADER: &str = "x-curvine-ops-token";

#[derive(Clone)]
struct MasterRuntimeMetadata {
    start_time: String,
    start_time_ms: u64,
}

async fn metrics(Extension(handler): Extension<Arc<MasterMetricsHandler>>) -> FsResult<String> {
    let metrics = Master::get_metrics()?;
    Ok(metrics.text_output(handler.fs.clone(), Some(&handler.journal_store))?)
}

async fn healthz(
    Extension(handler): Extension<Arc<MasterMetricsHandler>>,
) -> (StatusCode, &'static str) {
    health_status(probe_view(&handler.fs, &handler.journal_store))
}

async fn readyz(
    Extension(handler): Extension<Arc<MasterMetricsHandler>>,
) -> (StatusCode, &'static str) {
    ready_status(probe_view(&handler.fs, &handler.journal_store))
}

async fn recoveryz(
    Extension(handler): Extension<Arc<MasterMetricsHandler>>,
) -> (StatusCode, &'static str) {
    recovery_status(probe_view(&handler.fs, &handler.journal_store))
}

async fn rolloutz(
    Extension(handler): Extension<Arc<MasterMetricsHandler>>,
) -> (StatusCode, &'static str) {
    rollout_status(probe_view(&handler.fs, &handler.journal_store))
}

/// The HA identity a master publishes about *itself*.
///
/// The console cannot derive this from the leader's raft id: its cluster config
/// maps raft ids to master addresses in journal order, and pod recreation
/// reassigns IPs, so that mapping goes stale and the Active badge lands on a
/// standby. Each master therefore answers with its own view.
struct MasterIdentity {
    is_active: bool,
    raft_id: Option<u64>,
    hostname: String,
    role: String,
    master_state: String,
}

fn master_identity(handler: &MasterMetricsHandler) -> MasterIdentity {
    let monitor = &handler.fs.master_monitor;
    let conf = &handler.config_view.conf;
    let raft_id = if conf.journal.enable {
        conf.journal.node_id().ok()
    } else {
        None
    };
    MasterIdentity {
        is_active: monitor.is_active(),
        raft_id,
        hostname: InetAddr::from_str(&handler.node)
            .map(|addr| addr.hostname)
            .unwrap_or_else(|_| handler.node.clone()),
        role: format!("{:?}", monitor.journal_role()),
        master_state: format!("{:?}", monitor.journal_state()),
    }
}

fn runtime_payload(runtime: &MasterRuntimeMetadata, identity: &MasterIdentity) -> Value {
    let master_version = curvine_sys::version::component_version("master");
    json!({
        "version": master_version.release_version,
        "version_commit": master_version.git_commit,
        "start_time": runtime.start_time.as_str(),
        "start_time_ms": runtime.start_time_ms,
        "is_active": identity.is_active,
        "raft_id": identity.raft_id,
        "hostname": identity.hostname.as_str(),
        "role": identity.role.as_str(),
        "master_state": identity.master_state.as_str(),
    })
}

async fn master_runtime(Extension(handler): Extension<Arc<MasterMetricsHandler>>) -> Json<Value> {
    Json(runtime_payload(
        &handler.runtime,
        &master_identity(&handler),
    ))
}

/// Everything an operator needs to answer "what is this master doing", built
/// from published watermarks only: no raft locks, no fs_dir lock, so it keeps
/// answering while the apply worker is stuck (#CV-94).
fn phase_payload(handler: &MasterMetricsHandler) -> Value {
    let quarantined = is_quarantined(&handler.journal_store);
    let mut payload = phase_payload_from(
        &handler.fs.master_monitor,
        quarantined,
        &handler.node,
        LocalTime::mills(),
    );
    payload["ops_auth"] = json!(ops_auth_label(&handler.ops_token));
    payload
}

fn phase_payload_from(
    monitor: &MasterMonitor,
    quarantined: bool,
    node: &str,
    now_ms: u64,
) -> Value {
    let phase = monitor.phase(quarantined);
    let progress = monitor.progress();
    let entered_at_ms = progress.observe_phase(phase, now_ms);

    let applied = progress.applied();
    let committed = progress.commit();
    let replay_base = progress.replay_base();

    // "Stalled" is a statement about a recovery, not about an idle master: with
    // nothing committed beyond what is applied there is no progress to expect.
    let stall = committed > applied && progress.stalled(now_ms);
    let rate = progress.record_sample(now_ms);
    let (progress_json, eta_secs, estimating) =
        replay_progress(phase, applied, committed, replay_base, rate, stall);
    let reason = recovery_reason(phase, stall, progress.ufs_lag(), monitor.safe_mode_reason());

    json!({
        "node": node,
        "phase": phase.as_str(),
        "phase_code": i8::from(phase),
        "role": format!("{:?}", monitor.journal_role()),
        "master_state": format!("{:?}", monitor.journal_state()),
        "term": monitor.term(),
        "leader_id": monitor.leader_id(),
        "applied": applied,
        "committed": committed,
        "lag": committed.saturating_sub(applied),
        "replay_base": replay_base,
        "progress": progress_json,
        "rate": rate.map(|rate| json!({
            "entries_per_sec": rate.entries_per_sec,
            "samples": rate.samples,
            "window_ms": rate.window_ms,
        })),
        "eta_secs": eta_secs,
        "estimating": estimating,
        "stalled": stall,
        "reason": {
            "blocking_on": reason.blocking_on,
            "detail": reason.detail,
            "since_ms": entered_at_ms,
            "needs_operator": reason.needs_operator,
        },
        "ufs_applied": progress.ufs_applied(),
        "ufs_lag": progress.ufs_lag(),
        "safe_mode": monitor.is_safe_mode(),
        "quarantined": quarantined,
        "entered_at_ms": entered_at_ms,
        "observed_at_ms": now_ms,
    })
}

/// Progress of a catch-up episode, in entries.
///
/// A percentage is only honest while the denominator holds still. When a leader
/// is still catching up its own commit index keeps moving, so the payload
/// reports `denominator: moving` and no ratio/ETA -- a bar that slides backwards
/// is worse than a lag number. `current`/`total` stay absolute, which makes them
/// safe to chart directly.
fn replay_progress(
    phase: MasterPhase,
    applied: u64,
    committed: u64,
    replay_base: u64,
    rate: Option<ApplyRate>,
    stall: bool,
) -> (Value, Option<f64>, bool) {
    let base = replay_base.min(applied);
    let total = committed.saturating_sub(base);
    let current = applied.saturating_sub(base).min(total);
    let moving = phase == MasterPhase::CatchingUp && total > current;

    let ratio = if moving {
        None
    } else if total == 0 {
        Some(1.0)
    } else {
        Some(current as f64 / total as f64)
    };

    let progress = json!({
        "current": current,
        "total": total,
        "unit": "entries",
        "denominator": if moving { "moving" } else { "static" },
        "ratio": ratio,
    });

    let rate = rate.filter(|rate| rate.entries_per_sec > 0.0);
    let remaining = total.saturating_sub(current);
    let eta_secs = match (moving, rate, remaining) {
        (false, Some(rate), remaining) if remaining > 0 => {
            Some(remaining as f64 / rate.entries_per_sec)
        }
        _ => None,
    };
    // Below a few samples the window is dominated by the first batch, so say so
    // instead of showing a number that will change by an order of magnitude.
    let estimating = stall
        || (eta_secs.is_none() && remaining > 0)
        || rate.is_none_or(|rate| rate.samples < RATE_MIN_SAMPLES);

    (progress, eta_secs, estimating)
}

/// Why this master is not serving, in one field.
///
/// The phase says *what* it is doing; this says what it is waiting for and
/// whether waiting is enough. An operator who has to guess between 'wait' and
/// 'act' will act at the wrong time.
#[derive(Debug, Clone, PartialEq)]
struct RecoveryReason {
    blocking_on: &'static str,
    detail: String,
    needs_operator: bool,
}

fn recovery_reason(
    phase: MasterPhase,
    stalled: bool,
    ufs_lag: u64,
    safe_mode_reason: Option<String>,
) -> RecoveryReason {
    let (blocking_on, detail, needs_operator) = match phase {
        MasterPhase::Starting => (
            "awaiting_role",
            "the journal is loaded, waiting for the raft role".to_string(),
            false,
        ),
        MasterPhase::Replaying => (
            "replaying",
            if stalled {
                "replay has not advanced recently; check the apply errors in the master log"
                    .to_string()
            } else {
                "applying committed journal entries".to_string()
            },
            stalled,
        ),
        MasterPhase::CatchingUp => (
            "catching_up",
            if stalled {
                "the leader has not advanced its applied index recently".to_string()
            } else {
                "the elected leader is applying the committed tail".to_string()
            },
            stalled,
        ),
        MasterPhase::Serving if ufs_lag > 0 => (
            "ufs_replay_pending",
            format!("{ufs_lag} entries accepted but not yet exported to UFS"),
            false,
        ),
        MasterPhase::Serving => ("none", String::new(), false),
        MasterPhase::StandingBy => (
            "not_leader",
            "this member is a warm standby; the active leader serves".to_string(),
            false,
        ),
        MasterPhase::Quarantined => (
            "quarantined",
            "rebuilding local state from a healthy peer; the member rejoins by itself".to_string(),
            false,
        ),
        MasterPhase::SafeMode => (
            "safe_mode_damage",
            safe_mode_reason
                .unwrap_or_else(|| "local metadata is damaged; the reason was not recorded".into()),
            true,
        ),
        MasterPhase::Exiting => (
            "exiting",
            "the raft node stopped; the process is expected to be replaced".to_string(),
            false,
        ),
    };

    RecoveryReason {
        blocking_on,
        detail,
        needs_operator,
    }
}

/// The same state as `/ops/status`, so it honours the same token: a probe
/// carries a status code, this carries watermarks, term and leader id.
async fn master_phase(
    Extension(handler): Extension<Arc<MasterMetricsHandler>>,
    headers: HeaderMap,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let phase = current_phase(&handler);
    ops_token_gate(&handler.ops_token, &headers)
        .map_err(|(status, error)| ops_rejected(phase, status, error))?;
    Ok(Json(phase_payload(&handler)))
}

#[derive(Clone, Copy)]
struct ProbeView {
    phase: MasterPhase,
    ufs_caught_up: bool,
}

/// Quarantine is a durable marker in the journal store, so the observability
/// plane reads it there instead of asking the raft node.
fn is_quarantined(journal_store: &RocksLogStorage) -> bool {
    matches!(journal_store.recovery_state(), Ok(Some(_)))
}

fn probe_view(fs: &MasterFilesystem, journal_store: &RocksLogStorage) -> ProbeView {
    ProbeView {
        phase: fs.master_monitor.phase(is_quarantined(journal_store)),
        ufs_caught_up: fs.master_monitor.ufs_caught_up(),
    }
}

/// Liveness: the process is up and still has a raft node to run. That covers
/// `Starting`/`Replaying`, where the journal is being replayed and no role is
/// known yet -- a long recovery must not be restarted by the kubelet. Safe mode
/// stays healthy so a damaged node is not restarted in a loop either. Only
/// `Exiting` is unhealthy.
fn health_status(view: ProbeView) -> (StatusCode, &'static str) {
    if view.phase == MasterPhase::Exiting {
        (StatusCode::SERVICE_UNAVAILABLE, "master has exited\n")
    } else {
        (StatusCode::OK, "ok\n")
    }
}

/// Metadata readiness: only a leader whose metadata has caught up with the
/// committed log. The message names the phase, so a caller can tell "no leader
/// yet" from "leader is still recovering" without a second request.
fn ready_status(view: ProbeView) -> (StatusCode, &'static str) {
    match view.phase {
        MasterPhase::Serving => (StatusCode::OK, "ok\n"),
        MasterPhase::StandingBy => (StatusCode::SERVICE_UNAVAILABLE, "not ready: standby\n"),
        MasterPhase::Starting => (
            StatusCode::SERVICE_UNAVAILABLE,
            "not ready: master is starting\n",
        ),
        MasterPhase::Replaying => (
            StatusCode::SERVICE_UNAVAILABLE,
            "not ready: replaying journal\n",
        ),
        MasterPhase::CatchingUp => (
            StatusCode::SERVICE_UNAVAILABLE,
            "not ready: leader is catching up\n",
        ),
        MasterPhase::Quarantined => (
            StatusCode::SERVICE_UNAVAILABLE,
            "not ready: member recovery in progress\n",
        ),
        MasterPhase::SafeMode => (
            StatusCode::SERVICE_UNAVAILABLE,
            "not ready: master is in safe mode\n",
        ),
        MasterPhase::Exiting => (
            StatusCode::SERVICE_UNAVAILABLE,
            "not ready: master is not serving\n",
        ),
    }
}

/// UFS recovery: serving leader whose UFS watermark has caught the metadata
/// watermark.
fn recovery_status(view: ProbeView) -> (StatusCode, &'static str) {
    if view.phase == MasterPhase::Serving && view.ufs_caught_up {
        (StatusCode::OK, "ok\n")
    } else {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            "not recovered: ufs replay pending\n",
        )
    }
}

/// Rolling update gate. A standby is ready once its role is known. A leader
/// stays unready until UFS replay has caught up, so the controller does not
/// replace the next master while this one is still waiting for workers. A
/// leader that is still replaying is never ready: replacing it would move the
/// recovery to another node.
fn rollout_status(view: ProbeView) -> (StatusCode, &'static str) {
    match view.phase {
        MasterPhase::StandingBy => (StatusCode::OK, "ok\n"),
        MasterPhase::Serving if view.ufs_caught_up => (StatusCode::OK, "ok\n"),
        MasterPhase::Serving => (
            StatusCode::SERVICE_UNAVAILABLE,
            "not recovered: ufs replay pending\n",
        ),
        MasterPhase::CatchingUp | MasterPhase::Replaying => (
            StatusCode::SERVICE_UNAVAILABLE,
            "not ready: replay in progress\n",
        ),
        MasterPhase::Starting
        | MasterPhase::Quarantined
        | MasterPhase::SafeMode
        | MasterPhase::Exiting => (
            StatusCode::SERVICE_UNAVAILABLE,
            "not ready: master is not serving\n",
        ),
    }
}

async fn runtime_config(
    Extension(handler): Extension<Arc<MasterMetricsHandler>>,
) -> RuntimeConfigHttpResult<Json<Value>> {
    get_config_view(Extension(handler.config_view.clone())).await
}

async fn update_runtime_config(
    Extension(handler): Extension<Arc<MasterMetricsHandler>>,
    query: Query<Vec<(String, String)>>,
) -> RuntimeConfigHttpResult<Json<Value>> {
    post_update_config(handler.config_view.registry.clone(), query, Value::Null).await
}

/// Answer for a refused ops call: the phase is part of the body so a caller can
/// tell "wrong token" from "this phase does not offer that".
fn ops_rejected(phase: MasterPhase, status: StatusCode, error: &str) -> (StatusCode, Json<Value>) {
    (
        status,
        Json(json!({
            "phase": phase.as_str(),
            "error": error,
        })),
    )
}

fn current_phase(handler: &MasterMetricsHandler) -> MasterPhase {
    handler
        .fs
        .master_monitor
        .phase(is_quarantined(&handler.journal_store))
}

fn ops_auth_label(ops_token: &Option<String>) -> &'static str {
    if ops_token.is_some() {
        "token"
    } else {
        "none"
    }
}

/// Gate one ops call on the manifest and the optional token.
fn ops_gate(
    phase: MasterPhase,
    ops_token: &Option<String>,
    headers: &HeaderMap,
) -> Result<(), (StatusCode, Json<Value>)> {
    match crate::master::capability(phase, crate::master::Capability::OpsHttp).value {
        crate::master::CapabilityValue::Allow
        | crate::master::CapabilityValue::Degraded
        | crate::master::CapabilityValue::Deferred => {}
        crate::master::CapabilityValue::Deny => {
            return Err(ops_rejected(
                phase,
                StatusCode::SERVICE_UNAVAILABLE,
                "the ops plane is not available in this phase",
            ))
        }
    }

    ops_token_gate(ops_token, headers)
        .map_err(|(status, error)| ops_rejected(phase, status, error))?;

    Ok(())
}

/// Shared-secret check for every endpoint that exposes master state. Probes
/// keep answering without it: they carry a status code, not state.
fn ops_token_gate(
    ops_token: &Option<String>,
    headers: &HeaderMap,
) -> Result<(), (StatusCode, &'static str)> {
    let Some(expected) = ops_token else {
        return Ok(());
    };
    let presented = headers
        .get(OPS_TOKEN_HEADER)
        .and_then(|value| value.to_str().ok());
    if presented == Some(expected.as_str()) {
        Ok(())
    } else {
        Err((StatusCode::UNAUTHORIZED, "missing or invalid ops token"))
    }
}

/// What the manifest says about registering a snapshot in this phase.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SnapshotAdmission {
    Run,
    Deferred,
    Refused,
}

fn snapshot_admission(phase: MasterPhase) -> SnapshotAdmission {
    match crate::master::capability(phase, crate::master::Capability::SnapshotRegister).value {
        crate::master::CapabilityValue::Allow => SnapshotAdmission::Run,
        crate::master::CapabilityValue::Deferred => SnapshotAdmission::Deferred,
        crate::master::CapabilityValue::Degraded | crate::master::CapabilityValue::Deny => {
            SnapshotAdmission::Refused
        }
    }
}

/// Operator snapshot over the web plane.
///
/// The RPC route is gated on `is_active`, so during a recovery the capability
/// existed but the route did not: a node replaying its journal has no 8995
/// listener at all. This route answers with the phase and what the manifest
/// says about `snapshot-register`, so "not allowed" and "not reachable" stop
/// looking the same (#CV-94).
async fn ops_snapshot(
    Extension(handler): Extension<Arc<MasterMetricsHandler>>,
    headers: HeaderMap,
    Query(params): Query<Vec<(String, String)>>,
) -> Result<(StatusCode, Json<Value>), (StatusCode, Json<Value>)> {
    let phase = current_phase(&handler);
    ops_gate(phase, &handler.ops_token, &headers)?;

    let force = params
        .iter()
        .any(|(key, value)| key == "force" && value != "false" && value != "0");

    match snapshot_admission(phase) {
        SnapshotAdmission::Run => {}
        // Accepted, but it runs once the apply worker is free: say so instead
        // of letting the caller time out.
        SnapshotAdmission::Deferred => {
            return Ok((
                StatusCode::ACCEPTED,
                Json(json!({
                    "phase": phase.as_str(),
                    "accepted": false,
                    "deferred": true,
                    "message": "snapshot is queued behind the catch-up scan; retry once the phase is Serving",
                })),
            ))
        }
        SnapshotAdmission::Refused => {
            return Err(ops_rejected(
                phase,
                StatusCode::SERVICE_UNAVAILABLE,
                "this phase does not register a snapshot",
            ))
        }
    }

    let fs = handler.fs.clone();
    // The checkpoint holds the metadata read lock and copies RocksDB, so it
    // must not run on an async worker thread.
    let info = tokio::task::spawn_blocking(move || fs.create_leader_snapshot(force))
        .await
        .map_err(|error| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "phase": phase.as_str(), "error": error.to_string() })),
            )
        })?
        .map_err(|error| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({
                    "phase": phase.as_str(),
                    "error": error.to_string(),
                })),
            )
        })?;

    Ok((
        StatusCode::OK,
        Json(json!({
            "phase": phase.as_str(),
            "accepted": true,
            "deferred": false,
            "dir": info.dir,
            "cost_ms": info.cost_ms,
        })),
    ))
}

fn capabilities_payload(phase: MasterPhase) -> Value {
    let rows: Vec<Value> = crate::master::capabilities(phase)
        .iter()
        .map(|row| {
            json!({
                "capability": row.capability.as_str(),
                "reachability": row.reachability.as_str(),
                "value": row.value.as_str(),
                "note": row.note,
            })
        })
        .collect();

    json!({
        "phase": phase.as_str(),
        "capabilities": rows,
    })
}

async fn ops_status(
    Extension(handler): Extension<Arc<MasterMetricsHandler>>,
    headers: HeaderMap,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let phase = current_phase(&handler);
    ops_gate(phase, &handler.ops_token, &headers)?;
    let mut payload = phase_payload(&handler);
    payload["capabilities"] = capabilities_payload(phase)["capabilities"].clone();
    Ok(Json(payload))
}

async fn ops_capabilities(
    Extension(handler): Extension<Arc<MasterMetricsHandler>>,
    headers: HeaderMap,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let phase = current_phase(&handler);
    ops_gate(phase, &handler.ops_token, &headers)?;
    Ok(Json(capabilities_payload(phase)))
}

async fn ops_safemode(
    Extension(handler): Extension<Arc<MasterMetricsHandler>>,
    headers: HeaderMap,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let phase = current_phase(&handler);
    ops_gate(phase, &handler.ops_token, &headers)?;
    let monitor = &handler.fs.master_monitor;
    let safe_mode = monitor.is_safe_mode();
    Ok(Json(json!({
        "phase": phase.as_str(),
        "safe_mode": safe_mode,
        // Leaving safe mode is an operator decision; this endpoint only reports.
        "exit": "manual",
    })))
}

fn metrics_router(handler: MasterMetricsHandler) -> Router {
    // CIVI AI assistant: same whitelisted tool surface as the console, backed
    // by this master's in-process state. Gated by the optional X-CIVI-Token
    // (see `curvine_web::ai::auth`).
    let provider = Arc::new(MasterAiProvider::new(
        handler.fs.clone(),
        Arc::clone(&handler.config_view.conf),
        Arc::clone(&handler.config_view),
    ));
    // Mounted before the Extension layer: `mount` only adds a route and
    // needs the profiling gate, not the handler state.
    let router = curvine_web::router::profiling::mount(Router::new(), &handler.profiling);
    let router = router
        .route("/metrics", get(metrics))
        .route("/healthz", get(healthz))
        .route("/readyz", get(readyz))
        .route("/recoveryz", get(recoveryz))
        .route("/rolloutz", get(rolloutz))
        .route("/api/v1/master/runtime", get(master_runtime))
        .route("/api/v1/master/phase", get(master_phase))
        .route("/api/v1/master/ops/status", get(ops_status))
        .route("/api/v1/master/ops/capabilities", get(ops_capabilities))
        .route("/api/v1/master/ops/safemode", get(ops_safemode))
        .route("/api/v1/master/ops/snapshot", post(ops_snapshot))
        .route("/api/v1/master/config", get(runtime_config))
        // Frozen for one release: dashboards and curl recipes still scrape this
        // path, so it stays an alias of the full view above.
        .route("/api/v1/master/runtime/config", get(runtime_config))
        .route("/api/update_config", post(update_runtime_config))
        .layer(Extension(Arc::new(handler)));
    router.merge(curvine_web::ai::build_router(provider))
}

/// Minimal HTTP server exposing Prometheus `/metrics`, the `/healthz`
/// (liveness) and `/readyz` (leader-only readiness) probe endpoints, and
/// runtime metadata used by curvine-web.
pub struct MasterMetricsServer {
    rt: Arc<Runtime>,
    fs: MasterFilesystem,
    journal_store: RocksLogStorage,
    conf: ServerConf,
    address: InetAddr,
    /// Snapshot plus live registry behind the `/config` routes.
    config_view: Arc<ConfigViewState>,
    runtime: MasterRuntimeMetadata,
    /// Profiling gate forwarded from `ClusterConf`.
    profiling: curvine_config::ProfilingConf,
    /// Shared secret for the ops plane; empty means unauthenticated.
    ops_token: Option<String>,
}

impl MasterMetricsServer {
    #[allow(clippy::too_many_arguments)]
    pub fn with_rt(
        rt: Arc<Runtime>,
        conf: ServerConf,
        fs: MasterFilesystem,
        journal_store: RocksLogStorage,
        config_view: Arc<ConfigViewState>,
        start_time: String,
        start_time_ms: u64,
        profiling: curvine_config::ProfilingConf,
        ops_token: Option<String>,
    ) -> Self {
        let address = InetAddr::new(&conf.hostname, conf.port);
        Self {
            rt,
            fs,
            journal_store,
            conf,
            address,
            config_view,
            runtime: MasterRuntimeMetadata {
                start_time,
                start_time_ms,
            },
            profiling,
            ops_token,
        }
    }

    pub fn start(self) {
        let rt = self.rt.clone();
        rt.spawn(async move {
            if let Err(err) = self.run().await {
                error!("Master metrics server error: {err}");
            }
        });
    }

    /// Spawns the metrics server and notifies once bind succeeds or fails.
    pub fn start_with_bind_notify(self) -> oneshot::Receiver<CommonResult<()>> {
        let (tx, rx) = oneshot::channel();
        let rt = self.rt.clone();
        rt.spawn(async move {
            match Self::bind_listener(&self).await {
                Ok(listener) => {
                    if tx.send(Ok(())).is_err() {
                        return;
                    }
                    if let Err(err) = Self::serve(
                        listener,
                        self.fs,
                        self.journal_store,
                        self.config_view,
                        self.runtime,
                        self.profiling,
                        self.ops_token.clone(),
                        &self.conf,
                        &self.address,
                    )
                    .await
                    {
                        error!("Master metrics server serve error: {err}");
                    }
                }
                Err(err) => {
                    let _ = tx.send(Err(err));
                }
            }
        });
        rx
    }

    async fn bind_listener(server: &Self) -> CommonResult<TcpListener> {
        match NetUtils::take_held_listener(server.address.port) {
            Some(std_listener) => {
                std_listener.set_nonblocking(true)?;
                Ok(TcpListener::from_std(std_listener)?)
            }
            None => Ok(TcpListener::bind(server.bind_addr()).await?),
        }
    }

    fn bind_addr(&self) -> String {
        let hostname =
            env::var("ORPC_BIND_HOSTNAME").unwrap_or_else(|_| self.address.hostname.clone());
        format!("{hostname}:{}", self.address.port)
    }

    #[allow(clippy::too_many_arguments)]
    async fn serve(
        listener: TcpListener,
        fs: MasterFilesystem,
        journal_store: RocksLogStorage,
        config_view: Arc<ConfigViewState>,
        runtime: MasterRuntimeMetadata,
        profiling: curvine_config::ProfilingConf,
        ops_token: Option<String>,
        conf: &ServerConf,
        address: &InetAddr,
    ) -> CommonResult<()> {
        info!(
            "Master metrics server [{}] listening on {}",
            conf.name, address
        );
        let node = format!("{}:{}", address.hostname, address.port);
        let app = metrics_router(MasterMetricsHandler {
            node,
            fs,
            journal_store,
            config_view,
            runtime,
            profiling,
            ops_token,
        });
        axum::serve(listener, app).await?;
        Ok(())
    }

    async fn run(self) -> CommonResult<()> {
        let listener = Self::bind_listener(&self).await?;
        Self::serve(
            listener,
            self.fs,
            self.journal_store,
            self.config_view,
            self.runtime,
            self.profiling,
            self.ops_token,
            &self.conf,
            &self.address,
        )
        .await
    }
}

pub async fn wait_for_metrics_bind(
    bind_rx: oneshot::Receiver<CommonResult<()>>,
    port: u16,
) -> CommonResult<()> {
    match timeout(Duration::from_secs(5), bind_rx).await {
        Ok(Ok(Ok(()))) => {
            info!("Master metrics server is listening on port {port}");
            Ok(())
        }
        Ok(Ok(Err(err))) => {
            error!("Master metrics server failed to bind on port {port}: {err}");
            Err(err)
        }
        Ok(Err(_)) => {
            error!("Master metrics server startup channel dropped before bind");
            err_box!("Master metrics server startup channel dropped before bind")
        }
        Err(_) => {
            error!("Master metrics server bind timed out after 5s on port {port}");
            err_box!("Master metrics server bind timed out after 5s on port {port}")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use curvine_config::{ClusterConf, ConfigSourceMap, RuntimeConfigRegistry, ViewProfile};
    use curvine_raft::raft::RoleState;
    use curvine_runtime::sync::StateCtl;
    use curvine_web::router::config_view_for_state;
    use std::sync::Arc;

    fn view(phase: MasterPhase, ufs_caught_up: bool) -> ProbeView {
        ProbeView {
            phase,
            ufs_caught_up,
        }
    }

    /// Every phase answers all four probes. A missing arm here is a compile
    /// error in `ready_status`/`rollout_status`, and this table is what keeps
    /// the four answers jointly consistent (#CV-94).
    const ALL_PHASES: [MasterPhase; 8] = [
        MasterPhase::Starting,
        MasterPhase::Replaying,
        MasterPhase::CatchingUp,
        MasterPhase::Serving,
        MasterPhase::StandingBy,
        MasterPhase::Quarantined,
        MasterPhase::SafeMode,
        MasterPhase::Exiting,
    ];

    #[test]
    fn healthz_is_liveness_and_only_exit_is_unhealthy() {
        for phase in ALL_PHASES {
            let status = health_status(view(phase, false));
            if phase == MasterPhase::Exiting {
                assert_eq!(status.0, StatusCode::SERVICE_UNAVAILABLE, "{phase:?}");
                assert_eq!(status.1, "master has exited\n");
            } else {
                // Includes Starting/Replaying: a long recovery must not be
                // restarted by the kubelet.
                assert_eq!(status.0, StatusCode::OK, "{phase:?}");
            }
        }
    }

    #[test]
    fn readyz_is_metadata_service_and_names_the_phase() {
        for phase in ALL_PHASES {
            let status = ready_status(view(phase, true));
            assert_eq!(
                status.0,
                if phase == MasterPhase::Serving {
                    StatusCode::OK
                } else {
                    StatusCode::SERVICE_UNAVAILABLE
                },
                "{phase:?}"
            );
        }

        assert_eq!(
            ready_status(view(MasterPhase::StandingBy, true)).1,
            "not ready: standby\n"
        );
        assert_eq!(
            ready_status(view(MasterPhase::CatchingUp, true)).1,
            "not ready: leader is catching up\n"
        );
        assert_eq!(
            ready_status(view(MasterPhase::Quarantined, true)).1,
            "not ready: member recovery in progress\n"
        );
    }

    #[test]
    fn safe_mode_stays_alive_but_not_ready_or_recovered() {
        let view = view(MasterPhase::SafeMode, false);
        assert_eq!(health_status(view).0, StatusCode::OK);
        assert_eq!(ready_status(view).0, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(recovery_status(view).0, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(rollout_status(view).0, StatusCode::SERVICE_UNAVAILABLE);
    }

    #[test]
    fn rollout_waits_for_ufs_on_the_leader_only() {
        assert_eq!(
            rollout_status(view(MasterPhase::Serving, false)).0,
            StatusCode::SERVICE_UNAVAILABLE
        );
        assert_eq!(
            rollout_status(view(MasterPhase::Serving, true)).0,
            StatusCode::OK
        );
        // A standby is ready regardless of the UFS watermark: it owes no export.
        assert_eq!(
            rollout_status(view(MasterPhase::StandingBy, false)).0,
            StatusCode::OK
        );
        // Replacing a leader that is still replaying moves the recovery.
        assert_eq!(
            rollout_status(view(MasterPhase::CatchingUp, false)).0,
            StatusCode::SERVICE_UNAVAILABLE
        );
        assert_eq!(
            rollout_status(view(MasterPhase::Replaying, false)).0,
            StatusCode::SERVICE_UNAVAILABLE
        );
    }

    #[test]
    fn recoveryz_requires_a_serving_leader_with_ufs_caught_up() {
        assert_eq!(
            recovery_status(view(MasterPhase::Serving, true)).0,
            StatusCode::OK
        );
        for phase in [
            MasterPhase::CatchingUp,
            MasterPhase::Replaying,
            MasterPhase::StandingBy,
            MasterPhase::Quarantined,
            MasterPhase::SafeMode,
            MasterPhase::Starting,
            MasterPhase::Exiting,
        ] {
            assert_eq!(
                recovery_status(view(phase, true)).0,
                StatusCode::SERVICE_UNAVAILABLE,
                "{phase:?}"
            );
        }
    }

    fn headers_with(token: Option<&str>) -> HeaderMap {
        let mut headers = HeaderMap::new();
        if let Some(token) = token {
            headers.insert(OPS_TOKEN_HEADER, token.parse().expect("valid header value"));
        }
        headers
    }

    /// The manifest and the runtime gate must agree: whatever the table says
    /// about `ops-http`, that is what a caller actually gets.
    #[test]
    fn ops_gate_agrees_with_the_manifest_in_every_phase() {
        for phase in ALL_PHASES {
            let expected_allowed =
                crate::master::capability(phase, crate::master::Capability::OpsHttp)
                    .value
                    .is_allowed();
            let result = ops_gate(phase, &None, &HeaderMap::new());

            assert_eq!(
                result.is_ok(),
                expected_allowed,
                "{phase:?} must follow its own manifest"
            );
            if let Err((status, body)) = result {
                assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
                assert_eq!(body.0["phase"], json!(phase.as_str()));
                assert!(body.0["error"].is_string());
            }
        }
    }

    #[test]
    fn ops_gate_requires_the_configured_token() {
        let required = Some("s3cret".to_string());

        let missing = ops_gate(MasterPhase::Serving, &required, &HeaderMap::new());
        assert_eq!(missing.unwrap_err().0, StatusCode::UNAUTHORIZED);

        let wrong = ops_gate(
            MasterPhase::Serving,
            &required,
            &headers_with(Some("guess")),
        );
        assert_eq!(wrong.unwrap_err().0, StatusCode::UNAUTHORIZED);

        assert!(ops_gate(
            MasterPhase::Serving,
            &required,
            &headers_with(Some("s3cret"))
        )
        .is_ok());

        // Unauthenticated by configuration: any caller is accepted.
        assert!(ops_gate(MasterPhase::Serving, &None, &HeaderMap::new()).is_ok());
        assert_eq!(ops_auth_label(&None), "none");
        assert_eq!(ops_auth_label(&required), "token");
    }

    /// /phase exposes the same state as /ops/status, so it must honour the same
    /// token; otherwise the gate is theatre.
    #[test]
    fn the_phase_endpoint_obeys_the_same_token_gate() {
        let required = Some("s3cret".to_string());
        assert_eq!(
            ops_token_gate(&required, &HeaderMap::new()).unwrap_err().0,
            StatusCode::UNAUTHORIZED
        );
        assert!(ops_token_gate(&required, &headers_with(Some("s3cret"))).is_ok());
        assert!(ops_token_gate(&None, &HeaderMap::new()).is_ok());
    }

    /// The route's decision comes from the manifest, and the three outcomes
    /// must stay distinguishable: an operator told "deferred" knows to retry,
    /// one told "refused" knows not to.
    #[test]
    fn snapshot_admission_separates_deferred_from_refused() {
        assert_eq!(
            snapshot_admission(MasterPhase::Serving),
            SnapshotAdmission::Run
        );
        assert_eq!(
            snapshot_admission(MasterPhase::Starting),
            SnapshotAdmission::Run,
            "replay finished and no one writes: the safest moment"
        );
        assert_eq!(
            snapshot_admission(MasterPhase::CatchingUp),
            SnapshotAdmission::Deferred,
            "queued behind the catch-up scan"
        );
        for phase in [
            MasterPhase::Replaying,
            MasterPhase::Quarantined,
            MasterPhase::SafeMode,
            MasterPhase::Exiting,
        ] {
            assert_eq!(
                snapshot_admission(phase),
                SnapshotAdmission::Refused,
                "{phase:?}"
            );
        }
        // A standby may checkpoint locally without ever serving a write.
        assert_eq!(
            snapshot_admission(MasterPhase::StandingBy),
            SnapshotAdmission::Run
        );
    }

    #[test]
    fn capabilities_payload_renders_the_manifest_verbatim() {
        let payload = capabilities_payload(MasterPhase::Serving);
        assert_eq!(payload["phase"], json!("Serving"));

        let rows = payload["capabilities"].as_array().expect("rows");
        assert_eq!(rows.len(), crate::master::Capability::ALL.len());
        assert_eq!(rows[0]["capability"], json!("metadata-read"));
        assert_eq!(rows[0]["value"], json!("allow"));
        assert_eq!(rows[0]["reachability"], json!("rpc"));
        // A deferred row must explain why, otherwise the CLI has nothing to say.
        for row in rows {
            if row["value"] == json!("deferred") || row["value"] == json!("degraded") {
                assert!(
                    row["note"].as_str().is_some_and(|note| !note.is_empty()),
                    "deferred/degraded rows need a note: {row}"
                );
            }
        }
    }

    #[test]
    fn every_phase_names_what_it_is_blocked_on() {
        let reason = |phase, stalled, ufs_lag| recovery_reason(phase, stalled, ufs_lag, None);

        assert_eq!(reason(MasterPhase::Serving, false, 0).blocking_on, "none");
        assert_eq!(
            reason(MasterPhase::Serving, false, 7).blocking_on,
            "ufs_replay_pending"
        );
        assert_eq!(
            reason(MasterPhase::Replaying, false, 0).blocking_on,
            "replaying"
        );
        assert_eq!(
            reason(MasterPhase::CatchingUp, false, 0).blocking_on,
            "catching_up"
        );
        assert_eq!(
            reason(MasterPhase::StandingBy, false, 0).blocking_on,
            "not_leader"
        );
        assert_eq!(
            reason(MasterPhase::Quarantined, false, 0).blocking_on,
            "quarantined"
        );
        assert_eq!(
            reason(MasterPhase::SafeMode, false, 0).blocking_on,
            "safe_mode_damage"
        );
        assert_eq!(
            reason(MasterPhase::Starting, false, 0).blocking_on,
            "awaiting_role"
        );
        assert_eq!(
            reason(MasterPhase::Exiting, false, 0).blocking_on,
            "exiting"
        );
    }

    /// 'Wait' and 'act' must never look the same: only damage and a stalled
    /// replay ask for a human.
    #[test]
    fn only_damage_or_a_stalled_recovery_needs_an_operator() {
        assert!(recovery_reason(MasterPhase::SafeMode, false, 0, None).needs_operator);
        assert!(recovery_reason(
            MasterPhase::SafeMode,
            false,
            0,
            Some("checksum mismatch".into())
        )
        .detail
        .contains("checksum"));

        // Automatic recoveries stay hands-off, including quarantine.
        assert!(!recovery_reason(MasterPhase::Quarantined, false, 0, None).needs_operator);
        assert!(!recovery_reason(MasterPhase::StandingBy, false, 0, None).needs_operator);
        assert!(!recovery_reason(MasterPhase::Serving, false, 5, None).needs_operator);

        // A recovery that stopped moving is no longer automatic.
        assert!(recovery_reason(MasterPhase::Replaying, true, 0, None).needs_operator);
        assert!(recovery_reason(MasterPhase::CatchingUp, true, 0, None).needs_operator);
        assert!(recovery_reason(MasterPhase::Replaying, true, 0, None)
            .detail
            .contains("log"));
    }

    #[test]
    fn phase_payload_reports_watermarks_and_names_the_phase() {
        let monitor = MasterMonitor::new(StateCtl::new(RoleState::Init.into()), StateCtl::new(0));
        let progress = monitor.progress();
        progress.set_watermarks(1_200, 0);
        progress.set_commit(5_000);
        progress.set_replay_base(1_000);

        let body = phase_payload_from(&monitor, false, "cv-master-1:9001", 42);

        assert_eq!(body["node"], json!("cv-master-1:9001"));
        assert_eq!(body["phase"], json!("Replaying"));
        assert_eq!(body["phase_code"], json!(i8::from(MasterPhase::Replaying)));
        assert_eq!(body["role"], json!("Init"));
        assert_eq!(body["applied"], json!(1_200));
        assert_eq!(body["committed"], json!(5_000));
        assert_eq!(body["lag"], json!(3_800));
        assert_eq!(body["replay_base"], json!(1_000));
        assert_eq!(body["quarantined"], json!(false));
        assert_eq!(body["safe_mode"], json!(false));
        assert_eq!(body["observed_at_ms"], json!(42));
        assert!(
            body["entered_at_ms"].is_number(),
            "phase entry must be stamped"
        );
    }

    fn progress_for(phase: MasterPhase, applied: u64, committed: u64, base: u64) -> Value {
        let (progress, _eta, _estimating) =
            replay_progress(phase, applied, committed, base, None, false);
        progress
    }

    #[test]
    fn replay_progress_publishes_a_ratio_only_while_the_denominator_holds_still() {
        // Follower replaying a frozen commit index: a percentage is honest.
        let progress = progress_for(MasterPhase::Replaying, 1_200, 5_000, 1_000);
        assert_eq!(progress["current"], json!(200));
        assert_eq!(progress["total"], json!(4_000));
        assert_eq!(progress["denominator"], json!("static"));
        assert_eq!(progress["ratio"], json!(0.05));
        assert_eq!(progress["unit"], json!("entries"));

        // A leader still catching up sees its commit index move, so it gets no
        // ratio at all instead of a bar that slides backwards.
        let progress = progress_for(MasterPhase::CatchingUp, 1_200, 5_000, 1_000);
        assert_eq!(progress["denominator"], json!("moving"));
        assert_eq!(progress["ratio"], json!(null));
        assert_eq!(progress["current"], json!(200));
    }

    #[test]
    fn replay_progress_is_complete_when_nothing_is_left() {
        for phase in [MasterPhase::Serving, MasterPhase::StandingBy] {
            let progress = progress_for(phase, 5_000, 5_000, 1_000);
            assert_eq!(progress["ratio"], json!(1.0), "{phase:?}");
            assert_eq!(progress["current"], json!(4_000));
        }

        // A brand new master has nothing to replay and must not divide by zero.
        let progress = progress_for(MasterPhase::Starting, 0, 0, 0);
        assert_eq!(progress["ratio"], json!(1.0));
        assert_eq!(progress["total"], json!(0));
    }

    #[test]
    fn replay_progress_never_underflows_behind_the_replay_base() {
        // applied behind the recorded base (marker-only episodes): the ratio
        // must stay in range rather than wrap around.
        let progress = progress_for(MasterPhase::Replaying, 10, 100, 1_000);
        assert_eq!(progress["current"], json!(0));
        assert_eq!(progress["total"], json!(90));
    }

    #[test]
    fn eta_requires_a_rate_and_a_static_denominator() {
        let rate = ApplyRate {
            entries_per_sec: 200.0,
            samples: 10,
            window_ms: 5_000,
        };

        let (_, eta, estimating) =
            replay_progress(MasterPhase::Replaying, 1_000, 5_000, 0, Some(rate), false);
        assert_eq!(eta, Some(20.0), "4000 entries left at 200/s");
        assert!(!estimating);

        // Moving denominator: no ETA, reported as estimating.
        let (_, eta, estimating) =
            replay_progress(MasterPhase::CatchingUp, 1_000, 5_000, 0, Some(rate), false);
        assert_eq!(eta, None);
        assert!(estimating);

        // Too few samples: still estimating even with a static denominator.
        let thin = ApplyRate { samples: 1, ..rate };
        let (_, _, estimating) =
            replay_progress(MasterPhase::Replaying, 1_000, 5_000, 0, Some(thin), false);
        assert!(estimating, "one sample is not a rate");
    }

    #[test]
    fn a_stalled_recovery_is_flagged_even_when_the_math_looks_fine() {
        let rate = ApplyRate {
            entries_per_sec: 200.0,
            samples: 10,
            window_ms: 5_000,
        };
        let (_, _, estimating) =
            replay_progress(MasterPhase::Replaying, 1_000, 5_000, 0, Some(rate), true);
        assert!(
            estimating,
            "a frozen watermark must not look like steady progress"
        );
    }

    #[test]
    fn phase_payload_carries_the_leader_and_quarantine_flags() {
        let monitor = MasterMonitor::new(StateCtl::new(RoleState::Leader.into()), StateCtl::new(0));
        monitor.progress().set_watermarks(10, 10);
        monitor.progress().set_commit(10);

        assert_eq!(
            phase_payload_from(&monitor, true, "n1:9001", 1)["phase"],
            json!("Quarantined")
        );
        assert_eq!(
            phase_payload_from(&monitor, false, "n1:9001", 1)["phase"],
            json!("Serving")
        );
        assert_eq!(
            phase_payload_from(&monitor, false, "n1:9001", 1)["leader_id"],
            json!(0),
            "0 means no leader has been announced yet"
        );
    }

    #[test]
    fn runtime_payload_reports_master_version_and_start_time() {
        let runtime = MasterRuntimeMetadata {
            start_time: "2026-09-24 12:34:56.789".to_string(),
            start_time_ms: 1_727_085_296_789,
        };
        let master_version = curvine_sys::version::component_version("master");
        let identity = MasterIdentity {
            is_active: true,
            raft_id: Some(3),
            hostname: "cv-master-2".to_string(),
            role: "Leader".to_string(),
            master_state: "Active".to_string(),
        };

        let payload = runtime_payload(&runtime, &identity);

        assert_eq!(payload["version"], json!(master_version.release_version));
        assert_eq!(payload["version_commit"], json!(master_version.git_commit));
        assert_eq!(payload["start_time"], json!(runtime.start_time.as_str()));
        assert_eq!(payload["start_time_ms"], json!(runtime.start_time_ms));
        assert_eq!(payload["is_active"], json!(true));
        assert_eq!(payload["raft_id"], json!(3));
        assert_eq!(payload["hostname"], json!("cv-master-2"));
        assert_eq!(payload["role"], json!("Leader"));
        assert_eq!(payload["master_state"], json!("Active"));
    }

    /// The master's `/api/v1/master/config` route must publish the full view, not
    /// the flat map that `GET /api/v1/master/runtime/config` used to return: the
    /// console needs `values` for the tree and `writable` for the edit affordance.
    #[test]
    fn config_view_reports_role_node_and_full_tree() {
        let conf = ClusterConf::default();
        let state = ConfigViewState::new(
            "master",
            format!("{}:{}", conf.master.hostname, conf.master.web_port),
            Arc::new(conf),
            ViewProfile::Full,
            ConfigSourceMap::new(),
            Arc::new(RuntimeConfigRegistry::builder().build()),
        );

        let body = config_view_for_state(&state).expect("config view must build");

        assert_eq!(body["role"], json!("master"));
        assert!(body["node"].is_string(), "node must name the scrape target");
        assert!(
            body["values"]["master"]["hostname"].is_string(),
            "values must carry the effective tree, not a flat key map"
        );
        assert!(body["sources"].is_object());
        assert!(body["runtime"].is_array());
        assert!(body["writable"].is_array());
        assert_eq!(
            body["persistent"],
            json!(false),
            "runtime overrides die with the process"
        );
    }
}
