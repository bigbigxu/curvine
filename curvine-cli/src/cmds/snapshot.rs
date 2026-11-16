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

use crate::util::*;
use clap::{Parser, Subcommand};
use curvine_client_core::file::FsClient;
use curvine_config::ClusterConf;
use curvine_core_error::{err_box, CommonResult};
use serde_json::Value;
use std::sync::Arc;

/// Header the master checks when `master.ops_token` is configured.
const OPS_TOKEN_HEADER: &str = "x-curvine-ops-token";

/// Phases where the data plane is not (yet) serving, so the only reachable
/// route is the master's web port.
const RECOVERY_PHASES: [&str; 4] = ["Starting", "Replaying", "CatchingUp", "Quarantined"];

#[derive(Parser, Debug)]
pub struct SnapshotCommand {
    #[command(subcommand)]
    action: SnapshotAction,
}

#[derive(Subcommand, Debug)]
pub enum SnapshotAction {
    /// Ask the active master to create a metadata snapshot now
    ///
    /// The checkpoint runs on the master and cannot be cancelled. On a large
    /// namespace it can exceed `client.rpc_timeout_ms` (120s by default) even
    /// though the snapshot succeeds; the retry then reports `a leader snapshot
    /// is already in progress`. Raise `client.rpc_timeout_ms` in the
    /// configuration file passed with `--conf` before running this on a large
    /// namespace.
    ///
    /// A call inside `snapshot_min_interval` is rejected; `--force` runs the
    /// checkpoint anyway, stalling metadata writes for its duration.
    #[command(name = "create")]
    Create {
        /// Create the checkpoint even if the last one is inside snapshot_min_interval
        #[arg(long)]
        force: bool,

        /// Master web address (`host:web_port`) used when the data plane is not
        /// serving. Defaults to `master.hostname:master.web_port`.
        #[arg(long, value_name = "HOST:PORT")]
        node: Option<String>,

        /// Ops token; falls back to the CURVINE_OPS_TOKEN environment variable
        #[arg(long)]
        token: Option<String>,
    },
}

impl SnapshotCommand {
    pub async fn execute(&self, client: Arc<FsClient>, conf: &ClusterConf) -> CommonResult<()> {
        match &self.action {
            SnapshotAction::Create { force, node, token } => {
                let token = token
                    .clone()
                    .or_else(|| std::env::var("CURVINE_OPS_TOKEN").ok());
                let web = node.clone().unwrap_or_else(|| {
                    format!("{}:{}", conf.master.hostname, conf.master.web_port)
                });

                // The data plane is only up once a master serves. Ask the web
                // plane first: it is bound before the raft role is known, so it
                // can answer exactly when the RPC route cannot.
                let http = reqwest::Client::new();
                match probe_phase(&http, &web, token.as_deref()).await {
                    Ok(Some(phase)) if is_recovery(&phase) => {
                        println!("master {web} is in {phase}: creating the snapshot over HTTP");
                        return create_via_http(&http, &web, token.as_deref(), *force).await;
                    }
                    Ok(Some(phase)) => {
                        println!("master {web} is {phase}: using the data plane");
                    }
                    // Web plane unreachable or too old to know /phase: behave
                    // exactly as before, so this stays a drop-in change.
                    Ok(None) | Err(_) => {}
                }

                let info = handle_rpc_result(client.create_leader_snapshot(*force)).await;
                println!("snapshot dir: {}", info.dir);
                println!("checkpoint cost: {} ms", info.cost_ms);
                println!("journal truncation and checkpoint retention are applied asynchronously");
                Ok(())
            }
        }
    }
}

fn is_recovery(phase: &str) -> bool {
    RECOVERY_PHASES.contains(&phase)
}

/// `Ok(Some(phase))` when the master answered, `Ok(None)` when the endpoint is
/// missing (older build), `Err` when the web plane is unreachable.
async fn probe_phase(
    client: &reqwest::Client,
    web: &str,
    token: Option<&str>,
) -> CommonResult<Option<String>> {
    let url = format!("http://{}/api/v1/master/phase", web.trim_end_matches('/'));
    let mut request = client.get(&url).timeout(std::time::Duration::from_secs(2));
    if let Some(token) = token {
        request = request.header(OPS_TOKEN_HEADER, token);
    }

    let response = request
        .send()
        .await
        .map_err(|error| curvine_core_error::CommonError::from(format!("{url}: {error}")))?;
    if response.status() == reqwest::StatusCode::NOT_FOUND {
        return Ok(None);
    }
    if response.status() != reqwest::StatusCode::OK {
        return err_box!("{url} returned {}", response.status());
    }
    let body: Value = response
        .json()
        .await
        .map_err(|error| curvine_core_error::CommonError::from(format!("{url}: {error}")))?;
    Ok(body["phase"].as_str().map(|phase| phase.to_string()))
}

async fn create_via_http(
    client: &reqwest::Client,
    web: &str,
    token: Option<&str>,
    force: bool,
) -> CommonResult<()> {
    let url = format!(
        "http://{}/api/v1/master/ops/snapshot?force={}",
        web.trim_end_matches('/'),
        force
    );
    let mut request = client.post(&url);
    if let Some(token) = token {
        request = request.header(OPS_TOKEN_HEADER, token);
    }

    let response = request
        .send()
        .await
        .map_err(|error| curvine_core_error::CommonError::from(format!("{url}: {error}")))?;
    let status = response.status();
    let body: Value = response.json().await.unwrap_or(Value::Null);

    match status {
        reqwest::StatusCode::OK => {
            println!("snapshot dir: {}", body["dir"].as_str().unwrap_or("-"));
            println!(
                "checkpoint cost: {} ms",
                body["cost_ms"].as_u64().unwrap_or_default()
            );
            println!("phase was {}", body["phase"].as_str().unwrap_or("-"));
            Ok(())
        }
        // Accepted but not run: retrying is the right move, not giving up.
        reqwest::StatusCode::ACCEPTED => err_box!(
            "snapshot is queued, not created: {}",
            body["message"].as_str().unwrap_or("retry later")
        ),
        _ => err_box!(
            "snapshot refused in phase {}: {}",
            body["phase"].as_str().unwrap_or("-"),
            body["error"].as_str().unwrap_or("unknown error")
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    #[test]
    fn create_parses_node_and_force() {
        let cmd = SnapshotCommand::try_parse_from([
            "snapshot",
            "create",
            "--force",
            "--node",
            "cv-master-1:9001",
        ])
        .expect("parse");

        match cmd.action {
            SnapshotAction::Create { force, node, .. } => {
                assert!(force);
                assert_eq!(node.as_deref(), Some("cv-master-1:9001"));
            }
        }
    }

    /// The channel choice is the whole point: a master that is still replaying
    /// has no data plane at all, so the web plane must be used.
    #[test]
    fn recovery_phases_route_to_http() {
        for phase in ["Starting", "Replaying", "CatchingUp", "Quarantined"] {
            assert!(is_recovery(phase), "{phase} must use the web plane");
        }
        for phase in ["Serving", "StandingBy", "SafeMode", "Exiting"] {
            assert!(!is_recovery(phase), "{phase} keeps the data plane path");
        }
    }
}
