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

//! Read-only master state, over the observability plane.
//!
//! Every other admin command talks to the master RPC port, which is not
//! listening while a master replays its journal -- exactly when these answers
//! are needed. This one talks to the web port, which is bound first (#CV-94).
//! It never mutates anything.

use clap::{Parser, Subcommand};
use curvine_config::ClusterConf;
use curvine_core_error::{err_box, CommonResult};
use reqwest::StatusCode;
use serde_json::Value;

/// Header the master checks when `master.ops_token` is configured.
const OPS_TOKEN_HEADER: &str = "x-curvine-ops-token";

#[derive(Parser, Debug)]
pub struct MasterCommand {
    #[command(subcommand)]
    pub action: MasterSubCommand,

    /// Master web address (`host:web_port`). Repeat to inspect several masters.
    #[arg(long = "node", value_name = "HOST:PORT", global = true)]
    pub nodes: Vec<String>,

    /// Print the raw JSON body instead of the human summary
    #[arg(long, global = true)]
    pub json: bool,

    /// Ops token; falls back to the CURVINE_OPS_TOKEN environment variable
    #[arg(long, global = true)]
    pub token: Option<String>,
}

#[derive(Subcommand, Debug)]
pub enum MasterSubCommand {
    /// Phase, watermarks, replay progress and what it is blocked on
    Status,
    /// Phase and watermarks only
    Phase,
    /// What this phase can do, and over which channel
    Capabilities,
}

impl MasterSubCommand {
    fn path(&self) -> &'static str {
        match self {
            MasterSubCommand::Status => "/api/v1/master/ops/status",
            MasterSubCommand::Phase => "/api/v1/master/phase",
            MasterSubCommand::Capabilities => "/api/v1/master/ops/capabilities",
        }
    }
}

impl MasterCommand {
    pub async fn execute(&self, conf: &ClusterConf) -> CommonResult<()> {
        let nodes = self.resolve_nodes(conf);
        if nodes.is_empty() {
            return err_box!("no master web address: pass --node or configure master.hostname");
        }

        let client = reqwest::Client::new();
        let token = self.resolve_token();
        let mut failed = 0;

        for node in &nodes {
            // One unreachable master must not hide the others: recovery leaves
            // the group partially reachable by design.
            match self.fetch(&client, node, token.as_deref()).await {
                Ok(body) => {
                    if nodes.len() > 1 {
                        println!("== {node}");
                    }
                    if self.json {
                        println!("{}", serde_json::to_string_pretty(&body)?);
                    } else {
                        print!("{}", render(self.action_item(), &body));
                    }
                    println!();
                }
                Err(error) => {
                    failed += 1;
                    eprintln!("{}", render_error(node, &error));
                }
            }
        }

        if failed == nodes.len() {
            return err_box!("every master web address failed");
        }
        Ok(())
    }

    fn action_item(&self) -> &MasterSubCommand {
        &self.action
    }

    fn resolve_nodes(&self, conf: &ClusterConf) -> Vec<String> {
        if !self.nodes.is_empty() {
            return self.nodes.clone();
        }
        if conf.master.hostname.is_empty() {
            return Vec::new();
        }
        vec![format!("{}:{}", conf.master.hostname, conf.master.web_port)]
    }

    fn resolve_token(&self) -> Option<String> {
        self.token
            .clone()
            .or_else(|| std::env::var("CURVINE_OPS_TOKEN").ok())
            .filter(|token| !token.is_empty())
    }

    async fn fetch(
        &self,
        client: &reqwest::Client,
        node: &str,
        token: Option<&str>,
    ) -> CommonResult<Value> {
        let url = format!(
            "http://{}{}",
            node.trim_end_matches('/'),
            self.action.path()
        );
        let mut request = client.get(&url);
        if let Some(token) = token {
            request = request.header(OPS_TOKEN_HEADER, token);
        }

        let response = request
            .send()
            .await
            .map_err(|error| curvine_core_error::CommonError::from(format!("{url}: {error}")))?;
        let status = response.status();
        let text = response
            .text()
            .await
            .map_err(|error| curvine_core_error::CommonError::from(format!("{url}: {error}")))?;

        if status != StatusCode::OK {
            return err_box!("{url} returned {status}: {text}");
        }

        serde_json::from_str(&text)
            .map_err(|error| curvine_core_error::CommonError::from(format!("{url}: {error}")))
    }
}

fn render(action: &MasterSubCommand, body: &Value) -> String {
    match action {
        MasterSubCommand::Capabilities => render_capabilities(body),
        _ => render_status(body),
    }
}

fn text(body: &Value, key: &str) -> String {
    match &body[key] {
        Value::String(value) => value.clone(),
        Value::Null => String::new(),
        other => other.to_string(),
    }
}

fn number(body: &Value, key: &str) -> f64 {
    body[key].as_f64().unwrap_or(0.0)
}

/// Human summary: the four questions an operator asks, in order.
fn render_status(body: &Value) -> String {
    let mut out = String::new();
    let phase = text(body, "phase");
    out.push_str(&format!(
        "phase        {phase}  (role {}, term {})\n",
        text(body, "role"),
        text(body, "term"),
    ));
    out.push_str(&format!(
        "applied      {} / committed {}  lag {}\n",
        text(body, "applied"),
        text(body, "committed"),
        text(body, "lag"),
    ));

    let progress = &body["progress"];
    if !progress.is_null() {
        let current = text(progress, "current");
        let total = text(progress, "total");
        match progress["ratio"].as_f64() {
            Some(ratio) => out.push_str(&format!(
                "progress     {current}/{total} entries ({:.1}%)\n",
                ratio * 100.0
            )),
            // A moving denominator gets a percentage-free line on purpose.
            None => out.push_str(&format!(
                "progress     {current}/{total} entries (denominator is moving; see lag)\n"
            )),
        }
    }

    if let Some(rate) = body["rate"]["entries_per_sec"].as_f64() {
        if rate > 0.0 {
            out.push_str(&format!("rate         {rate:.0} entries/s\n"));
        }
    }

    // Nothing outstanding: an ETA or a stall warning would be noise.
    let outstanding = body["lag"].as_f64().unwrap_or(0.0) > 0.0;
    if outstanding {
        if body["estimating"] == Value::Bool(true) {
            out.push_str("eta          estimating...\n");
        } else if let Some(eta) = body["eta_secs"].as_f64() {
            out.push_str(&format!("eta          {}\n", human_secs(eta)));
        }
        if body["stalled"] == Value::Bool(true) {
            out.push_str("stalled      no progress recently\n");
        }
    }

    let reason = &body["reason"];
    if !reason.is_null() {
        let blocking = text(reason, "blocking_on");
        let detail = text(reason, "detail");
        if detail.is_empty() {
            out.push_str(&format!("blocked on   {blocking}\n"));
        } else {
            out.push_str(&format!("blocked on   {blocking}: {detail}\n"));
        }
        if reason["needs_operator"] == Value::Bool(true) {
            out.push_str("action       needs an operator\n");
        }
    }

    let ufs_lag = number(body, "ufs_lag");
    if ufs_lag > 0.0 {
        out.push_str(&format!("ufs lag      {ufs_lag:.0} entries\n"));
    }
    if body["safe_mode"] == Value::Bool(true) {
        out.push_str("safe mode    on (metadata service stopped)\n");
    }
    if body["quarantined"] == Value::Bool(true) {
        out.push_str("quarantine   member recovery in progress\n");
    }
    if text(body, "ops_auth") == "none" {
        out.push_str("ops auth     none (set master.ops_token to require a token)\n");
    }

    out
}

fn render_capabilities(body: &Value) -> String {
    let mut out = format!("phase        {}\n", text(body, "phase"));
    let rows = body["capabilities"].as_array().cloned().unwrap_or_default();
    if rows.is_empty() {
        return out;
    }

    out.push_str(&format!(
        "\n{:<20} {:<8} {:<6} note\n",
        "capability", "value", "via"
    ));
    for row in rows {
        out.push_str(&format!(
            "{:<20} {:<8} {:<6} {}\n",
            text(&row, "capability"),
            text(&row, "value"),
            text(&row, "reachability"),
            text(&row, "note"),
        ));
    }
    out
}

fn human_secs(secs: f64) -> String {
    if secs < 1.0 {
        "less than a second".to_string()
    } else if secs < 60.0 {
        format!("{:.0}s", secs)
    } else if secs < 3_600.0 {
        format!("{:.1}m", secs / 60.0)
    } else {
        format!("{:.1}h", secs / 3_600.0)
    }
}

fn render_error(node: &str, error: &dyn std::fmt::Display) -> String {
    format!("{node}: {error}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cmds::admin::AdminSubCommand;
    use crate::cmds::AdminCommand;
    use clap::Parser;
    use serde_json::json;

    #[test]
    fn admin_master_status_parses() {
        let args = AdminCommand::try_parse_from(["admin", "master", "status"]).unwrap();
        match args.action {
            AdminSubCommand::Master(master) => {
                assert!(matches!(master.action, MasterSubCommand::Status));
                assert!(!master.json);
                assert!(master.nodes.is_empty());
            }
            other => panic!("unexpected subcommand: {other:?}"),
        }
    }

    #[test]
    fn admin_master_accepts_several_nodes_and_json() {
        let args = AdminCommand::try_parse_from([
            "admin", "master", "phase", "--node", "a:9001", "--node", "b:9001", "--json",
        ])
        .unwrap();

        match args.action {
            AdminSubCommand::Master(master) => {
                assert!(matches!(master.action, MasterSubCommand::Phase));
                assert!(master.json);
                assert_eq!(master.nodes, vec!["a:9001", "b:9001"]);
            }
            other => panic!("unexpected subcommand: {other:?}"),
        }
    }

    fn replaying_body() -> Value {
        json!({
            "node": "cv-master-1:9001",
            "phase": "Replaying",
            "phase_code": 2,
            "role": "Init",
            "term": 7,
            "applied": 1200,
            "committed": 5000,
            "lag": 3800,
            "progress": {"current": 200, "total": 4000, "unit": "entries", "denominator": "static", "ratio": 0.05},
            "rate": {"entries_per_sec": 200.0, "samples": 10, "window_ms": 4000},
            "eta_secs": 19.0,
            "estimating": false,
            "stalled": false,
            "reason": {"blocking_on": "replaying", "detail": "applying committed journal entries", "since_ms": 1, "needs_operator": false},
            "ufs_lag": 0,
            "safe_mode": false,
            "quarantined": false,
            "ops_auth": "token"
        })
    }

    #[test]
    fn status_line_names_phase_progress_and_eta() {
        let out = render_status(&replaying_body());
        assert!(out.contains("phase        Replaying"), "{out}");
        assert!(out.contains("lag 3800"), "{out}");
        assert!(out.contains("5.0%"), "{out}");
        assert!(out.contains("200 entries/s"), "{out}");
        assert!(out.contains("eta          19s"), "{out}");
        assert!(out.contains("blocked on   replaying"), "{out}");
        assert!(!out.contains("needs an operator"), "{out}");
    }

    #[test]
    fn an_idle_master_prints_no_stall_or_eta() {
        let mut body = replaying_body();
        body["phase"] = json!("Serving");
        body["applied"] = json!(5000);
        body["committed"] = json!(5000);
        body["lag"] = json!(0);
        body["progress"] = json!({"current": 4000, "total": 4000, "unit": "entries", "denominator": "static", "ratio": 1.0});
        body["eta_secs"] = Value::Null;
        body["estimating"] = json!(true);
        body["stalled"] = json!(true);
        body["reason"] =
            json!({"blocking_on": "none", "detail": "", "since_ms": 1, "needs_operator": false});

        let out = render_status(&body);
        assert!(
            !out.contains("stalled"),
            "an idle master is not stalled: {out}"
        );
        assert!(!out.contains("estimating"), "{out}");
        assert!(out.contains("blocked on   none"), "{out}");
    }

    #[test]
    fn a_moving_denominator_prints_no_percentage() {
        let mut body = replaying_body();
        body["phase"] = json!("CatchingUp");
        body["progress"] = json!({"current": 200, "total": 4000, "unit": "entries", "denominator": "moving", "ratio": null});
        body["eta_secs"] = Value::Null;
        body["estimating"] = json!(true);

        let out = render_status(&body);
        assert!(
            !out.contains('%'),
            "no ratio while the denominator moves: {out}"
        );
        assert!(out.contains("denominator is moving"), "{out}");
        assert!(out.contains("estimating"), "{out}");
    }

    #[test]
    fn damage_and_stalls_are_called_out() {
        let mut body = replaying_body();
        body["phase"] = json!("SafeMode");
        body["safe_mode"] = json!(true);
        body["stalled"] = json!(true);
        body["reason"] = json!({
            "blocking_on": "safe_mode_damage",
            "detail": "journal entry corrupt",
            "since_ms": 1,
            "needs_operator": true
        });

        let out = render_status(&body);
        assert!(out.contains("safe mode    on"), "{out}");
        assert!(out.contains("needs an operator"), "{out}");
        assert!(out.contains("journal entry corrupt"), "{out}");
        assert!(out.contains("no progress recently"), "{out}");
    }

    #[test]
    fn unauthenticated_ops_plane_is_visible() {
        let mut body = replaying_body();
        body["ops_auth"] = json!("none");
        let out = render_status(&body);
        assert!(out.contains("ops auth     none"), "{out}");
    }

    #[test]
    fn capabilities_table_keeps_value_and_channel() {
        let body = json!({
            "phase": "SafeMode",
            "capabilities": [
                {"capability": "metadata-write", "reachability": "none", "value": "deny", "note": "local metadata is damaged"},
                {"capability": "ops-http", "reachability": "http", "value": "allow", "note": ""},
                {"capability": "snapshot-export", "reachability": "http", "value": "degraded", "note": "diagnosis only"}
            ]
        });

        let out = render_capabilities(&body);
        assert!(out.contains("phase        SafeMode"), "{out}");
        assert!(out.contains("metadata-write"), "{out}");
        assert!(out.contains("degraded"), "{out}");
        assert!(out.contains("diagnosis only"), "{out}");
    }

    #[test]
    fn durations_are_human_readable() {
        assert_eq!(human_secs(0.4), "less than a second");
        assert_eq!(human_secs(19.0), "19s");
        assert_eq!(human_secs(90.0), "1.5m");
        assert_eq!(human_secs(7_200.0), "2.0h");
    }

    #[test]
    fn default_node_comes_from_the_master_web_port() {
        let conf = ClusterConf::default();
        let command = MasterCommand {
            action: MasterSubCommand::Status,
            nodes: Vec::new(),
            json: false,
            token: None,
        };
        let nodes = command.resolve_nodes(&conf);
        assert_eq!(
            nodes,
            vec![format!("{}:{}", conf.master.hostname, conf.master.web_port)]
        );
    }
}
