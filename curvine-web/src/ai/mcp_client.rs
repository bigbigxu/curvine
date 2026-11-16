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

//! Minimal MCP (Model Context Protocol) client over Streamable HTTP
//! (Phase 3).
//!
//! Supports the subset needed to pull tools from an external MCP server:
//! `initialize` → `notifications/initialized` → `tools/list`, then
//! `tools/call` per invocation. Responses may be JSON or SSE; both are
//! handled. Sessions are not persisted between calls (each call re-opens a
//! session), which every stateless-capable server supports.

use std::collections::HashMap;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct McpTool {
    pub name: String,
    #[serde(default)]
    pub description: Value,
    #[serde(rename = "inputSchema", default)]
    pub input_schema: Value,
}

fn http_client() -> Result<reqwest::Client, String> {
    reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(10))
        .timeout(Duration::from_secs(60))
        .build()
        .map_err(|e| e.to_string())
}

/// Sends one JSON-RPC request and returns the result (None for
/// notifications) plus the session id header if the server sent one.
async fn rpc(
    client: &reqwest::Client,
    url: &str,
    session_id: Option<&str>,
    method: &str,
    params: Value,
    notification: bool,
) -> Result<(Option<Value>, Option<String>), String> {
    let mut body = json!({"jsonrpc": "2.0", "method": method});
    if !params.is_null() {
        body["params"] = params;
    }
    if !notification {
        body["id"] = json!(1);
    }

    let mut req = client
        .post(url)
        .header("Accept", "application/json, text/event-stream")
        .json(&body);
    if let Some(sid) = session_id {
        req = req.header("mcp-session-id", sid);
    }
    let resp = req.send().await.map_err(|e| e.to_string())?;
    let status = resp.status();
    let session = resp
        .headers()
        .get("mcp-session-id")
        .and_then(|v| v.to_str().ok())
        .map(str::to_string);
    if !status.is_success() {
        let text = resp.text().await.unwrap_or_default();
        return Err(format!("MCP server returned {}: {}", status, text));
    }
    if notification {
        return Ok((None, session));
    }

    let content_type = resp
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();

    let payload: Value = if content_type.contains("text/event-stream") {
        let text = resp.text().await.map_err(|e| e.to_string())?;
        // Take the first `data:` JSON line of the SSE stream.
        text.lines()
            .filter_map(|l| l.strip_prefix("data:"))
            .find_map(|d| serde_json::from_str::<Value>(d.trim()).ok())
            .ok_or_else(|| "MCP server sent an empty SSE stream".to_string())?
    } else {
        resp.json::<Value>().await.map_err(|e| e.to_string())?
    };

    if let Some(err) = payload.get("error") {
        return Err(format!(
            "MCP error {}: {}",
            err.get("code").and_then(Value::as_i64).unwrap_or(0),
            err.get("message").and_then(Value::as_str).unwrap_or("?")
        ));
    }
    Ok((Some(payload["result"].clone()), session))
}

/// Lists the tools offered by an MCP server.
pub async fn list_tools(url: &str) -> Result<Vec<McpTool>, String> {
    let client = http_client()?;
    let init = json!({
        "protocolVersion": "2025-03-26",
        "capabilities": {},
        "clientInfo": {"name": "curvine-civi", "version": "0.1.0"}
    });
    let (_, session) = rpc(&client, url, None, "initialize", init, false).await?;
    let session_id = session.as_deref();

    rpc(
        &client,
        url,
        session_id,
        "notifications/initialized",
        json!({}),
        true,
    )
    .await?;

    let (result, _) = rpc(&client, url, session_id, "tools/list", json!({}), false).await?;
    let result = result.ok_or("empty tools/list response")?;
    let tools = result
        .get("tools")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    Ok(tools
        .into_iter()
        .filter_map(|t| serde_json::from_value::<McpTool>(t).ok())
        .collect())
}

/// Invokes a tool on an MCP server.
pub async fn call_tool(url: &str, name: &str, args: &Value) -> Result<Value, String> {
    let client = http_client()?;
    let init = json!({
        "protocolVersion": "2025-03-26",
        "capabilities": {},
        "clientInfo": {"name": "curvine-civi", "version": "0.1.0"}
    });
    let (_, session) = rpc(&client, url, None, "initialize", init, false).await?;
    let session_id = session.as_deref();
    rpc(
        &client,
        url,
        session_id,
        "notifications/initialized",
        json!({}),
        true,
    )
    .await?;

    let (result, _) = rpc(
        &client,
        url,
        session_id,
        "tools/call",
        json!({"name": name, "arguments": args}),
        false,
    )
    .await?;
    let result = result.ok_or("empty tools/call response")?;

    // Normalize the MCP result into a plain JSON value for the model.
    let is_error = result
        .get("isError")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let mut text = String::new();
    if let Some(content) = result.get("content").and_then(Value::as_array) {
        for item in content {
            if let Some(t) = item.get("text").and_then(Value::as_str) {
                if !text.is_empty() {
                    text.push('\n');
                }
                text.push_str(t);
            }
        }
    }
    if text.is_empty() {
        Ok(json!({"ok": !is_error, "data": result}))
    } else if is_error {
        Err(text)
    } else {
        // Try to keep structured data structured.
        match serde_json::from_str::<Value>(&text) {
            Ok(v) => Ok(v),
            Err(_) => Ok(json!({"text": text})),
        }
    }
}

/// Maps `server_name -> url` lookups for prefixed tool dispatch.
pub fn server_url<'a>(
    servers: &'a [crate::ai::conf::McpServerConf],
    prefix: &str,
) -> Option<&'a str> {
    servers
        .iter()
        .find(|s| mcp_prefix(&s.name) == prefix)
        .map(|s| s.url.as_str())
}

/// External tool name for a given server/tool pair.
pub fn external_name(server: &str, tool: &str) -> String {
    format!("mcp_{}_{}", sanitize(server), sanitize(tool))
}

/// Splits `mcp_<server>_<tool>` back into (server_prefix, tool).
pub fn split_external(name: &str) -> Option<(String, String)> {
    let rest = name.strip_prefix("mcp_")?;
    let idx = rest.find('_')?;
    let (server, tool) = rest.split_at(idx);
    Some((server.to_string(), tool[1..].to_string()))
}

fn sanitize(s: &str) -> String {
    s.chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '_' })
        .collect()
}

pub fn mcp_prefix(server: &str) -> String {
    sanitize(server)
}

pub type McpToolIndex = HashMap<String, (String, McpTool)>;
