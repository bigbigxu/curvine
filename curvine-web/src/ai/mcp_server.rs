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

//! CIVI MCP server (Phase 2): exposes the whitelisted Curvine tools over
//! the Model Context Protocol at `POST /mcp` on the web server, so any MCP
//! client (Claude Desktop, IDE agents, ...) can reuse the same permission
//! surface as the chat agent.
//!
//! Implemented as a stateless Streamable-HTTP JSON-RPC endpoint:
//! `initialize`, `notifications/initialized` (202), `tools/list`,
//! `tools/call`.

use std::sync::Arc;

use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde_json::{json, Value};

use crate::ai::auth;
use crate::ai::conf::AiSettingsStore;
use crate::ai::provider::AiDataProvider;
use crate::ai::tools;

pub const PROTOCOL_VERSION: &str = "2025-03-26";
pub const SERVER_NAME: &str = "curvine-civi";
pub const SERVER_VERSION: &str = "0.1.0";

#[derive(Clone)]
pub struct McpServer {
    store: Arc<AiSettingsStore>,
    provider: Arc<dyn AiDataProvider>,
}

impl McpServer {
    pub fn new(store: Arc<AiSettingsStore>, provider: Arc<dyn AiDataProvider>) -> Self {
        McpServer { store, provider }
    }

    pub fn tools_payload(settings: &crate::ai::conf::AiSettings) -> Value {
        let list: Vec<Value> = tools::builtin_tools()
            .into_iter()
            .filter(|t| settings.is_tool_enabled(t.name))
            .map(|t| {
                json!({
                    "name": t.name,
                    "description": t.description,
                    "inputSchema": t.parameters,
                    "annotations": {"readOnlyHint": t.perm == "readonly"}
                })
            })
            .collect();
        json!({ "tools": list })
    }

    async fn handle_rpc(&self, req: Value) -> Response {
        let method = req["method"].as_str().unwrap_or("").to_string();
        let id = req["id"].clone();

        // Notifications (no id) get an empty 202.
        if id.is_null() {
            return StatusCode::ACCEPTED.into_response();
        }

        let result: Result<Value, (i64, String)> = match method.as_str() {
            "initialize" => Ok(json!({
                "protocolVersion": PROTOCOL_VERSION,
                "capabilities": {"tools": {}},
                "serverInfo": {"name": SERVER_NAME, "version": SERVER_VERSION}
            })),
            "ping" => Ok(json!({})),
            "tools/list" => {
                let settings = self.store.get();
                Ok(Self::tools_payload(&settings))
            }
            "tools/call" => {
                let settings = self.store.get();
                if !settings.enabled {
                    Err((-32000, "AI assistant is disabled".to_string()))
                } else {
                    let name = req["params"]["name"].as_str().unwrap_or("");
                    let mut args = req["params"]["arguments"].clone();
                    if args.is_null() {
                        args = json!({});
                    }
                    match tools::dispatch(self.provider.as_ref(), &settings, name, &args, None)
                        .await
                    {
                        Ok(v) => Ok(json!({
                            "content": [{"type": "text", "text": v.to_string()}],
                            "isError": false
                        })),
                        Err(e) => Ok(json!({
                            "content": [{"type": "text", "text": e}],
                            "isError": true
                        })),
                    }
                }
            }
            _ => Err((-32601, format!("method not found: {}", method))),
        };

        let body = match result {
            Ok(value) => json!({"jsonrpc": "2.0", "id": id, "result": value}),
            Err((code, message)) => json!({
                "jsonrpc": "2.0", "id": id,
                "error": {"code": code, "message": message}
            }),
        };
        Json(body).into_response()
    }
}

pub async fn post_mcp(
    State(server): State<Arc<McpServer>>,
    headers: HeaderMap,
    Json(req): Json<Value>,
) -> Response {
    if let Err(resp) = auth::check_token(&server.store, &headers) {
        return *resp;
    }
    server.handle_rpc(req).await
}

pub async fn get_mcp() -> Response {
    (
        StatusCode::METHOD_NOT_ALLOWED,
        Json(json!({"error": "GET is not supported; POST JSON-RPC requests to /mcp"})),
    )
        .into_response()
}
