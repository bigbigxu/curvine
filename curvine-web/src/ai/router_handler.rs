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

//! HTTP surface of the CIVI assistant: settings API, skills, streaming chat
//! (with write-tool confirmation flow), and the MCP server at `/mcp`.

use std::collections::HashMap;
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, Mutex};

use axum::http::{HeaderMap, StatusCode};
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Extension, Json, Router};
use futures::StreamExt;
use serde_json::{json, Value};

use crate::ai::agent::{Agent, AgentEvent, ChatBody, PendingCall};
use crate::ai::audit::AuditLogger;
use crate::ai::auth;
use crate::ai::conf::{AiSettings, AiSettingsStore};
use crate::ai::grants::SessionGrants;
use crate::ai::limiter::RateLimiter;
use crate::ai::mcp_server::{self, McpServer};
use crate::ai::provider::AiDataProvider;
use crate::ai::skills;
use crate::ai::tools;

#[derive(Clone)]
pub struct AiRouterHandler {
    store: Arc<AiSettingsStore>,
    provider: Arc<dyn AiDataProvider>,
    audit: AuditLogger,
    limiter: RateLimiter,
    call_counter: Arc<AtomicU64>,
    /// Write-tool calls waiting for user confirmation.
    pending: Arc<Mutex<HashMap<String, PendingCall>>>,
    /// Session-scoped write grants ("approve all for this session").
    grants: Arc<SessionGrants>,
}

impl AiRouterHandler {
    pub fn new(provider: Arc<dyn AiDataProvider>) -> Self {
        AiRouterHandler {
            store: Arc::new(AiSettingsStore::load_default()),
            provider,
            audit: AuditLogger::load_default(),
            limiter: RateLimiter::new(),
            call_counter: Arc::new(AtomicU64::new(0)),
            pending: Arc::new(Mutex::new(HashMap::new())),
            grants: Arc::new(SessionGrants::new()),
        }
    }

    /// Optional shared-token gate (see `auth`).
    pub fn check_token(&self, headers: &HeaderMap) -> Result<(), Box<Response>> {
        auth::check_token(&self.store, headers)
    }
}

fn settings_payload(handler: &AiRouterHandler) -> Value {
    let settings = handler.store.get();
    let available: Vec<Value> = tools::builtin_tools()
        .iter()
        .map(|t| {
            json!({
                "name": t.name,
                "description": t.description,
                "perm": t.perm,
                "enabled": settings.is_tool_enabled(t.name)
            })
        })
        .collect();
    json!({
        "settings": settings.masked(),
        "api_key_set": !handler.store.api_key().is_empty(),
        "available_tools": available,
        "skills": skills::builtin_skills()
    })
}

async fn get_settings(Extension(handler): Extension<Arc<AiRouterHandler>>) -> Json<Value> {
    Json(settings_payload(&handler))
}

async fn update_settings(
    Extension(handler): Extension<Arc<AiRouterHandler>>,
    headers: HeaderMap,
    Json(incoming): Json<AiSettings>,
) -> Response {
    if let Err(resp) = handler.check_token(&headers) {
        return *resp;
    }
    handler.store.update(incoming);
    handler.audit.record("settings_updated", json!({}));
    Json(settings_payload(&handler)).into_response()
}

async fn get_skills(Extension(_handler): Extension<Arc<AiRouterHandler>>) -> Json<Value> {
    Json(json!({"skills": skills::builtin_skills()}))
}

async fn chat(
    Extension(handler): Extension<Arc<AiRouterHandler>>,
    headers: HeaderMap,
    Json(body): Json<ChatBody>,
) -> Response {
    let settings = handler.store.get();
    if !settings.enabled {
        return forbidden("AI assistant is disabled");
    }
    if settings.endpoint.trim().is_empty() || settings.model.trim().is_empty() {
        return bad_request("AI endpoint or model is not configured");
    }
    if let Err(resp) = handler.check_token(&headers) {
        return *resp;
    }

    match body {
        ChatBody::New(req) => {
            let limit = settings.rate_limit_per_min;
            if !handler.limiter.allow(limit) {
                let retry = handler.limiter.retry_after_ms(limit);
                handler
                    .audit
                    .record("rate_limited", json!({"retry_after_ms": retry}));
                return (
                    StatusCode::TOO_MANY_REQUESTS,
                    Json(json!({"message": format!("rate limit exceeded, retry in {} ms", retry)})),
                )
                    .into_response();
            }
            let messages = match Agent::normalize_messages(&req.messages) {
                Ok(m) => m,
                Err(e) => return bad_request(&e),
            };
            handler.audit.record_chat(messages.len(), "start");

            let agent = Arc::new(Agent::new(
                handler.provider.clone(),
                settings,
                handler.audit.clone(),
                handler.call_counter.clone(),
                req.cluster,
                req.locale,
                req.session_id,
                handler.grants.clone(),
            ));
            sse_response(&handler, agent.run(messages, None))
        }
        ChatBody::Resume(req) => {
            let pending = handler
                .pending
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .remove(&req.resume_call_id);
            let pending = match pending {
                Some(p) => p,
                None => return bad_request("unknown or expired confirmation call id"),
            };
            handler.audit.record(
                "confirm_resolved",
                json!({"call_id": pending.call_id, "tool": pending.name, "approve": req.approve}),
            );

            // "Approve all" grants the tool for the rest of the session so
            // batch write flows do not need one card per call.
            if req.approve && req.approve_all {
                if let Some(session) = pending.session_id.as_deref() {
                    handler.grants.grant(session, &pending.name);
                }
                handler.audit.record(
                    "session_granted",
                    json!({"tool": pending.name, "session": pending.session_id}),
                );
            }

            let agent = Arc::new(Agent::new(
                handler.provider.clone(),
                settings,
                handler.audit.clone(),
                handler.call_counter.clone(),
                pending.cluster.clone(),
                None,
                pending.session_id.clone(),
                handler.grants.clone(),
            ));
            let resume = (pending.call_id, pending.name, pending.args, req.approve);
            sse_response(&handler, agent.run(pending.messages, Some(resume)))
        }
    }
}

fn sse_response(
    handler: &Arc<AiRouterHandler>,
    stream: std::pin::Pin<Box<dyn futures::Stream<Item = AgentEvent> + Send>>,
) -> Response {
    let pending_store = handler.pending.clone();
    let event_stream = stream.map(move |event| {
        let (name, data): (&str, Value) = match event {
            AgentEvent::Delta { content } => ("delta", json!({"content": content})),
            AgentEvent::ToolCall { name, args } => {
                ("tool_call", json!({"name": name, "args": args}))
            }
            AgentEvent::ToolResult { name, ok, preview } => (
                "tool_result",
                json!({"name": name, "ok": ok, "preview": preview}),
            ),
            AgentEvent::ConfirmRequired {
                call_id,
                name,
                args,
                messages,
                cluster,
                session_id,
            } => {
                // Park the write call until the user approves or rejects it.
                pending_store
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .insert(
                        call_id.clone(),
                        PendingCall {
                            call_id: call_id.clone(),
                            name: name.clone(),
                            args: args.clone(),
                            messages,
                            cluster,
                            session_id,
                        },
                    );
                (
                    "confirm_required",
                    json!({"call_id": call_id, "name": name, "args": args}),
                )
            }
            AgentEvent::Done { finish_reason } => ("done", json!({"finish_reason": finish_reason})),
            AgentEvent::Error { message } => ("error", json!({"message": message})),
        };
        Ok::<Event, std::convert::Infallible>(Event::default().event(name).data(data.to_string()))
    });
    Sse::new(event_stream)
        .keep_alive(KeepAlive::default())
        .into_response()
}

fn forbidden(message: &str) -> Response {
    (StatusCode::FORBIDDEN, Json(json!({"message": message}))).into_response()
}

fn bad_request(message: &str) -> Response {
    (StatusCode::BAD_REQUEST, Json(json!({"message": message}))).into_response()
}

pub fn build_router(provider: Arc<dyn AiDataProvider>) -> Router {
    let handler = AiRouterHandler::new(provider);
    let mcp = Arc::new(McpServer::new(
        handler.store.clone(),
        handler.provider.clone(),
    ));

    let this = Arc::new(handler);
    Router::new()
        .route("/api/ai/settings", get(get_settings).post(update_settings))
        .route("/api/ai/skills", get(get_skills))
        .route("/api/ai/chat", post(chat))
        .route("/mcp", post(mcp_server::post_mcp).get(mcp_server::get_mcp))
        .layer(Extension(this))
        .with_state(mcp)
}
