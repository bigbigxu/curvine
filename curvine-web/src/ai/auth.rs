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

//! Shared-token gate for CIVI POST endpoints and the MCP server (Phase 3).
//!
//! The console web service gates `/api/ai/*` with its session cookie (see
//! `require_ai_session_auth`); the master's metrics server has no login
//! system, so deployments that expose it can set `api_token` in the AI
//! settings. When set, all mutating endpoints require the `X-CIVI-Token`
//! header to match.

use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde_json::json;

use crate::ai::conf::AiSettingsStore;

pub const TOKEN_HEADER: &str = "X-CIVI-Token";

/// Equality without short-circuit on the first differing byte.
pub fn constant_eq(a: &str, b: &str) -> bool {
    let (a, b) = (a.as_bytes(), b.as_bytes());
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

pub fn check_token(store: &AiSettingsStore, headers: &HeaderMap) -> Result<(), Box<Response>> {
    let token = store.api_token();
    if token.is_empty() {
        return Ok(());
    }
    let provided = headers
        .get(TOKEN_HEADER)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    if constant_eq(provided, &token) {
        Ok(())
    } else {
        Err(Box::new(
            (
                StatusCode::UNAUTHORIZED,
                Json(json!({"message": "unauthorized: missing or invalid X-CIVI-Token header"})),
            )
                .into_response(),
        ))
    }
}
