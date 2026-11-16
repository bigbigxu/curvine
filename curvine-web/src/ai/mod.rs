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

//! CIVI — the Curvine Web AI assistant.
//!
//! This module embeds a chat agent into the web console. The LLM is accessed
//! through a server-side proxy (OpenAI-compatible endpoint) so that API keys
//! never reach the browser. The agent can only touch the cluster through the
//! whitelisted tools declared in [`tools`], served by a provider implemented
//! on top of the server's own web API state (`AiDataProvider`).

pub mod agent;
pub mod audit;
pub mod auth;
pub mod conf;
pub mod limiter;
pub mod llm;
pub mod mcp_client;
pub mod mcp_server;
pub mod provider;
pub mod router_handler;
pub mod skills;
pub mod tools;

pub use self::audit::AuditLogger;
pub use self::conf::{AiSettings, AiSettingsStore, McpServerConf};
pub use self::provider::AiDataProvider;
pub use self::router_handler::{build_router, AiRouterHandler};
pub use self::tools::redact_secrets;
