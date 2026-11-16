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

//! AI assistant settings with server-side persistence.
//!
//! The settings file lives next to the served webui (`webui/ai.json` by
//! default) so it can be edited from the CIVI settings panel without
//! restarting the master. The API key is only ever stored server-side; the
//! GET endpoint returns it masked.

use std::fs;
use std::path::PathBuf;
use std::sync::RwLock;

use log::info;
use serde::{Deserialize, Serialize};

/// Marker returned to the browser instead of the real API key.
/// A POST carrying this marker keeps the previously stored key.
pub const API_KEY_MASK: &str = "***";

pub const DEFAULT_ENDPOINT: &str = "https://api.deepseek.com/v1/chat/completions";
pub const DEFAULT_MODEL: &str = "deepseek-chat";

/// Hard limit for one agent turn, independent of the UI setting.
pub const MAX_ITERATIONS_LIMIT: u32 = 16;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AiSettings {
    /// Master switch. The chat endpoint rejects requests while disabled.
    pub enabled: bool,
    /// OpenAI-compatible chat completions URL.
    pub endpoint: String,
    /// Stored server-side only, never returned unmasked.
    #[serde(default)]
    pub api_key: String,
    pub model: String,
    #[serde(default = "default_temperature")]
    pub temperature: f32,
    /// Max tool-call iterations for a single chat request.
    #[serde(default = "default_max_iterations")]
    pub max_iterations: u32,
    /// Whitelist of tool names the agent may use. Unknown names are ignored.
    #[serde(default = "default_enabled_tools")]
    pub enabled_tools: Vec<String>,
    /// Max chat requests per minute (0 = unlimited).
    #[serde(default = "default_rate_limit")]
    pub rate_limit_per_min: u32,
    /// Optional shared token. When set, POST endpoints (chat / settings /
    /// confirm) and the MCP server require the `X-CIVI-Token` header.
    #[serde(default)]
    pub api_token: String,
    /// External MCP servers whose tools are merged into the agent toolset.
    #[serde(default)]
    pub mcp_servers: Vec<McpServerConf>,
}

/// An external MCP server the agent can pull extra tools from (Phase 3).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct McpServerConf {
    /// Logical name; tools are exposed to the model as `mcp_{name}_{tool}`.
    pub name: String,
    /// Streamable-HTTP MCP endpoint, e.g. `http://host:port/mcp`.
    pub url: String,
}

fn default_temperature() -> f32 {
    0.3
}

fn default_max_iterations() -> u32 {
    8
}

fn default_rate_limit() -> u32 {
    20
}

/// Default whitelist: read-only tools only. Write tools must be enabled
/// explicitly (and go through the user-confirmation flow).
fn default_enabled_tools() -> Vec<String> {
    crate::ai::tools::builtin_tools()
        .into_iter()
        .filter(|t| t.perm == "readonly")
        .map(|t| t.name.to_string())
        .collect()
}

impl Default for AiSettings {
    fn default() -> Self {
        AiSettings {
            enabled: false,
            endpoint: DEFAULT_ENDPOINT.to_string(),
            api_key: String::new(),
            model: DEFAULT_MODEL.to_string(),
            temperature: default_temperature(),
            max_iterations: default_max_iterations(),
            enabled_tools: default_enabled_tools(),
            rate_limit_per_min: default_rate_limit(),
            api_token: String::new(),
            mcp_servers: Vec::new(),
        }
    }
}

impl AiSettings {
    /// Copy with secrets masked for API responses.
    pub fn masked(&self) -> Self {
        let mut copy = self.clone();
        if !copy.api_key.is_empty() {
            copy.api_key = API_KEY_MASK.to_string();
        }
        if !copy.api_token.is_empty() {
            copy.api_token = API_KEY_MASK.to_string();
        }
        copy
    }

    pub fn is_tool_enabled(&self, name: &str) -> bool {
        self.enabled_tools.iter().any(|n| n == name)
    }
}

#[derive(Clone)]
pub struct AiSettingsStore {
    path: PathBuf,
    settings: std::sync::Arc<RwLock<AiSettings>>,
}

impl AiSettingsStore {
    /// Loads from `CURVINE_AI_CONF_FILE` or `webui/ai.json` relative to the
    /// working directory (same convention as the webui static dir).
    pub fn load_default() -> Self {
        let path =
            std::env::var("CURVINE_AI_CONF_FILE").unwrap_or_else(|_| "webui/ai.json".to_string());
        Self::from_path(PathBuf::from(path))
    }

    pub fn from_path(path: PathBuf) -> Self {
        let settings = fs::read_to_string(&path)
            .ok()
            .and_then(|text| serde_json::from_str::<AiSettings>(&text).ok())
            .unwrap_or_default();
        Self {
            path,
            settings: std::sync::Arc::new(RwLock::new(settings)),
        }
    }

    pub fn get(&self) -> AiSettings {
        self.settings
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    pub fn api_key(&self) -> String {
        self.settings
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .api_key
            .clone()
    }

    pub fn api_token(&self) -> String {
        self.settings
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .api_token
            .clone()
    }

    /// Replaces the settings and persists them. `API_KEY_MASK` as the
    /// incoming api_key/api_token keeps the previously stored value.
    pub fn update(&self, mut incoming: AiSettings) -> AiSettings {
        if incoming.api_key.trim() == API_KEY_MASK {
            incoming.api_key = self.api_key();
        }
        if incoming.api_token.trim() == API_KEY_MASK {
            incoming.api_token = self.api_token();
        }
        incoming.max_iterations = incoming.max_iterations.clamp(1, MAX_ITERATIONS_LIMIT);

        *self.settings.write().unwrap_or_else(|e| e.into_inner()) = incoming.clone();
        if let Err(e) = self.persist(&incoming) {
            log::error!(
                "Failed to persist AI settings to {}: {}",
                self.path.display(),
                e
            );
        }
        incoming.masked()
    }

    fn persist(&self, settings: &AiSettings) -> std::io::Result<()> {
        if let Some(parent) = self.path.parent() {
            fs::create_dir_all(parent)?;
        }
        let text = serde_json::to_string_pretty(settings)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))?;
        fs::write(&self.path, text)?;
        info!("AI settings saved to {}", self.path.display());
        Ok(())
    }
}
