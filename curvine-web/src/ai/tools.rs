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

//! Tool registry for the CIVI agent.
//!
//! Every capability the model may reach is declared here with a JSON-schema
//! style parameter spec. Dispatch is the single choke point: it enforces the
//! whitelist, validates arguments and truncates oversized results. Write
//! tools (perm = "write") additionally go through the user-confirmation
//! flow before executing (see design doc §4).

use serde_json::{json, Value};

use crate::ai::conf::AiSettings;
use crate::ai::provider::AiDataProvider;

/// Serialized tool results are truncated to this size before being fed back
/// into the model context.
const MAX_RESULT_BYTES: usize = 48 * 1024;

/// Substrings (lower-cased) whose presence in a config key marks the value
/// as secret for redaction.
const SECRET_KEY_PATTERNS: &[&str] = &[
    "password",
    "passwd",
    "secret",
    "token",
    "api_key",
    "apikey",
    "credential",
    "private_key",
    "access_key",
];

#[derive(Debug, Clone)]
pub struct ToolSpec {
    pub name: &'static str,
    pub description: &'static str,
    pub parameters: Value,
    /// Permission tier, surfaced in the settings UI.
    pub perm: &'static str,
}

/// Read-only tools backed by `AiDataProvider`.
pub fn builtin_tools() -> Vec<ToolSpec> {
    vec![
        ToolSpec {
            name: "cluster_overview",
            description:
                "Get the Curvine cluster overview: total capacity, available space, \
                          fs_used bytes, file/dir counts, live/lost worker counts and master state.",
            parameters: json!({"type": "object", "properties": {}}),
            perm: "readonly",
        },
        ToolSpec {
            name: "list_dir",
            description: "List the entries of a directory in the Curvine filesystem. \
                          `path` must be an absolute path starting with '/'.",
            parameters: json!({
                "type": "object",
                "properties": {
                    "path": {
                        "type": "string",
                        "description": "Absolute directory path, e.g. '/data'."
                    }
                },
                "required": ["path"]
            }),
            perm: "readonly",
        },
        ToolSpec {
            name: "worker_status",
            description: "Get the list of live and lost storage workers with their capacity \
                          and usage. Useful for capacity and health analysis.",
            parameters: json!({"type": "object", "properties": {}}),
            perm: "readonly",
        },
        ToolSpec {
            name: "session_summary",
            description: "Get the summary of active client sessions (host, mount point, lease \
                          state). Useful for diagnosing zombie mounts or stale clients.",
            parameters: json!({"type": "object", "properties": {}}),
            perm: "readonly",
        },
        ToolSpec {
            name: "cluster_config",
            description: "Get the Curvine cluster configuration with all secret fields \
                          (passwords, tokens, keys) redacted. Useful to answer \
                          configuration questions.",
            parameters: json!({"type": "object", "properties": {}}),
            perm: "readonly",
        },
        ToolSpec {
            name: "add_dcm",
            description: "Mark storage workers as decommissioned (DCM). Data on the listed \
                          workers will be migrated away. Requires explicit user confirmation \
                          in the console before execution.",
            parameters: json!({
                "type": "object",
                "properties": {
                    "workers": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Worker addresses to mark as decommissioned, e.g. \"host:port\"."
                    }
                },
                "required": ["workers"]
            }),
            perm: "write",
        },
    ]
}

/// Recursively replaces values of secret-looking keys with `***`.
pub fn redact_secrets(value: &Value) -> Value {
    match value {
        Value::Object(map) => Value::Object(
            map.iter()
                .map(|(k, v)| {
                    let key = k.to_lowercase();
                    if SECRET_KEY_PATTERNS.iter().any(|p| key.contains(p)) && !v.is_null() {
                        (k.clone(), Value::String("***".to_string()))
                    } else {
                        (k.clone(), redact_secrets(v))
                    }
                })
                .collect(),
        ),
        Value::Array(items) => Value::Array(items.iter().map(redact_secrets).collect()),
        other => other.clone(),
    }
}

pub fn find_spec(name: &str) -> Option<ToolSpec> {
    builtin_tools().into_iter().find(|t| t.name == name)
}

/// Tool specs in OpenAI `tools` format, filtered by the settings whitelist.
pub fn openai_specs(settings: &AiSettings) -> Vec<Value> {
    builtin_tools()
        .into_iter()
        .filter(|t| settings.enabled_tools.iter().any(|n| n == t.name))
        .map(|t| {
            json!({
                "type": "function",
                "function": {
                    "name": t.name,
                    "description": t.description,
                    "parameters": t.parameters
                }
            })
        })
        .collect()
}

/// Single entry point for executing a tool call issued by the model.
/// Never trust the model: the whitelist is enforced again here even though
/// disabled tools were never announced to it.
///
/// `cluster` carries the console's selected cluster through to the provider;
/// single-cluster servers ignore it.
pub async fn dispatch(
    provider: &dyn AiDataProvider,
    settings: &AiSettings,
    name: &str,
    args: &Value,
    cluster: Option<&str>,
) -> Result<Value, String> {
    if !settings.enabled_tools.iter().any(|n| n == name) {
        return Err(format!(
            "tool '{}' is not enabled for the AI assistant",
            name
        ));
    }

    let value = match name {
        "cluster_overview" => provider.cluster_overview(cluster).await,
        "list_dir" => {
            let path = args
                .get("path")
                .and_then(Value::as_str)
                .ok_or_else(|| "missing required parameter: path".to_string())?;
            let path = validate_path(path)?;
            provider.list_dir(cluster, &path).await
        }
        "worker_status" => provider.worker_status(cluster).await,
        "session_summary" => provider.session_summary(cluster).await,
        "cluster_config" => provider.cluster_config(cluster).await,
        "add_dcm" => {
            let workers = parse_worker_list(args)?;
            if workers.is_empty() {
                return Err("'workers' must contain at least one address".to_string());
            }
            provider.add_dcm(cluster, &workers).await
        }
        other => return Err(format!("unknown tool '{}'", other)),
    }?;

    Ok(truncate(value))
}

fn parse_worker_list(args: &Value) -> Result<Vec<String>, String> {
    let list = args
        .get("workers")
        .and_then(Value::as_array)
        .ok_or_else(|| "missing required parameter: workers (array of strings)".to_string())?;
    let mut out = Vec::new();
    for item in list {
        let s = item
            .as_str()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .ok_or_else(|| "'workers' items must be non-empty strings".to_string())?;
        out.push(s.to_string());
    }
    Ok(out)
}

/// Paths coming from the model are user-controllable data: enforce absolute
/// paths and reject traversal segments.
fn validate_path(path: &str) -> Result<String, String> {
    let path = path.trim();
    if !path.starts_with('/') {
        return Err(format!("path '{}' must be absolute (start with '/')", path));
    }
    if path.contains('\0') || path.contains('\\') {
        return Err(format!("path '{}' contains invalid characters", path));
    }
    if path.split('/').any(|seg| seg == "..") {
        return Err(format!("path '{}' must not contain '..' segments", path));
    }
    Ok(path.to_string())
}

fn truncate(value: Value) -> Value {
    let text = value.to_string();
    if text.len() <= MAX_RESULT_BYTES {
        return value;
    }
    let mut cut = MAX_RESULT_BYTES;
    while !text.is_char_boundary(cut) {
        cut -= 1;
    }
    json!({
        "truncated": true,
        "note": format!(
            "Result exceeded {} bytes and was truncated. Narrow your query (e.g. list a \
             sub-directory) instead of expecting the full payload.",
            MAX_RESULT_BYTES
        ),
        "data_prefix": &text[..cut]
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::sync::Mutex;

    struct FakeProvider {
        calls: Mutex<Vec<String>>,
    }

    impl FakeProvider {
        fn new() -> Self {
            FakeProvider {
                calls: Mutex::new(vec![]),
            }
        }
    }

    #[async_trait::async_trait]
    impl AiDataProvider for FakeProvider {
        async fn cluster_overview(&self, _cluster: Option<&str>) -> Result<Value, String> {
            self.calls.lock().unwrap().push("overview".into());
            Ok(json!({"capacity": 100}))
        }

        async fn list_dir(&self, _cluster: Option<&str>, path: &str) -> Result<Value, String> {
            self.calls.lock().unwrap().push(format!("ls:{}", path));
            Ok(json!([{"name": "a", "path": path.to_string()}]))
        }

        async fn worker_status(&self, _cluster: Option<&str>) -> Result<Value, String> {
            self.calls.lock().unwrap().push("workers".into());
            Ok(json!({"live_workers": []}))
        }

        async fn session_summary(&self, _cluster: Option<&str>) -> Result<Value, String> {
            self.calls.lock().unwrap().push("sessions".into());
            Ok(json!([]))
        }

        async fn cluster_config(&self, _cluster: Option<&str>) -> Result<Value, String> {
            self.calls.lock().unwrap().push("config".into());
            Ok(json!({"cluster_id": "test"}))
        }

        async fn add_dcm(
            &self,
            _cluster: Option<&str>,
            workers: &[String],
        ) -> Result<Value, String> {
            self.calls
                .lock()
                .unwrap()
                .push(format!("add_dcm:{:?}", workers));
            Ok(json!(workers))
        }
    }

    fn all_enabled_settings() -> AiSettings {
        AiSettings {
            enabled_tools: builtin_tools().iter().map(|t| t.name.to_string()).collect(),
            ..AiSettings::default()
        }
    }

    fn default_settings() -> AiSettings {
        AiSettings::default()
    }

    #[tokio::test]
    async fn test_dispatch_routes_and_validates() {
        let provider = FakeProvider::new();
        let settings = all_enabled_settings();

        let v = dispatch(&provider, &settings, "cluster_overview", &json!({}), None)
            .await
            .unwrap();
        assert_eq!(v["capacity"], 100);

        let v = dispatch(
            &provider,
            &settings,
            "list_dir",
            &json!({"path": "/data/logs"}),
            None,
        )
        .await
        .unwrap();
        assert_eq!(v[0]["path"], "/data/logs");

        // Path validation.
        assert!(dispatch(
            &provider,
            &settings,
            "list_dir",
            &json!({"path": "relative"}),
            None
        )
        .await
        .is_err());
        assert!(dispatch(
            &provider,
            &settings,
            "list_dir",
            &json!({"path": "/a/../b"}),
            None
        )
        .await
        .is_err());
        assert!(dispatch(&provider, &settings, "list_dir", &json!({}), None)
            .await
            .is_err());

        // Unknown tool.
        assert!(dispatch(&provider, &settings, "rm_rf", &json!({}), None)
            .await
            .is_err());
    }

    #[tokio::test]
    async fn test_whitelist_enforced_at_dispatch() {
        let provider = FakeProvider::new();
        let mut settings = all_enabled_settings();
        settings.enabled_tools = vec!["cluster_overview".to_string()];

        assert!(
            dispatch(&provider, &settings, "worker_status", &json!({}), None)
                .await
                .is_err()
        );
        assert!(
            dispatch(&provider, &settings, "cluster_overview", &json!({}), None)
                .await
                .is_ok()
        );
    }

    #[test]
    fn test_specs_respect_whitelist() {
        let mut settings = all_enabled_settings();
        settings.enabled_tools = vec!["list_dir".to_string()];
        let specs = openai_specs(&settings);
        assert_eq!(specs.len(), 1);
        assert_eq!(specs[0]["function"]["name"], "list_dir");
    }

    #[test]
    fn test_truncation() {
        let big: Vec<_> = (0..100_000)
            .map(|i| json!({"i": i, "pad": "x".repeat(8)}))
            .collect();
        let out = truncate(json!(big));
        assert_eq!(out["truncated"], true);
        assert!(out["data_prefix"].as_str().unwrap().len() <= MAX_RESULT_BYTES);

        let small = truncate(json!({"ok": true}));
        assert_eq!(small["ok"], true);
    }

    #[tokio::test]
    async fn test_write_tool_off_by_default() {
        let provider = FakeProvider::new();
        let settings = default_settings();
        // Default whitelist only contains readonly tools.
        assert!(dispatch(
            &provider,
            &settings,
            "add_dcm",
            &json!({"workers": ["h:1"]}),
            None
        )
        .await
        .is_err());
        assert!(
            dispatch(&provider, &settings, "cluster_config", &json!({}), None)
                .await
                .is_ok()
        );

        // Once enabled, arg validation applies.
        let mut enabled = settings.clone();
        enabled.enabled_tools.push("add_dcm".to_string());
        assert!(dispatch(
            &provider,
            &enabled,
            "add_dcm",
            &json!({"workers": []}),
            None
        )
        .await
        .is_err());
        assert!(dispatch(&provider, &enabled, "add_dcm", &json!({}), None)
            .await
            .is_err());
        let v = dispatch(
            &provider,
            &enabled,
            "add_dcm",
            &json!({"workers": ["host:9002"]}),
            None,
        )
        .await
        .unwrap();
        assert_eq!(v[0], "host:9002");
    }

    #[test]
    fn test_redact_secrets() {
        let conf = json!({
            "cluster_id": "test",
            "master": {"username": "admin", "password": "p@ss", "port": 8995},
            "journal": {"token": "abc"},
            "s3": {"access_key": "ak", "secret_key": "sk", "region": "us"},
            "workers": [{"passwd": "x", "hostname": "h1"}]
        });
        let redacted = redact_secrets(&conf);
        assert_eq!(redacted["cluster_id"], "test");
        assert_eq!(redacted["master"]["password"], "***");
        assert_eq!(redacted["master"]["username"], "admin");
        assert_eq!(redacted["master"]["port"], 8995);
        assert_eq!(redacted["journal"]["token"], "***");
        assert_eq!(redacted["s3"]["access_key"], "***");
        assert_eq!(redacted["s3"]["secret_key"], "***");
        assert_eq!(redacted["s3"]["region"], "us");
        assert_eq!(redacted["workers"][0]["passwd"], "***");
        assert_eq!(redacted["workers"][0]["hostname"], "h1");
    }

    #[test]
    fn test_settings_mask_roundtrip() {
        let settings = AiSettings {
            api_key: "sk-secret".to_string(),
            ..AiSettings::default()
        };
        let masked = settings.masked();
        assert_eq!(masked.api_key, "***");

        // The store keeps the real key when the mask is posted back.
        let store = crate::ai::conf::AiSettingsStore::from_path(
            std::env::temp_dir().join(format!("civi-test-{}.json", std::process::id())),
        );
        store.update(settings);
        assert_eq!(store.api_key(), "sk-secret");
        let mut posted = store.get().masked();
        posted.enabled = true;
        let saved = store.update(posted);
        assert_eq!(saved.api_key, "***");
        assert!(store.get().enabled);
        assert_eq!(store.api_key(), "sk-secret");
    }
}
