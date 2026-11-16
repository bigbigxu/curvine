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

//! Durable audit log for CIVI (Phase 2).
//!
//! Every chat request and every tool execution is appended as one JSON line
//! to `webui/ai-audit.jsonl` (override with `CURVINE_AI_AUDIT_FILE`). The
//! log is the authoritative record for "what did the bot look at / change".

use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::PathBuf;
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

use serde_json::{json, Value};

const MAX_ARG_BYTES: usize = 300;

#[derive(Clone)]
pub struct AuditLogger {
    path: PathBuf,
    file: std::sync::Arc<Mutex<Option<File>>>,
}

impl AuditLogger {
    pub fn load_default() -> Self {
        let path = std::env::var("CURVINE_AI_AUDIT_FILE")
            .unwrap_or_else(|_| "webui/ai-audit.jsonl".to_string());
        Self::from_path(PathBuf::from(path))
    }

    pub fn from_path(path: PathBuf) -> Self {
        Self {
            path,
            file: std::sync::Arc::new(Mutex::new(None)),
        }
    }

    /// Appends one audit record; failures are logged but never propagate —
    /// auditing must not break the chat.
    pub fn record(&self, kind: &str, fields: Value) {
        let ts_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        let mut record = json!({
            "ts_ms": ts_ms,
            "kind": kind,
        });
        if let (Some(obj), Some(extra)) = (record.as_object_mut(), fields.as_object()) {
            for (k, v) in extra {
                obj.insert(k.clone(), v.clone());
            }
        }
        let line = record.to_string();
        if let Err(e) = self.append_line(&line) {
            log::error!(
                "[civi-audit] failed to write {}: {}",
                self.path.display(),
                e
            );
        }
    }

    pub fn record_tool_call(&self, tool: &str, args: &str, ok: bool, elapsed_ms: u128) {
        self.record(
            "tool_call",
            json!({
                "tool": tool,
                "args": truncate(args),
                "ok": ok,
                "elapsed_ms": elapsed_ms as u64,
            }),
        );
    }

    pub fn record_chat(&self, messages: usize, action: &str) {
        self.record("chat", json!({"messages": messages, "action": action}));
    }

    fn append_line(&self, line: &str) -> std::io::Result<()> {
        let mut guard = self.file.lock().unwrap_or_else(|e| e.into_inner());
        if guard.is_none() {
            if let Some(parent) = self.path.parent() {
                std::fs::create_dir_all(parent)?;
            }
            let file = OpenOptions::new()
                .create(true)
                .append(true)
                .open(&self.path)?;
            *guard = Some(file);
        }
        let file = guard.as_mut().expect("audit file must be open here");
        file.write_all(line.as_bytes())?;
        file.write_all(b"\n")?;
        Ok(())
    }
}

fn truncate(text: &str) -> String {
    if text.len() <= MAX_ARG_BYTES {
        return text.to_string();
    }
    let mut cut = MAX_ARG_BYTES;
    while !text.is_char_boundary(cut) {
        cut -= 1;
    }
    format!("{}...", &text[..cut])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_audit_records() {
        let path =
            std::env::temp_dir().join(format!("civi-audit-test-{}.jsonl", std::process::id()));
        let _ = std::fs::remove_file(&path);
        let logger = AuditLogger::from_path(path.clone());
        logger.record_chat(3, "start");
        logger.record_tool_call("list_dir", r#"{"path":"/data"}"#, true, 12);

        let text = std::fs::read_to_string(&path).unwrap();
        let lines: Vec<&str> = text.lines().collect();
        assert_eq!(lines.len(), 2);
        let first: Value = serde_json::from_str(lines[0]).unwrap();
        assert_eq!(first["kind"], "chat");
        assert_eq!(first["messages"], 3);
        let second: Value = serde_json::from_str(lines[1]).unwrap();
        assert_eq!(second["tool"], "list_dir");
        assert_eq!(second["ok"], true);
        let _ = std::fs::remove_file(&path);
    }
}
