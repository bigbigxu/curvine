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

//! Minimal OpenAI-compatible chat client with SSE streaming.
//!
//! Works with any `/chat/completions` endpoint (DeepSeek, vLLM, Ollama, ...).
//! Tool-call deltas are passed through un-aggregated; the agent loop in
//! [`crate::ai::agent`] accumulates fragments and executes tools.

use std::pin::Pin;

use futures::{Stream, TryStreamExt};
use serde_json::{json, Value};

use crate::ai::conf::AiSettings;

pub type LlmStream = Pin<Box<dyn Stream<Item = Result<LlmChunk, String>> + Send>>;

/// One streaming chunk from an OpenAI-compatible `chat/completions` SSE.
#[derive(Debug, Default)]
pub struct LlmChunk {
    pub content: Option<String>,
    /// Incremental fragments of tool calls, keyed by `index` at the caller.
    pub tool_calls: Vec<ToolCallFragment>,
    pub finish_reason: Option<String>,
}

#[derive(Debug)]
pub struct ToolCallFragment {
    pub index: usize,
    pub id: Option<String>,
    pub name: Option<String>,
    pub arguments: Option<String>,
}

#[derive(Clone)]
pub struct LlmClient {
    endpoint: String,
    api_key: String,
    model: String,
    temperature: f32,
}

impl LlmClient {
    pub fn from_settings(settings: &AiSettings) -> Self {
        LlmClient {
            endpoint: settings.endpoint.trim().to_string(),
            api_key: settings.api_key.clone(),
            model: settings.model.clone(),
            temperature: settings.temperature,
        }
    }

    /// Starts a streaming chat completion. Returns a stream of chunks;
    /// network/HTTP failures are surfaced as the outer error or as `Err`
    /// items mid-stream.
    pub async fn chat_stream(
        &self,
        messages: &[Value],
        tools: &[Value],
    ) -> Result<LlmStream, String> {
        if self.endpoint.is_empty() {
            return Err("AI endpoint is not configured".to_string());
        }
        if self.model.is_empty() {
            return Err("AI model is not configured".to_string());
        }

        let mut body = json!({
            "model": self.model,
            "messages": messages,
            "stream": true,
        });
        if !tools.is_empty() {
            body["tools"] = json!(tools);
            body["tool_choice"] = json!("auto");
        }
        body["temperature"] = json!(self.temperature);

        let client = reqwest::Client::builder()
            .connect_timeout(std::time::Duration::from_secs(10))
            .build()
            .map_err(|e| e.to_string())?;
        let mut req = client.post(&self.endpoint).json(&body);
        if !self.api_key.is_empty() {
            req = req.bearer_auth(&self.api_key);
        }

        let resp = req.send().await.map_err(|e| e.to_string())?;
        let status = resp.status();
        if !status.is_success() {
            let text = resp.text().await.unwrap_or_default();
            return Err(format!(
                "LLM endpoint returned {}: {}",
                status,
                truncate_text(&text, 500)
            ));
        }

        Ok(sse_stream(resp.bytes_stream().map_err(|e| e.to_string())))
    }
}

/// Parses an OpenAI-compatible SSE byte stream into `LlmChunk`s.
fn sse_stream<S, B>(mut bytes: S) -> LlmStream
where
    S: Stream<Item = Result<B, String>> + Send + Unpin + 'static,
    B: AsRef<[u8]> + Send,
{
    let inner = async_stream::stream! {
        let mut buffer = String::new();
        loop {
            let chunk = match bytes.try_next().await {
                Ok(Some(c)) => c,
                Ok(None) => break,
                Err(e) => {
                    yield Err(e);
                    return;
                }
            };
            buffer.push_str(&String::from_utf8_lossy(chunk.as_ref()));

            // SSE events are separated by a blank line.
            while let Some(pos) = buffer.find("\n\n") {
                let event: String = buffer.drain(..pos + 2).collect();
                for line in event.lines() {
                    let data = match line.strip_prefix("data:") {
                        Some(d) => d.trim(),
                        None => continue,
                    };
                    if data == "[DONE]" {
                        return;
                    }
                    let parsed: Value = match serde_json::from_str(data) {
                        Ok(v) => v,
                        Err(_) => continue, // keep-alive comments or non-JSON lines
                    };
                    let choice = &parsed["choices"][0];
                    let delta = &choice["delta"];

                    let mut tool_calls = Vec::new();
                    if let Some(arr) = delta["tool_calls"].as_array() {
                        for tc in arr {
                            tool_calls.push(ToolCallFragment {
                                index: tc["index"].as_u64().unwrap_or(0) as usize,
                                id: tc["id"].as_str().map(str::to_string),
                                name: tc["function"]["name"].as_str().map(str::to_string),
                                arguments: tc["function"]["arguments"].as_str().map(str::to_string),
                            });
                        }
                    }

                    yield Ok(LlmChunk {
                        content: delta["content"].as_str().map(str::to_string),
                        tool_calls,
                        finish_reason: choice["finish_reason"].as_str().map(str::to_string),
                    });
                }
            }
        }
    };
    Box::pin(inner)
}

fn truncate_text(text: &str, max: usize) -> String {
    if text.len() <= max {
        return text.to_string();
    }
    let mut cut = max;
    while !text.is_char_boundary(cut) {
        cut -= 1;
    }
    format!("{}...", &text[..cut])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_rejects_unconfigured() {
        let settings = AiSettings {
            endpoint: String::new(),
            ..AiSettings::default()
        };
        let client = LlmClient::from_settings(&settings);
        let err = match client
            .chat_stream(&[json!({"role": "user", "content": "hi"})], &[])
            .await
        {
            Err(e) => e,
            Ok(_) => panic!("expected error for unconfigured endpoint"),
        };
        assert!(err.contains("endpoint"));
    }

    #[tokio::test]
    async fn test_sse_parse() {
        let raw = b"data: {\"choices\":[{\"delta\":{\"content\":\"he\"}}]}\n\n\
                    data: {\"choices\":[{\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"c1\",\"function\":{\"name\":\"list_dir\",\"arguments\":\"{\\\"pa\"}}]}}]}\n\n\
                    data: {\"choices\":[{\"delta\":{\"tool_calls\":[{\"index\":0,\"function\":{\"arguments\":\"th\\\": \\\"/\\\"}\"}}]}}]}\n\n\
                    data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"tool_calls\"}]}\n\n\
                    data: [DONE]\n\n";
        let raw: Vec<u8> = raw.to_vec();
        let byte_stream = futures::stream::iter(vec![Ok::<Vec<u8>, String>(raw)]);
        let mut s = sse_stream(byte_stream);

        let mut chunks = vec![];
        while let Some(item) = futures::StreamExt::next(&mut s).await {
            chunks.push(item.unwrap());
        }
        assert_eq!(chunks.len(), 4);
        assert_eq!(chunks[0].content.as_deref(), Some("he"));
        assert_eq!(chunks[1].tool_calls.len(), 1);
        assert_eq!(chunks[1].tool_calls[0].arguments.as_deref(), Some("{\"pa"));
        assert_eq!(
            chunks[2].tool_calls[0].arguments.as_deref(),
            Some("th\": \"/\"}")
        );
        assert_eq!(chunks[3].finish_reason.as_deref(), Some("tool_calls"));
    }
}
