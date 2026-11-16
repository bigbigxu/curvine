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

//! Simple sliding-window rate limiter for the CIVI chat endpoint (Phase 2).
//!
//! There is no web auth yet, so the limiter is global rather than per-user;
//! it protects the master (and the LLM bill) from runaway clients.

use std::collections::VecDeque;
use std::sync::Mutex;
use std::time::{Duration, Instant};

const WINDOW: Duration = Duration::from_secs(60);

#[derive(Clone)]
pub struct RateLimiter {
    hits: std::sync::Arc<Mutex<VecDeque<Instant>>>,
}

impl RateLimiter {
    pub fn new() -> Self {
        RateLimiter {
            hits: std::sync::Arc::new(Mutex::new(VecDeque::new())),
        }
    }

    /// Returns `true` if the request is allowed under `limit` hits per
    /// minute. `limit == 0` disables limiting.
    pub fn allow(&self, limit: u32) -> bool {
        if limit == 0 {
            return true;
        }
        let mut hits = self.hits.lock().unwrap_or_else(|e| e.into_inner());
        let now = Instant::now();
        while let Some(front) = hits.front() {
            if now.duration_since(*front) > WINDOW {
                hits.pop_front();
            } else {
                break;
            }
        }
        if hits.len() >= limit as usize {
            return false;
        }
        hits.push_back(now);
        true
    }

    /// Milliseconds until the next request would be allowed (best effort).
    pub fn retry_after_ms(&self, _limit: u32) -> u64 {
        let hits = self.hits.lock().unwrap_or_else(|e| e.into_inner());
        match hits.front() {
            Some(front) => {
                let elapsed = front.elapsed();
                WINDOW.saturating_sub(elapsed).as_millis() as u64
            }
            None => 0,
        }
    }
}

impl Default for RateLimiter {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rate_limiter_window() {
        let limiter = RateLimiter::new();
        assert!(limiter.allow(2));
        assert!(limiter.allow(2));
        assert!(!limiter.allow(2));
        assert!(limiter.retry_after_ms(2) <= 60_000);
        // Unlimited mode.
        assert!(limiter.allow(0));
    }
}
