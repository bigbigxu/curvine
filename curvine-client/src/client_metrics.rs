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

use orpc::common::Metrics as m;
use orpc::common::{Counter, Metrics};
use orpc::CommonResult;
use std::fmt::{Debug, Formatter};

pub struct ClientMetrics {
    pub local_cache_hits: Counter,
    pub local_cache_misses: Counter,
}

impl ClientMetrics {
    pub fn new() -> CommonResult<Self> {
        let cm = Self {
            local_cache_hits: m::new_counter("local_cache_hits", "Cache hit count")?,
            local_cache_misses: m::new_counter("local_cache_misses", "Cache miss count")?,
        };
        Ok(cm)
    }

    pub fn text_output(&self) -> CommonResult<String> {
        Metrics::text_output()
    }
}

impl Debug for ClientMetrics {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "ClientMetrics")
    }
}
