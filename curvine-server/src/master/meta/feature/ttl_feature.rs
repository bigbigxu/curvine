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

use crate::master::meta::feature::Feature;
use curvine_common::state::TtlAction;
use serde::{Deserialize, Serialize};

// File and directory expiration time function
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TtlFeature {
    ttl_ms: i64,
    ttl_action: TtlAction,
}

impl TtlFeature {
    pub fn new(ttl_ms: i64, ttl_action: TtlAction) -> Self {
        Self { ttl_ms, ttl_action }
    }
}

impl Feature for TtlFeature {
    fn name(&self) -> &str {
        "ttl"
    }
}

impl Default for TtlFeature {
    fn default() -> Self {
        Self::new(0, TtlAction::None)
    }
}
