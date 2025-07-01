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

use curvine_common::proto::ConsistencyConfig;
use serde::{Deserialize, Serialize};

#[repr(i32)]
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum ConsistencyStrategy {
    None = 0,
    Always = 1,
    Period(u64) = 2,
}

impl From<i32> for ConsistencyStrategy {
    fn from(value: i32) -> Self {
        match value {
            0 => ConsistencyStrategy::None,
            1 => ConsistencyStrategy::Always,
            2 => ConsistencyStrategy::Period(60),
            _ => ConsistencyStrategy::None,
        }
    }
}

impl From<ConsistencyStrategy> for i32 {
    fn from(value: ConsistencyStrategy) -> Self {
        match value {
            ConsistencyStrategy::None => 0,
            ConsistencyStrategy::Always => 1,
            ConsistencyStrategy::Period(_) => 2,
        }
    }
}

impl From<ConsistencyStrategy> for ConsistencyConfig {
    fn from(value: ConsistencyStrategy) -> Self {
        let mut config = ConsistencyConfig {
            strategy: value.into(),
            ..Default::default()
        };

        if let ConsistencyStrategy::Period(secs) = value {
            config.period_seconds = Some(secs);
        }

        config
    }
}

impl From<ConsistencyConfig> for ConsistencyStrategy {
    fn from(value: ConsistencyConfig) -> Self {
        let strategy = ConsistencyStrategy::from(value.strategy);

        if let ConsistencyStrategy::Period(_) = strategy {
            if let Some(secs) = value.period_seconds {
                return ConsistencyStrategy::Period(secs);
            }
        }

        strategy
    }
}
