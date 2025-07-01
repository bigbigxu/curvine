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

use chrono::{Local, Utc};
use std::time::{SystemTime, UNIX_EPOCH};
use tracing_subscriber::fmt::format::Writer;
use tracing_subscriber::fmt::time::FormatTime;

const DEFAULT_FORMAT: &str = "%Y-%m-%d %H:%M:%S%.3f";
const LOG_FORMAT: &str = "%y/%m/%d %H:%M:%S%.3f";

pub struct LocalTime {
    timezone: Local,
}

impl LocalTime {
    pub const NANOSECONDS_PER_MILLISECOND: u128 = 1000000;

    pub fn new() -> Self {
        Self { timezone: Local }
    }

    pub fn nanos() -> u128 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    }

    pub fn mills() -> u64 {
        (Self::nanos() / Self::NANOSECONDS_PER_MILLISECOND) as u64
    }

    pub fn now_datetime() -> String {
        Local::now().format(DEFAULT_FORMAT).to_string()
    }
}

impl Default for LocalTime {
    fn default() -> Self {
        Self::new()
    }
}

impl FormatTime for LocalTime {
    fn format_time(&self, w: &mut Writer<'_>) -> std::fmt::Result {
        let time = Utc::now().with_timezone(&self.timezone);
        write!(w, "{}", time.format(LOG_FORMAT))
    }
}
