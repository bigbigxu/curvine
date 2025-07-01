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

use crate::common::{ByteUnit, TimeSpent};

pub struct SpeedCounter(TimeSpent);

impl SpeedCounter {
    pub fn new() -> Self {
        SpeedCounter(TimeSpent::new())
    }

    pub fn to_string(&self, mark: &str, bytes: u64) -> String {
        let cost = self.0.used_ms() as f64 / 1000.0;
        let size_string = ByteUnit::byte_to_string(bytes);
        let speed = ByteUnit::byte_to_string((bytes as f64 / cost) as u64);
        let band_width = ByteUnit::byte_to_string((bytes as f64 / cost) as u64 * 8);

        format!(
            "{} size: {}, cost: {:.2} s, speed: {}/s, bandwidth: {}/s",
            mark, size_string, cost, speed, band_width
        )
    }

    pub fn reset(&mut self) {
        self.0.reset();
    }

    pub fn print(&self, mark: &str, bytes: u64) {
        println!("{}", self.to_string(mark, bytes))
    }

    pub fn log(&self, mark: &str, bytes: u64) {
        log::info!("{}", self.to_string(mark, bytes))
    }
}

impl Default for SpeedCounter {
    fn default() -> Self {
        Self::new()
    }
}
