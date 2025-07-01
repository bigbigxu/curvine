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

use orpc::common::DurationUnit;
use serde::{Deserialize, Deserializer, Serializer};
use serde_with::{DeserializeAs, SerializeAs};
use std::time::Duration;

pub struct DurationString;

impl DurationString {
    fn from_str<'de, D>(deserializer: D) -> Result<Duration, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        let unit = DurationUnit::from_str(&s).map_err(serde::de::Error::custom)?;

        Ok(unit.as_duration())
    }
}

impl<'de> DeserializeAs<'de, Duration> for DurationString {
    fn deserialize_as<D>(deserializer: D) -> Result<Duration, D::Error>
    where
        D: Deserializer<'de>,
    {
        Self::from_str(deserializer)
    }
}

impl SerializeAs<Duration> for DurationString {
    fn serialize_as<S>(source: &Duration, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let str = DurationUnit::new(source.as_millis() as u64);
        serializer.serialize_str(&str.to_string())
    }
}

#[cfg(test)]
mod tests {
    use crate::conf::DurationString;
    use serde::{Deserialize, Serialize};
    use serde_with::serde_as;
    use std::time::Duration;

    #[test]
    fn test() {
        #[serde_as]
        #[derive(Debug, Serialize, Deserialize)]
        struct Config {
            #[serde_as(as = "DurationString")]
            dur: Duration,
        }

        let json = r#"{"dur": "1m"}"#;
        let config: Config = serde_json::from_str(json).unwrap();
        println!("Parsed: {:?}", config);

        // Serialization: usize → string
        let json = serde_json::to_string(&config).unwrap();
        println!("Serialized: {}", json);
    }
}
