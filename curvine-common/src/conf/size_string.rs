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

use orpc::common::ByteUnit;
use serde::{Deserialize, Deserializer, Serializer};
use serde_with::{DeserializeAs, SerializeAs};

pub struct SizeString;

impl SizeString {
    fn from_str<'de, D>(deserializer: D) -> Result<u64, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        let unit = ByteUnit::from_str(s).map_err(serde::de::Error::custom)?;

        Ok(unit.as_byte())
    }
}

impl<'de> DeserializeAs<'de, usize> for SizeString {
    fn deserialize_as<D>(deserializer: D) -> Result<usize, D::Error>
    where
        D: Deserializer<'de>,
    {
        Ok(Self::from_str(deserializer)? as usize)
    }
}

impl SerializeAs<i64> for SizeString {
    fn serialize_as<S>(source: &i64, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let str = ByteUnit::byte_to_string(*source as u64);
        serializer.serialize_str(&str)
    }
}

impl<'de> DeserializeAs<'de, i64> for SizeString {
    fn deserialize_as<D>(deserializer: D) -> Result<i64, D::Error>
    where
        D: Deserializer<'de>,
    {
        Ok(Self::from_str(deserializer)? as i64)
    }
}

impl SerializeAs<u64> for SizeString {
    fn serialize_as<S>(source: &u64, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let str = ByteUnit::byte_to_string(*source);
        serializer.serialize_str(&str)
    }
}

impl<'de> DeserializeAs<'de, u64> for SizeString {
    fn deserialize_as<D>(deserializer: D) -> Result<u64, D::Error>
    where
        D: Deserializer<'de>,
    {
        Self::from_str(deserializer)
    }
}

#[cfg(test)]
mod tests {
    use crate::conf::SizeString;
    use serde::{Deserialize, Serialize};
    use serde_with::serde_as;

    #[test]
    fn test() {
        #[serde_as]
        #[derive(Debug, Serialize, Deserialize)]
        struct Config {
            #[serde_as(as = "SizeString")]
            size: u64,
        }

        let json = r#"{"size": "64KB"}"#;
        let config: Config = serde_json::from_str(json).unwrap();
        println!("Parsed: {:?}", config);

        // Serialization: usize → string
        let json = serde_json::to_string(&config).unwrap();
        println!("Serialized: {}", json);
    }
}
