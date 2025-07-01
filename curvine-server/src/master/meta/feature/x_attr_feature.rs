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
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

// File or directory custom properties.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct XAttrFeature {
    pub(crate) attrs: HashMap<String, Vec<u8>>,
}

impl XAttrFeature {
    pub fn new(attrs: HashMap<String, Vec<u8>>) -> Self {
        Self { attrs }
    }

    pub fn add<T: AsRef<str>>(&mut self, key: T, value: &[u8]) {
        self.attrs
            .insert(key.as_ref().to_string(), Vec::from(value));
    }

    pub fn remove<T: AsRef<str>>(&mut self, key: T) -> Option<Vec<u8>> {
        self.attrs.remove(key.as_ref())
    }

    pub fn attrs(&self) -> &HashMap<String, Vec<u8>> {
        &self.attrs
    }
}

impl Feature for XAttrFeature {
    fn name(&self) -> &str {
        "x_attr"
    }
}

impl Default for XAttrFeature {
    fn default() -> Self {
        Self::new(HashMap::new())
    }
}
