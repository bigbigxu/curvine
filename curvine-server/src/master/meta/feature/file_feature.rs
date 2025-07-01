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

use crate::master::meta::feature::{WriteFeature, XAttrFeature};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::mem;

// File extension function.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FileFeature {
    pub(crate) x_attrs: XAttrFeature,
    pub(crate) file_write: Option<WriteFeature>,
}

impl FileFeature {
    pub fn new() -> Self {
        Self {
            x_attrs: XAttrFeature::default(),
            file_write: None,
        }
    }

    pub fn add_attr<K: AsRef<str>, V: AsRef<[u8]>>(&mut self, key: K, value: V) {
        self.x_attrs
            .attrs
            .insert(key.as_ref().to_string(), Vec::from(value.as_ref()));
    }

    pub fn remove_attr<K: AsRef<str>>(&mut self, key: K) {
        self.x_attrs.attrs.remove(key.as_ref());
    }

    pub fn set_attrs(&mut self, map: HashMap<String, Vec<u8>>) {
        self.x_attrs.attrs = map
    }

    pub fn set_writing(&mut self, client_name: String) {
        let _ = mem::replace(&mut self.file_write, Some(WriteFeature::new(client_name)));
    }

    pub fn set_finalized(&mut self) {
        let _ = self.file_write.take();
    }

    pub fn all_attrs(&self) -> &HashMap<String, Vec<u8>> {
        &self.x_attrs.attrs
    }
}

impl Default for FileFeature {
    fn default() -> Self {
        Self::new()
    }
}
