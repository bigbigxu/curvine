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

use serde::{Deserialize, Serialize};
use std::any::Any;
use std::fmt::Debug;

mod write_feature;
pub use self::write_feature::*;

mod x_attr_feature;
pub use self::x_attr_feature::*;

mod ttl_feature;
pub use self::ttl_feature::*;

mod acl_feature;
pub use self::acl_feature::*;

mod file_feature;
pub use self::file_feature::*;

pub trait Feature: Debug + Serialize + Any + for<'a> Deserialize<'a> {
    fn name(&self) -> &str;
}

pub const FILE_WRITE_NAME: &str = "file_write";

pub const X_ATTR_NAME: &str = "x_attr";

pub const TTL_NAME: &str = "ttl";

pub const ACT_NAME: &str = "acl";
