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

use crate::io::IOResult;
use crate::sys;
use crate::sys::CInt;
use std::fs::File;

const LONG_READ_THRESHOLD_LEN: i64 = 256 * 1024;

#[allow(unused)]
pub struct ReadAheadTask {
    off: i64,
    len: i64,
    handle: IOResult<CInt>,
}

/// Operating system page cache manager, currently only supports linux。
#[allow(unused)]
#[derive(Debug, Clone)]
pub struct CacheManager {
    pub read_ahead_len: i64,
    pub drop_cache_len: i64,
    pub enable: bool,
}

impl CacheManager {
    pub fn new(enable: bool, read_ahead_len: i64, drop_cache_len: i64) -> Self {
        let enable = if cfg!(target_os = "linux") {
            if read_ahead_len <= 0 {
                false
            } else {
                enable
            }
        } else {
            false
        };

        CacheManager {
            read_ahead_len,
            drop_cache_len,
            enable,
        }
    }

    pub fn with_place() -> Self {
        Self::new(false, 4 * 1024 * 1024, 1024 * 1024)
    }

    pub fn read_ahead(
        &self,
        file: &File,
        cur_pos: i64,
        total_len: i64,
        last_task: Option<ReadAheadTask>,
    ) -> Option<ReadAheadTask> {
        // The file is greater than 256kb, use the read-previous API.It is not necessary to use pre-reading of small files.
        if !self.enable || total_len < LONG_READ_THRESHOLD_LEN {
            // If read preview is not supported, no error will be returned.
            return None;
        };

        // The location of the last read.
        let last_offset = if let Some(t) = last_task.as_ref() {
            t.off
        } else {
            i64::MIN
        };

        // When cur_pos is halfway, perform reading preview.
        let next_offset = last_offset + self.read_ahead_len / 2;
        if cur_pos >= next_offset {
            let len = self.read_ahead_len.min(i64::MAX - cur_pos);
            if len <= 0 {
                None
            } else {
                let handle = sys::read_ahead(file, cur_pos, len);
                Some(ReadAheadTask {
                    off: cur_pos,
                    len,
                    handle,
                })
            }
        } else {
            last_task
        }
    }
}

impl Default for CacheManager {
    fn default() -> Self {
        Self::new(true, 4 * 1024 * 1024, 1024 * 1024)
    }
}
