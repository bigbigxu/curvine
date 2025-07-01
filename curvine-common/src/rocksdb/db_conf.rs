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

use crate::rocksdb::DEFAULT_FAMILY;
use orpc::common::{ByteUnit, FileUtils, Utils};
use rocksdb::*;

#[derive(Clone, Debug)]
pub struct DBConf {
    pub base_dir: String,
    pub data_dir: String,
    pub checkpoint_dir: String,
    pub family_list: Vec<String>,

    // Whether to disable wal, default to false, enable wal.
    pub disable_wal: bool,

    // Data compression type, value type: none, Lz4. Currently, only lz4 dependencies are compiled, and all compressed formats only support lz4
    // Default: none
    pub compression_type: DBCompressionType,

    // The amount of data to accumulate in the memory tables of all column families before writing to disk.
    // Default: 0
    pub db_write_buffer_size: usize,

    // Column family buffer size
    // Default 64m
    pub write_buffer_size: usize,
}

impl DBConf {
    pub const DATA_DIR: &'static str = "data";

    pub const CHECKPOINT_DIR: &'static str = "checkpoint";

    pub const COMPRESSION_NONE: &'static str = "none";
    pub const COMPRESSION_LZ4: &'static str = "lz4";

    pub fn new<T: AsRef<str>>(dir: T) -> Self {
        Self {
            base_dir: dir.as_ref().to_string(),
            data_dir: FileUtils::join_path(dir.as_ref(), Self::DATA_DIR),
            checkpoint_dir: FileUtils::join_path(dir.as_ref(), Self::CHECKPOINT_DIR),
            family_list: vec![DEFAULT_FAMILY.to_string()],
            disable_wal: false, //Wal is disabled by default
            compression_type: DBCompressionType::None,
            db_write_buffer_size: 0,
            write_buffer_size: 64 * 1024 * 1024,
        }
    }

    // Add column family.
    pub fn add_cf<T: AsRef<str>>(mut self, cf: T) -> Self {
        self.family_list.push(cf.as_ref().to_string());
        self
    }

    pub fn set_disable_wal(mut self, disable_wal: bool) -> Self {
        self.disable_wal = disable_wal;
        self
    }

    pub fn set_db_write_buffer_size(mut self, size: impl AsRef<str>) -> Self {
        self.db_write_buffer_size = ByteUnit::from_str(size).unwrap().as_byte() as usize;
        self
    }

    pub fn set_write_buffer_size(mut self, size: impl AsRef<str>) -> Self {
        self.write_buffer_size = ByteUnit::from_str(size).unwrap().as_byte() as usize;
        self
    }

    pub fn set_compress_type(mut self, t: impl AsRef<str>) -> Self {
        self.compression_type = match t.as_ref() {
            Self::COMPRESSION_LZ4 => DBCompressionType::Lz4,
            Self::COMPRESSION_NONE => DBCompressionType::None,
            s => panic!("Unsupported compression type：{}", s),
        };
        self
    }

    // Build a rocksdb database configuration.
    pub fn create_db_opt(&self) -> Options {
        let mut opt = Options::default();
        opt.set_allow_concurrent_memtable_write(false);
        opt.create_if_missing(true);
        opt.create_missing_column_families(true);
        opt.set_max_open_files(-1);
        // opt.set_memtable_factory(MemtableFactory::Vector);

        opt.set_compression_type(self.compression_type);
        if self.db_write_buffer_size > 0 {
            opt.set_db_write_buffer_size(self.db_write_buffer_size);
        }
        if self.write_buffer_size > 0 {
            opt.set_write_buffer_size(self.write_buffer_size);
        }

        opt
    }

    // Read configuration.
    pub fn create_read_opt(&self) -> ReadOptions {
        ReadOptions::default()
    }

    pub fn create_iterator_opt(&self) -> ReadOptions {
        let mut opt = ReadOptions::default();
        opt.set_readahead_size(64 * 1024 * 1024);
        opt
    }

    // Write configuration
    pub fn create_write_opt(&self) -> WriteOptions {
        let mut opt = WriteOptions::default();
        opt.disable_wal(self.disable_wal);
        opt
    }

    // Get the column family you want to create.
    pub fn create_cf_opt(&self) -> Vec<(String, Options)> {
        let opt = self.create_db_opt();

        let mut cfs = Vec::new();
        cfs.push((DEFAULT_FAMILY.to_string(), opt.clone()));

        for family_name in &self.family_list {
            cfs.push((family_name.to_string(), opt.clone()));
        }
        cfs
    }
}

impl Default for DBConf {
    fn default() -> Self {
        let dir = Utils::test_sub_dir(format!("testing/db-{}", Utils::rand_id()));
        Self::new(dir)
    }
}
