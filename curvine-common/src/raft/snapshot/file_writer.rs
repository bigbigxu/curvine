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

use crate::raft::{RaftResult, RaftUtils};
use flate2::read::ZlibDecoder;
use orpc::common::Utils;
use orpc::io::LocalFile;
use prost::bytes::BytesMut;

// Decompress the compressed data block and write it to the file.
pub struct FileWriter {
    inner: LocalFile,
    buf: BytesMut,
    chunk_size: usize,
    checksum: u64,
}

impl FileWriter {
    pub fn from_file<T: AsRef<str>>(file: T, chunk_size: usize) -> RaftResult<Self> {
        let inner = LocalFile::with_write(file, false)?;

        let writer = Self {
            inner,
            buf: BytesMut::with_capacity(chunk_size),
            chunk_size,
            checksum: 0,
        };
        Ok(writer)
    }

    pub fn write_chunk(&mut self, chunk: &[u8]) -> RaftResult<()> {
        self.buf.reserve(self.chunk_size);
        unsafe {
            self.buf.set_len(self.chunk_size);
        }

        // Decompress the data.
        let mut decoder = ZlibDecoder::new(chunk);
        let read_len = RaftUtils::zlib_read_full(&mut decoder, &mut self.buf)?;
        let decompress_data = self.buf.split_to(read_len);

        // Write data to file
        self.inner.write_all(&decompress_data[..])?;
        self.checksum += Utils::crc32(&decompress_data[..]) as u64;

        Ok(())
    }

    pub fn checksum(&self) -> u64 {
        self.checksum
    }

    pub fn write_len(&self) -> u64 {
        self.inner.pos() as u64
    }
}
