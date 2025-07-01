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

use crate::session::{FuseResponse, ResponseData};
use crate::FUSE_IN_HEADER_LEN;
use orpc::io::IOResult;
use orpc::sync::channel::AsyncSender;
use orpc::sys::pipe::{AsyncFd, Pipe2, PipeFd};
use orpc::{err_box, sys};
use std::sync::Arc;
use tokio_util::bytes::BytesMut;

// Read data from the fuse mount point.
pub struct FuseReceiver {
    kernel_fd: Arc<AsyncFd>,
    sender: AsyncSender<ResponseData>,
    pipe2: Pipe2,
    buf: BytesMut,
    fuse_len: usize,
    debug: bool,
}

impl FuseReceiver {
    pub fn new(
        kernel_fd: Arc<AsyncFd>,
        sender: AsyncSender<ResponseData>,
        buf_size: usize,
        debug: bool,
    ) -> IOResult<Self> {
        let pipe2 = Pipe2::new(PipeFd::new(buf_size, false, false)?)?;
        let buf = BytesMut::zeroed(buf_size);

        let client = Self {
            kernel_fd,
            sender,
            pipe2,
            buf,
            fuse_len: buf_size,
            debug,
        };

        Ok(client)
    }

    // Read a data from fuse.
    pub async fn receive(&mut self) -> IOResult<BytesMut> {
        self.splice().await
    }

    pub fn new_reply(&self, unique: u64) -> FuseResponse {
        FuseResponse::new(unique, self.sender.clone(), self.debug)
    }

    // Use libc::read to read data, test it, and there are multiple memory copies.
    pub async fn read(&mut self) -> IOResult<BytesMut> {
        let len = self
            .kernel_fd
            .async_read(|fd| sys::read(fd.fd(), &mut self.buf))
            .await
            .unwrap();
        Ok(BytesMut::from(&self.buf[..len as usize]))
    }

    pub async fn splice(&mut self) -> IOResult<BytesMut> {
        let write_len = self
            .pipe2
            .write_io(&self.kernel_fd, None, self.fuse_len)
            .await
            .unwrap();

        self.buf.reserve(write_len);
        unsafe {
            self.buf.set_len(write_len);
        }

        let read_len = self.pipe2.read_buf(&mut self.buf[..write_len]).await?;

        if write_len != read_len {
            return err_box!(
                "Splice read and write lengths are inconsistent, write len {}, read len {}",
                write_len,
                read_len
            );
        }
        if read_len < FUSE_IN_HEADER_LEN {
            return err_box!("short read on fuse device");
        };

        let req_buf = self.buf.split_to(read_len);
        Ok(req_buf)
    }
}
