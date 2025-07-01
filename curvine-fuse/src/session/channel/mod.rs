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

mod fuse_receiver;

pub use self::fuse_receiver::FuseReceiver;
use crate::FuseUtils;
use curvine_common::conf::FuseConf;
use orpc::io::IOResult;
use orpc::sync::channel::AsyncChannel;
use orpc::sys::pipe::AsyncFd;
use std::sync::Arc;

mod fuse_sender;
pub use self::fuse_sender::FuseSender;

pub struct FuseChannel {
    pub sender: FuseSender,
    pub receiver: FuseReceiver,
}

impl FuseChannel {
    pub fn new(fd_pair: (Arc<AsyncFd>, Arc<AsyncFd>), conf: &FuseConf) -> IOResult<Self> {
        let buf_size = FuseUtils::get_fuse_buf_size();
        let (tx, rx) = AsyncChannel::new(conf.fuse_channel_size).split();
        let sender = FuseSender::new(fd_pair.0, rx, buf_size)?;
        let receiver = FuseReceiver::new(fd_pair.1, tx, buf_size, conf.debug)?;

        Ok(Self { sender, receiver })
    }
}
