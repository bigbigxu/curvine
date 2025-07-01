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

use bytes::BytesMut;
use clap::{arg, Parser};
use log::info;
use orpc::common::{ByteUnit, FileUtils, Logger, SpeedCounter, Utils};
use orpc::io::LocalFile;
use orpc::runtime::{AsyncRuntime, RpcRuntime, Runtime};
use orpc::sys::RawPtr;
use orpc::{err_box, CommonResult};
use std::sync::Arc;
use tokio::task::JoinHandle;

// FUSE performance testing.
// Use fixed threads to simulate concurrent read and write of multiple files.
// 40 threads.One queue per thread.
// Write: cargo run --release --bin fuse-bench -- --action write --checksum 1 --dir mnt
// Read: cargo run --release --bin fuse-bench -- --action read --checksum 1 --dir mnt
fn main() -> CommonResult<()> {
    Logger::default();

    let args: Args = Args::parse();
    let rt = Arc::new(AsyncRuntime::new(
        "fuse-bench",
        args.client_threads,
        args.client_threads,
    ));

    let speed = SpeedCounter::new();

    let res = rt.block_on(run_task(rt.clone(), args.clone()))?;
    let total_len = (args.file_num as u64) * (args.file_size() as u64);

    speed.print(args.action.as_str(), total_len);

    assert_eq!(total_len, res.0);
    println!("Action {}, len {}, checksum {}", args.action, res.0, res.1);

    Ok(())
}

async fn run_task(rt: Arc<Runtime>, args: Args) -> CommonResult<(u64, u64)> {
    let mut handle: Vec<JoinHandle<CommonResult<(u64, u64)>>> = vec![];
    let dir_list = args.dir_list()?;
    for (id, i) in (0..args.file_num).enumerate() {
        let next_dir = &dir_list[id % dir_list.len()];
        let path = format!("{}/{}", next_dir, i);
        info!("path = {}", path);

        let h = match args.action.as_str() {
            "write" => rt.spawn(write(rt.clone(), args.clone(), path)),

            "read" => rt.spawn(read(rt.clone(), args.clone(), path)),

            _ => return err_box!("Unsupported operation"),
        };

        handle.push(h);
    }

    let mut res = (0, 0);
    for h in handle {
        let v = h.await??;
        res.0 += v.0;
        res.1 += v.1;
    }

    Ok(res)
}

async fn write(rt: Arc<Runtime>, args: Args, path: String) -> CommonResult<(u64, u64)> {
    let file = LocalFile::with_write(path, true)?;
    let str = Arc::new(Utils::rand_str(args.buf_size()));

    let mut checksum: u64 = 0;
    let ptr = RawPtr::from_ref(&file);

    let loop_num = args.file_size() / args.buf_size();
    for _ in 0..loop_num {
        let write_str = str.clone();
        let mut p = ptr.clone();
        rt.spawn(async move { p.write_all(write_str.as_bytes()) })
            .await??;

        if args.checksum == 1 {
            checksum += Utils::crc32(str.as_bytes()) as u64
        }
    }

    Ok((file.pos() as u64, checksum))
}

async fn read(rt: Arc<Runtime>, args: Args, path: String) -> CommonResult<(u64, u64)> {
    let file = LocalFile::with_read(path, 0)?;
    let base_size = args.buf_size();
    let mut buf = BytesMut::zeroed(2 * base_size);

    let mut checksum: u64 = 0;
    let loop_num = args.file_size() / args.buf_size();
    let ptr = RawPtr::from_ref(&file);

    for _ in 0..loop_num {
        let mut read_buf = {
            buf.reserve(base_size);
            unsafe { buf.set_len(base_size) }
            buf.split_to(base_size)
        };

        let mut p = ptr.clone();
        let res: CommonResult<BytesMut> = rt
            .spawn(async move {
                p.read_all(&mut read_buf[..])?;
                Ok(read_buf)
            })
            .await?;
        let read_buf = res?;
        if args.checksum == 1 {
            checksum += Utils::crc32(&read_buf) as u64;
        }
    }

    Ok((file.pos() as u64, checksum))
}

#[derive(Debug, Parser, Clone)]
struct Args {
    #[arg(long, default_value = "")]
    pub action: String,

    // Configuration file path
    #[arg(long, default_value = "")]
    conf: String,

    // Write the directory list, separated by multiple directories using the "," sign.
    #[arg(long, default_value = "/curvine-fuse/fuse-bench")]
    pub dirs: String,

    #[arg(long, default_value = "10")]
    pub file_num: usize,

    // How many threads are used to simulate the client service
    #[arg(long, default_value = "10")]
    pub client_threads: usize,

    // The data size is read and written every time.Default 1mb
    #[arg(long, default_value = "128KB")]
    pub buf_size: String,

    // File size.Default is 10mb.
    #[arg(long, default_value = "100MB")]
    pub file_size: String,

    // Whether to delete the file after reading is completed.
    #[arg(long, default_value = "false")]
    pub delete_file: bool,

    // Whether to calculate checksum
    #[arg(long, default_value = "0")]
    pub checksum: i32,
}

impl Args {
    fn buf_size(&self) -> usize {
        ByteUnit::string_to_byte(&self.buf_size) as usize
    }

    fn file_size(&self) -> usize {
        ByteUnit::string_to_byte(&self.file_size) as usize
    }

    fn dir_list(&self) -> CommonResult<Vec<String>> {
        let mut list = vec![];
        for dir in self.dirs.split(",") {
            FileUtils::create_dir(dir, true)?;
            list.push(dir.to_string())
        }
        Ok(list)
    }
}
