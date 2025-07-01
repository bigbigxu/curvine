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
use curvine_client::file::CurvineFileSystem;
use curvine_common::conf::ClusterConf;
use curvine_common::fs::{Path, Reader, Writer};
use orpc::common::{ByteUnit, Logger, SpeedCounter, Utils};
use orpc::runtime::{AsyncRuntime, RpcRuntime, Runtime};
use orpc::{err_box, CommonResult};
use std::sync::Arc;
use tokio::task::JoinHandle;

/// File system read and write performance test
/// cargo run --release --bin curvine-fs-bench -- --action write --checksum 1 --file-num 100 --conf /home/xuen/glacier-cache/build/dist/conf/curvine-cluster.toml
/// cargo run --release --bin curvine-fs-bench -- --action read --checksum 1 --file-num 100 --conf /home/xuen/glacier-cache/build/dist/conf/curvine-cluster.toml
///
/// Test writing: ./lib/curvine-fs-bench --action write
/// Test writing: ./lib/curvine-fs-bench --action read
fn main() -> CommonResult<()> {
    let args: Args = Args::parse();
    println!("args: {:?}", args);

    let conf = args.get_conf()?;
    Logger::init(conf.log.clone());

    let rt = Arc::new(AsyncRuntime::new(
        "fs-stress",
        args.client_threads,
        args.client_threads,
    ));
    let fs = CurvineFileSystem::with_rt(conf, rt.clone())?;
    let _ = rt.block_on(fs.mkdir(&Path::from_str(&args.dir)?, true))?;

    let speed = SpeedCounter::new();

    let res = rt.block_on(run_task(rt.clone(), args.clone(), fs))?;
    let total_len = (args.file_num as u64) * (args.file_size() as u64);

    speed.print(args.action.as_str(), total_len);

    assert_eq!(total_len, res.0 as u64);
    println!("Action {}, len {}, checksum {}", args.action, res.0, res.1);

    Ok(())
}

#[derive(Debug, Parser, Clone)]
struct Args {
    #[arg(long, default_value = "write")]
    pub action: String,

    // Configuration file path
    #[arg(long, default_value = "conf/curvine-cluster.toml")]
    conf: String,

    #[arg(long, default_value = "/fs-bench")]
    pub dir: String,

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
    #[arg(long, default_value = "1")]
    pub checksum: i32,
}

impl Args {
    fn get_conf(&self) -> CommonResult<ClusterConf> {
        ClusterConf::from(&self.conf)
    }

    fn get_action(&self) -> CommonResult<Action> {
        match self.action.to_lowercase().as_str() {
            "write" => Ok(Action::Write),
            "read" => Ok(Action::Read),
            _ => err_box!("Unsupported operation"),
        }
    }

    fn buf_size(&self) -> usize {
        ByteUnit::string_to_byte(&self.buf_size) as usize
    }

    fn file_size(&self) -> usize {
        ByteUnit::string_to_byte(&self.file_size) as usize
    }
}

enum Action {
    Write,
    Read,
}

async fn run_task(rt: Arc<Runtime>, args: Args, fs: CurvineFileSystem) -> CommonResult<(i64, u64)> {
    let mut handle: Vec<JoinHandle<CommonResult<(i64, u64)>>> = vec![];
    let action = args.get_action()?;
    for i in 0..args.file_num {
        let path = Path::from_str(format!("{}/{}", args.dir, i))?;
        let h = match action {
            Action::Write => rt.spawn(write(fs.clone(), args.clone(), path)),

            Action::Read => rt.spawn(read(fs.clone(), args.clone(), path)),
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

async fn write(fs: CurvineFileSystem, args: Args, path: Path) -> CommonResult<(i64, u64)> {
    let str = "A".repeat(args.buf_size());
    let mut sum: u64 = 0;
    let loop_num = args.file_size() / args.buf_size();

    let mut writer = fs.create(&path, true).await?;
    for _ in 0..loop_num {
        writer.write(str.as_bytes()).await?;
        if args.checksum == 1 {
            sum += Utils::crc32(str.as_bytes()) as u64;
        }
    }

    let len = writer.pos();
    writer.complete().await?;

    Ok((len, sum))
}

async fn read(fs: CurvineFileSystem, args: Args, path: Path) -> CommonResult<(i64, u64)> {
    let mut sum: u64 = 0;
    let mut reader = fs.open(&path).await?;
    let mut buf = BytesMut::zeroed(args.buf_size());

    loop {
        let n = reader.read_full(&mut buf).await?;
        if n == 0 {
            break;
        }

        if args.checksum == 1 {
            sum += Utils::crc32(&buf[..n]) as u64;
        }
    }

    let len = reader.pos();
    reader.complete().await?;

    Ok((len, sum))
}
