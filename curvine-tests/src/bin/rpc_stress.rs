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

use bytes::{BufMut, BytesMut};
use curvine_tests::rpc_stress::{StressArgs, StressEnv};
use orpc::client::RpcClient;
use orpc::common::{Logger, SpeedCounter, Utils};
use orpc::message::{Builder, RequestStatus};
use orpc::runtime::{JoinHandle, RpcRuntime};
use orpc::server::RpcServer;
use orpc::sync::CountDownLatch;
use orpc::sys::DataSlice;
use orpc::test::file::file_handler::{FileService, RpcCode};
use orpc::CommonResult;
use std::sync::Arc;

///
/// RPC framework performance testing.
/// Compilation: cargo build --release
///
/// Start server: ./rpc_stress --action server --dirs data/d1,data/d2
/// Start writing client: ./rpc_stress --action write --file-num 20
/// Start reading client: ./rpc_stress --action read --file-num 20
///
fn main() {
    Logger::default();
    let args: StressArgs = StressArgs::new();
    log::info!("args: {:?}", args);

    match args.action.as_str() {
        "server" => start_server(args),
        _ => start_client(args),
    }
}

// cargo run --bin server -- --dirs data/d1,data/d2
fn start_server(args: StressArgs) {
    let conf = args.server_conf();
    let service = FileService::new(args.get_dir_list());
    let server = RpcServer::new(conf, service);

    server.block_on_start();
}

// Start writing to the client service.
// cargo run --bin client --  --name write --dirs data/d1,data/d2
fn start_client(args: StressArgs) {
    let args = Arc::new(args);
    let env = StressEnv::new(args.clone());
    let speed = SpeedCounter::new();

    let _ = match args.action.as_str() {
        "write" => run_task(RpcCode::Write, env.clone()),

        "read" => run_task(RpcCode::Read, env.clone()),

        _ => panic!("unsupported command"),
    };

    speed.log(
        args.action.as_str(),
        (args.file_num as u64) * (args.file_size as u64),
    );
    log::info!("{} end", args.action);
}

fn run_task(code: RpcCode, env: StressEnv) -> Vec<JoinHandle<CommonResult<(u64, u64)>>> {
    let mut res: Vec<JoinHandle<CommonResult<(u64, u64)>>> = Vec::new();
    let latch = Arc::new(CountDownLatch::new(env.args.file_num as u32));

    for id in 0..env.args.file_num {
        let t = env.clone();
        let l = latch.clone();

        let h = env.rt.spawn(async move {
            let client = t.create_client().await?;
            let res = match code {
                RpcCode::Write => {
                    write_block(id as u64, client, t.args.file_size, t.args.block_size).await?
                }

                RpcCode::Read => {
                    read_block(id as u64, client, t.args.file_size, t.args.block_size).await?
                }
            };

            l.count_down();
            Ok(res)
        });

        res.push(h);
    }

    latch.wait();
    res
}

async fn write_block(
    id: u64,
    client: RpcClient,
    file_size: usize,
    block_size: usize,
) -> CommonResult<(u64, u64)> {
    // open
    let mut bytes = BytesMut::new();
    bytes.put_u64(id);
    let req_id = Utils::req_id();
    let msg = Builder::new()
        .code(RpcCode::Write)
        .req_id(req_id)
        .request(RequestStatus::Open)
        .header(bytes)
        .build();

    client.rpc(msg).await?;

    let str = Utils::rand_str(block_size);
    let mut checksum: u64 = 0;
    let mut reaming = file_size;

    while reaming > 0 {
        let write_len = reaming.min(str.len());
        let data = BytesMut::from(&str[..write_len]);
        checksum += data.len() as u64;

        let msg = Builder::new()
            .code(RpcCode::Write)
            .req_id(req_id)
            .request(RequestStatus::Running)
            .data(DataSlice::Buffer(data))
            .build();

        client.rpc(msg).await?;
        reaming -= write_len;
    }

    Ok((id, checksum))
}

async fn read_block(
    id: u64,
    client: RpcClient,
    file_size: usize,
    _block_size: usize,
) -> CommonResult<(u64, u64)> {
    // open
    let mut bytes = BytesMut::new();
    bytes.put_u64(id);
    let req_id = Utils::req_id();
    let msg = Builder::new()
        .code(RpcCode::Read)
        .req_id(req_id)
        .request(RequestStatus::Open)
        .header(bytes)
        .build();
    client.rpc(msg).await?;

    // read。
    let mut checksum: u64 = 0;
    let mut reaming = file_size;
    while reaming > 0 {
        let msg = Builder::new()
            .code(RpcCode::Read)
            .req_id(req_id)
            .request(RequestStatus::Running)
            .build();

        let response = client.rpc(msg).await?;
        let bytes = response.data_bytes().unwrap();
        reaming -= bytes.len();
        checksum += bytes.len() as u64;
    }

    //log::info!("file {} read end, checksum = {}", id, checksum);
    Ok((id, checksum))
}
