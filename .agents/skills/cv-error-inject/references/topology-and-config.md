# Topology and config

Single-host layout for 3 master / 3 transfer / 5 worker. Everything lives under
one base directory so teardown is a single `rm -rf`.

## Port plan

| Role | RPC | Web | Notes |
| ---- | --- | --- | ----- |
| master 1-3 | 8995, 8996 (raft) | 9110 | raft membership uses `journal_addrs` |
| transfer 1-3 | 9012 / 9022 / 9032 | 9013 / 9023 / 9033 | `discovery = "sql"` |
| worker 1-5 | 8997 on 127.0.0.2-6 | n/a | loopback aliases, identical port |
| console | — | 9005 | optional |

Pick a host IP for the transfer endpoints (for example the value of
`hostname -I | awk '{print $1}'`).

## Why loopback aliases for workers

Several workers on one host must each own a distinct address; the RPC port stays
at the default 8997 for every one of them.

Do **not** give each worker a distinct `rpc_port` instead. With non-default
`rpc_port` the master's capacity aggregation collapses: `fs df` reports
`Size 0`, the transfer snapshot reports `live_workers = 0`, and every job stalls
in `waiting for worker capacity` while `node list` still shows healthy workers.

```bash
for i in 0 1 2 3 4; do mkdir -p "$BASE/data/h$i/<cluster_id>"; done
```

The `<dir>/<cluster_id>` subdirectory must exist **before** the worker starts.
A missing directory makes the worker register as `unhealthy` with
`path_missing(...)` and `0` capacity, which looks like a transfer bug but is not.

## Storage type

Use a plain filesystem path (`/tmp/<base>/data/hN`). A memory-backed directory
(`[MEM:2GB]/tmp/...`) is accepted by the worker but the master reports zero
capacity for it, so dispatch never happens. Only use memory storage when you are
explicitly testing capacity handling.

## Base config template

```toml
format_master = true
format_worker = true
cluster_id = "cv-fault"

[master]
hostname = "localhost"
web_port = 9110
meta_dir = "/tmp/cv-fault/meta"

[journal]
journal_dir = "/tmp/cv-fault/journal"
journal_addrs = [
    {id = 1, hostname = "localhost", port = 8996}
]

[worker]
hostname = "127.0.0.2"
data_dir = ["/tmp/cv-fault/data/h0"]

[client]
master_addrs = [
    {hostname = "localhost", port = 8995}
]
short_circuit = false

[transfer]
enabled = true
hostname = "<host-ip>"            # must not be loopback
rpc_port = 9012
web_port = 9013
store_url = "mysql://root:curvine@127.0.0.1:3308/curvine_transfer"
discovery = "sql"
lease_timeout = "20s"
endpoints = ["<host-ip>:9012", "<host-ip>:9022", "<host-ip>:9032"]
max_running_transfers = 16
log = { level = "info", log_dir = "/tmp/cv-fault/logs", file_name = "transfer.log" }

[web]
port = 9005
hostname = "localhost"

[log]
level = "info"
log_dir = "/tmp/cv-fault/logs"
```

Per-instance derivation: change `transfer.rpc_port` / `web_port` / log file name,
`worker.hostname` / `data_dir`, and nothing else.

## Config traps

| Symptom | Cause | Fix |
| ------- | ----- | --- |
| `transfer.endpoints must not advertise loopback addresses` | mysql/postgres store with loopback endpoints | advertise the host IP |
| `invalid service id: host:port` | unknown TOML key rejected elsewhere, or an unsanitised id | ids allow `[A-Za-z0-9_-]` only |
| `path_missing(<dir>/<cluster_id>)` | worker data subdir absent | create it before start |
| `fs df` shows `Size 0` | non-default worker `rpc_port`, or `[MEM:...]` storage | one address per worker, default port, disk path |
| `lease_timeout` unrecognised under another section | it belongs to `[transfer]` | move it |
| unknown-key warnings for `master.port`, `worker.port`, `journal.raft_port` | wrong key names; ports come from defaults | harmless, but do not rely on them |

## Startup order

1. master, wait ~12s until it accepts RPC
2. all workers, ~2s apart, then wait ~15s
3. all transfers, ~5s apart, then wait ~14s
4. gate on readiness before submitting any job

```bash
./target/debug/curvine-server --service master   --conf $BASE/curvine.toml &
./target/debug/curvine-server --service worker   --conf $BASE/worker-h0.toml &
./target/debug/curvine-server --service transfer --conf $BASE/transfer-1.toml &
```

## Killing processes safely

Never use `pkill -f "curvine-server --service worker"`: the pattern also matches
the command line of the shell running it, so the shell kills itself
(`exit 143`) and the rest of the script never runs.

```bash
# by listening port
PID=$(ss -ltnp | grep ":9012 " | grep -oP 'pid=\K[0-9]+' | head -1); kill -9 $PID
# by bound address, for workers sharing a port
PID=$(ss -ltnp | grep "127.0.0.4:8997" | grep -oP 'pid=\K[0-9]+' | head -1); kill -9 $PID
```

## Liveness timing

| Signal | Latency |
| ------ | ------- |
| transfer heartbeat / registry lease | 3s heartbeat, 10s TTL |
| transfer's own worker snapshot | ~10s refresh (`client.mount_update_ttl_ms`) |
| master marking a dead worker not-Live | can take minutes; ignore it |
| job lease expiry after owner death | `transfer.lease_timeout` (use 20s) |
| task stale timeout | derived from `lease_timeout` (~1/6 of it) |

Long summaries of the reasoning behind these numbers live in the design notes,
not here; measure them if they matter for a specific run.
