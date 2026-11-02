# Worker / master state machine topology and config

Single-host harness for **worker state machine, intent and block locations**.
Companion to [worker-state-fault-matrix.md](worker-state-fault-matrix.md).
For the transfer topology use [topology-and-config.md](topology-and-config.md)
instead — the two harnesses can coexist on one host if the port ranges differ.

## Port plan

| Role | Bind | Notes |
| ---- | ---- | ----- |
| master 1-3 (client RPC) | `127.0.0.1:8895 / 8897 / 8899` | `master.rpc_port` |
| master 1-3 (raft) | `127.0.0.1:8896 / 8898 / 8900` | `journal.rpc_port`, one per member |
| master 1-3 (web/metrics) | `9210 / 9211 / 9212` | scrape `/metrics` here |
| worker 1-5 | `127.0.0.10-14:8997` | loopback aliases, **default** port |
| CLI | `conf/m1.toml` | `short_circuit = false` |

Do **not** give the workers distinct `rpc_port` values to avoid a conflict:
with a non-default rpc_port the master's capacity aggregation collapses
(`fs df` reports 0). Give each worker a distinct **loopback address** and keep
8997.

The harness must not collide with an older `/tmp/cv*` cluster that may still be
running on `127.0.0.2-6:8997` and `9110`; pick different aliases/ports, or stop
that one first.

## Base config

```toml
format_master = false          # clean dirs + false: first start initialises, restarts keep state
format_worker = false
cluster_id = "cv-e2"

[client]
master_addrs = [
  {hostname = "127.0.0.1", port = 8895},
  {hostname = "127.0.0.1", port = 8897},
  {hostname = "127.0.0.1", port = 8899},
]
short_circuit = false
replicas = 2                   # default upload replica count

[log]
level = "info"
log_dir = "/data/cvtest/worker-state/logs"

[master]
hostname = "127.0.0.1"         # MUST be one of journal_addrs hostnames
heartbeat_interval = "2s"
worker_check_interval = "2s"   # must be < worker_lost_interval
worker_stale_interval = "10s"  # old key worker_blacklist_interval still accepted
worker_lost_interval = "60s"
worker_shutdown_grace = "20s"
worker_drain_timeout = "20s"   # short on purpose: test the force-completion path
worker_drain_scan_interval = "5s"
block_replication_enabled = true           # decommission is refused without it
block_replication_retry_interval = "2s"
block_replication_submit_timeout = "10s"
block_replication_job_timeout = "20s"
audit_logging_enabled = true

[master.log]                   # per-instance file; the global [log] does NOT apply to master
level = "info"
log_dir = "/data/cvtest/worker-state/logs"
file_name = "master-m1.log"
```

Per-instance derivation for `m1..m3`: `master.rpc_port`, `master.web_port`,
`master.meta_dir`, `journal.rpc_port`, `journal.journal_dir`, `master.log.file_name`.
Each master's `journal.journal_addrs` lists **all three** raft addresses:

```toml
[journal]
enable = true
hostname = "127.0.0.1"
rpc_port = 8896
journal_dir = "/data/cvtest/worker-state/journal-m1"
journal_addrs = [
  {id = 1, hostname = "127.0.0.1", port = 8896},
  {id = 2, hostname = "127.0.0.1", port = 8898},
  {id = 3, hostname = "127.0.0.1", port = 8900},
]
```

Worker per-instance:

```toml
[worker]
hostname = "127.0.0.10"
rpc_port = 8997
web_port = 9220
data_dir = ["/data/cvtest/worker-state/data/h1"]

[worker.log]
level = "info"
log_dir = "/data/cvtest/worker-state/logs"
file_name = "worker-h1.log"
```

Data directory: the worker expects `<data_dir>/<cluster_id>/` to exist before it
starts. A missing subdirectory registers the worker as unhealthy with
`path_missing(...)` and zero capacity.

## Startup order

1. masters (2s apart), wait ~15s until a leader is elected;
2. workers (1s apart), wait ~20s;
3. gate on readiness: `cv node list` shows all workers `normal`, `fs df` reports
   non-zero `Size`.

```bash
BIN=<worktree>/target/debug
BASE=/data/cvtest/worker-state
for i in 1 2 3; do nohup $BIN/curvine-server --service master --conf $BASE/conf/m$i.toml > $BASE/logs/m$i.out 2>&1 & sleep 2; done
sleep 15
for i in 1 2 3 4 5; do nohup $BIN/curvine-server --service worker --conf $BASE/conf/w$i.toml > $BASE/logs/w$i.out 2>&1 & sleep 1; done
sleep 20
$BIN/curvine-cli --conf $BASE/conf/m1.toml node list
```

## Leader detection

Never grep logs for "became leader" (stale lines survive a killed leader). Ask the
cluster and read the active master out of `fs df`:

```bash
leader_rpc(){ $CV fs df 2>/dev/null | grep -oP 'curvine://127\.0\.0\.1:\K[0-9]+' | head -1; }
leader_web(){ case "$(leader_rpc)" in 8895) echo 9210;; 8897) echo 9211;; 8899) echo 9212;; esac; }
```

## Killing processes safely

Never `pkill -f "curvine-server --service ..."`: the pattern also matches the
shell that runs it (`pgrep` kills your own command line). Kill by listening
socket instead:

```bash
# master by rpc port
kill -9 $(ss -ltnp | grep ':8895 ' | grep -oP 'pid=\K[0-9]+' | head -1)
# worker by address (all workers share port 8997)
kill -9 $(ss -ltnp | grep '127.0.0.10:8997' | grep -oP 'pid=\K[0-9]+' | head -1)
```

A `teardown.sh` that loops exactly these patterns (and nothing else) is the
safest form; keep it in the harness root and use it instead of ad-hoc kills.

## Data setup

- Multi-replica files exercise drain without data loss: the client default
  `replicas = 2` in the config above.
- Single-replica / last-copy cases need a second client config with
  `replicas = 1`:

```bash
sed 's/^replicas = 2$/replicas = 1/' conf/m1.toml > conf/tmp-r1.toml
CV1="$CV_BIN --conf $BASE/conf/tmp-r1.toml"
$CV1 fs put /tmp/data-64m.bin /r4/j.bin
```

- 64MB of `/dev/urandom` per file is enough; keep each round under a few hundred
  MB and delete it afterwards.
- Record the layout before and after with `cv fs blocks <path>` (worker list per
  block); it is the only cheap way to see "which worker really holds this".

## Observation toolkit

| What | Where |
| ---- | ----- |
| state (admin+liveness label) | `cv node list` (`normal`, `normal+stale`, `decommissioning`, …) |
| transitions | master log `Worker state changed: host=…, workers=[…], <from> -> <to>, trigger=…, operator=…, reason=…` |
| transition counters | `worker_state_transition_count{from,to}` on the active master's `/metrics` |
| drain backlog | `worker_drain_blocks_remaining{worker}`, `worker_drain_stalled{reason}`, `worker_drain_single_replica_blocks` |
| retained bytes | `worker_retained_bytes`, and `du -sh <worker data dir>` |
| fleet snapshot | `worker_num{tag}` (`live` / `blacklist` / `stale` / `decommission` / `lost`) |
| intent records | `journal` (`WorkerIntentEntry`), master log `PersistIntent` path, `cv node list` after failover |
| operator audit | `logs/audit.log` (`cmd=SetWorkerStatus`, `src` = worker selector) |

## Teardown

```bash
./teardown.sh                  # kills only this harness (exact ports/addresses)
rm -rf $BASE                    # meta, journal, data, logs
df -h /
```

Keep the harness root when the run is evidence for a review; it holds
`report/*.log` per scenario.
