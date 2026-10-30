---
name: cv-error-inject
description: Build a multi-instance Curvine deployment (3 master / 3 transfer / 5 worker) on one host and run fault-injection rounds against it. Use when verifying transfer resilience, killing transfer instances or workers mid-job, checking failover/takeover behaviour, or reproducing a fault-tolerance claim before review.
---

# cv-error-inject

Fault-injection harness for multi-instance Curvine deployments.

**Always deploy multiple instances.** A single-process cluster cannot exercise
ownership, takeover, placement or retry paths, so it proves nothing about
resilience. The default topology for every run is:

| Component | Count | Default ports |
| --------- | ----- | ------------- |
| master | 3 | 8995 + i / web 9110 + i |
| transfer | 3 | 9012 / 9022 / 9032 (web +1) |
| worker | 5 | 8997 on loopback aliases 127.0.0.2-6 |

Scope of this revision: **transfer only**. Master-only and worker-only fault
scenarios are intentionally not documented yet.

## When to Use

- A change touches transfer ownership, leases, dispatch, retries or reporting
- You need to prove a transfer job survives an instance or worker dying
- You are reviewing a fault-tolerance claim and want evidence, not reasoning
- You need to reproduce a "job completed but nothing was copied" class of bug

## Test objectives

Each run must prove all of these; a green job status alone is not enough.

1. **Correctness** — every source file is present at the target and byte-identical.
   Never trust the job summary for this; it describes the *last* run (see
   [references/fault-matrix.md](references/fault-matrix.md)).
2. **Convergence** — the job reaches a terminal state without manual action.
3. **Ownership** — a taken-over job records `takeover_count > 0`; a worker-only
   fault must leave it at 0.
4. **No persisted task rows** for file-level tasks (`transfer_tasks` stays empty).
5. **Bounded recovery** — terminal state is reached within the lease budget, not
   minutes.

## Step 1: Build the binaries once

Build only these three; avoid repeated full rebuilds.

```bash
cargo build -p curvine-server -p curvine-cli -p curvine-web
```

## Step 2: Provide MySQL for placement

`transfer.discovery = "sql"` is what makes placement and takeover testable, and
it needs MySQL. When no MySQL account is available, start a throwaway instance
under `/tmp` (few hundred MB, no image pull):

```bash
D=/tmp/cvtest-mysql; rm -rf $D; mkdir -p $D/data
mysqld --no-defaults --initialize-insecure --datadir=$D/data --user=$(whoami)
nohup mysqld --no-defaults --datadir=$D/data --port=3308 \
  --socket=$D/mysql.sock --bind-address=127.0.0.1 --mysqlx=0 \
  --pid-file=$D/mysqld.pid --log-error=$D/error.log > $D/out.log 2>&1 &
sleep 10
mysql -h127.0.0.1 -P3308 -uroot -e "alter user 'root'@'localhost' identified by 'curvine'; create database curvine_transfer;"
```

## Step 3: Generate configs from one template

Write a base config, then derive per-instance files from it. Full template and
every required field: [references/topology-and-config.md](references/topology-and-config.md).

Key values for a fault run:

```toml
[transfer]
enabled = true
discovery = "sql"
lease_timeout = "20s"                      # default 120s is too slow to observe
store_url = "mysql://root:curvine@127.0.0.1:3308/curvine_transfer"
endpoints = ["<host-ip>:9012", "<host-ip>:9022", "<host-ip>:9032"]
```

## Step 4: Start the topology and gate on readiness

Start master, then workers, then transfers. **Do not start transfers before
workers exist** or the first snapshot has no capacity.

```bash
# after each start, wait and verify:
cv --conf $BASE/curvine.toml node list                     # 5 healthy workers
curl -s http://<host-ip>:9013/metrics | grep transfer_cluster_snapshot_live_workers
```

The transfer's own `transfer_cluster_snapshot_live_workers` gauge is the source
of truth for dispatch readiness. The master keeps reporting a killed worker as
`Live` for a long time, so `node list` alone is misleading.

## Step 5: Run one fault round

For each round: create a **fresh** source tree (already-cached files make a job
finish instantly and prove nothing), submit, let it run briefly, kill, then poll.

```bash
mkdir -p $BASE/ufs/r1
for i in $(seq -w 1 150); do head -c 1048576 /dev/urandom > $BASE/ufs/r1/f-$i.dat; done

JOB=$(cv --conf $BASE/curvine.toml load file://$BASE/ufs/r1 $BASE/ufs/r1 | grep -oP 'Job ID: \K.*')
sleep 2
# kill the owner instance / a worker here (see the matrix)
until mysql -h127.0.0.1 -P3308 -uroot -pcurvine curvine_transfer -N \
  -e "select state from transfer_jobs where job_id='$JOB'" | grep -qE '^(6|7|8|9)$'; do
  sleep 2
done
```

Fault types, expected outcomes and the exact assertions: [references/fault-matrix.md](references/fault-matrix.md).

## Step 6: Verify, never assume

```bash
# ownership + terminal counters
mysql -h127.0.0.1 -P3308 -uroot -pcurvine curvine_transfer -e \
  "select state, json_extract(record_json,'\$.takeover_count'),
          json_extract(record_json,'\$.terminal_counts.completed'),
          json_extract(record_json,'\$.summary.message')
     from transfer_jobs where job_id='$JOB';"

# no persisted task rows for file-level tasks
mysql -h127.0.0.1 -P3308 -uroot -pcurvine curvine_transfer -e \
  "select count(*) from transfer_tasks;"

# data really landed: blocks exist, and content matches
cv --conf $BASE/curvine.toml fs blocks $BASE/ufs/r1/f-150.dat
cv --conf $BASE/curvine.toml fs get  $BASE/ufs/r1/f-150.dat /tmp/check.bin
cmp $BASE/ufs/r1/f-150.dat /tmp/check.bin
```

`fs stat` showing `state: Both` and a `ufs_mtime` is **not** proof: mount resync
sets that metadata for every inode before any byte is copied. Use `fs blocks`
(committed block + worker) or a `--cache-only` read.

## Step 7: Tear down and clean up

```bash
# kill by listening port / PID; never pkill -f "curvine-server --service ..."
for p in 8995 9012 9022 9032; do
  PID=$(ss -ltnp | grep ":$p " | grep -oP 'pid=\K[0-9]+' | head -1); [ -n "$PID" ] && kill $PID
done
rm -rf /tmp/cvtest-*            # data, logs, datasets, MySQL datadir
df -h /                          # confirm the host is not left near-full
```

Datasets are deliberately MB-scale. Keep each round's source tree under a few
hundred MB and delete it afterwards, or a few rounds will fill the disk.

## Related

- [cv-csi-test](../cv-csi-test/SKILL.md) — CSI integration testing
- [cv-test-report](../cv-test-report/SKILL.md) — publish results
- [cv-create-pr](../cv-create-pr/SKILL.md) — attach evidence to a PR

## Checklist

- [ ] 3 master / 3 transfer / 5 worker started, readiness gauges green
- [ ] Fresh dataset per round (no pre-cached files)
- [ ] Fault type matches a row in the matrix
- [ ] Job reached a terminal state without manual intervention
- [ ] `takeover_count` matches the expected value for that fault type
- [ ] `transfer_tasks` empty for file-level jobs
- [ ] Data verified by `fs blocks` + `cmp`, not by the job summary
- [ ] Environment torn down, disk checked
