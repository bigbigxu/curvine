# Transfer fault matrix

Scope: transfer service resilience. Every row kills something while a transfer
job is executing and asserts the job still converges with correct data.

Datasets are deliberately large enough that the job is still running when the
fault lands: 100-200 files of 256KB-1MB. A smaller or already-cached dataset
finishes before the kill and proves nothing.

## Matrix

| # | Fault | How | Expected job result | `takeover_count` | Other expected signals |
| - | ----- | --- | ------------------- | ---------------- | ---------------------- |
| T1 | kill the owning transfer mid-flight | find `transfer_jobs.owner`, map to rpc port, `kill -9` | terminal, usually `Completed` | `>= 1` | lease expires after `lease_timeout`, another instance acquires and finishes |
| T2 | kill a non-owning transfer | `kill -9` an instance that logs no transition for this job | unchanged, progresses normally | `0` | no requeue, no re-plan for that job |
| T3 | kill 2 of 3 transfers (majority loss) | `kill -9` both, including the owner | terminal | `>= 1` | the survivor serves reports and completes; registry drops the dead within TTL |
| T4 | kill all transfers, restart one | `kill -9` all three, restart a single instance | terminal | `>= 1` | restart re-plans: `re-planning: this instance holds no task state for the job` |
| T5 | restart the only transfer (same address, new owner) | `kill -9` then restart with the same config | terminal | `>= 1` | re-plan path; already-committed files are skipped, not re-copied |
| T6 | kill the worker running the job's task | kill the worker that accepted the task | terminal, `Completed` | `0` | task retried (`retrying failed file-level tasks`), dead worker briefly blacklisted, retry lands on a healthy node |
| T7 | kill a worker **and** the owning transfer together | combine T1 + T6 | terminal | `>= 1` | retry and takeover are independent mechanisms and must both work |

`takeover_count` semantics: incremented only when a lease is won over an
**expired** one while the job was still `Dispatching`/`Running`. Ordinary
renewals never count, and worker failures never count — a large value there means
a bug.

## Assertions per round

```bash
JOB=<job id>

# 1. terminal state, reachable without manual action
mysql -h127.0.0.1 -P3308 -uroot -pcurvine curvine_transfer -N -e \
  "select state from transfer_jobs where job_id='$JOB'"          # 6|7|8|9

# 2. ownership accounting
mysql ... -N -e "select json_extract(record_json,'\$.takeover_count')
                 from transfer_jobs where job_id='$JOB'"

# 3. no persisted rows for a file-level job
mysql ... -N -e "select count(*) from transfer_tasks"            # 0

# 4. data really landed (per matrix "verify" column)
cv --conf $BASE/curvine.toml fs blocks $BASE/ufs/r1/f-001.dat
cv --conf $BASE/curvine.toml fs blocks $BASE/ufs/r1/f-150.dat     # last file too
cv --conf $BASE/curvine.toml fs get    $BASE/ufs/r1/f-150.dat /tmp/c.bin
cmp $BASE/ufs/r1/f-150.dat /tmp/c.bin
```

Check the **last** file, not just the first: a partial transfer shows up as
missing blocks at the tail.

## Reporting caveat: the summary describes the last run only

A failover or restart re-plans the job. If the earlier attempt had already
committed every file, the re-plan skips everything and finishes with
`incremental transfer complete: skipped_files=N`, leaving
`summary.loaded_size = 0`, `total_size = 0` and `terminal_counts.completed = 0`
on a job that actually copied every byte.

Therefore:

- never conclude "nothing was copied" from a 0 summary
- verify with `fs blocks` and `cmp`
- distinguish the two outcomes by the terminal message:
  - `all tasks completed` — this run did the work
  - `incremental transfer complete: skipped_files=N` — earlier attempts did it

## Interpreting failures

| Observation | Likely cause |
| ----------- | ------------ |
| job `Failed`, cause `retry:never ... Connection reset by peer` | transport failures classified as permanent (should be retryable) |
| job `PartialSuccess`, progress stalled well below total | retries burned on the same dead worker (no dispatch blacklist) |
| job stuck `Running`, `loaded` frozen, worker killed | stale detection not reaching the task (stale probe is store-row based) |
| job stuck in `waiting for worker capacity` | snapshot reports zero live workers: worker dir missing, non-default worker `rpc_port`, or memory storage |
| `takeover_count` grows on a healthy job | renewal counted as a takeover |
| terminal `Completed` but target blocks missing | convergence decided on an empty task set without re-planning |

## Recovery budget to expect

| Fault | Observed recovery |
| ----- | ----------------- |
| worker death | one retry round, seconds |
| owner transfer death | `lease_timeout` + one scheduler tick (tens of seconds) |
| all transfers down | after one restarts and its snapshot refreshes |

A run where recovery takes minutes means the retry budget is being consumed by
picking the same dead node — treat it as a defect, not as slowness.
