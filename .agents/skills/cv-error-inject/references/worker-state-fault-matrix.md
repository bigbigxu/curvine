# Worker / master state machine fault matrix

Scope: **worker state machine, intent durability and block-location accounting**
(CV-60). Every row injects a process-level fault and asserts the *state* and the
*data* are still what the operator asked for. Independent of the transfer matrix
in [fault-matrix.md](fault-matrix.md); run them as separate rounds and do not mix
datasets.

Prerequisite: the build under test is the branch that owns the state machine
(`feature/CV-60-worker-state-machine` or later). `block_replication_enabled` must
be `true` — `decommission` is refused without it, and drain has no pipeline.

Topology, config, startup order and observation commands:
[worker-state-topology.md](worker-state-topology.md).

## What a round must prove

A green `node list` proves nothing. Each round asserts:

1. **State** — `cv node list` label (`normal` / `cordoned` / `decommissioning` /
   `decommissioned` / `normal+stale` / `normal+lost` / `normal+shutdown`) matches
   the operator action, and survives ≥2 heartbeat periods **and** a master
   failover.
2. **Intent durability** — the intent is in the journal, so a restarted or newly
   elected master still enforces it. `cv node list` after failover is the check.
3. **Reads** — read the file back and `cmp` it. Do not trust `fs blocks` order.
4. **No storm** — count `Successfully replicated` / `Drained block` lines in the
   master log across the round. A graceful restart or a short disconnect must add
   **0**; only `LOST` and the promotion cleanup may add jobs.
5. **Bytes retained** — `du -sh <worker data dir>` and the block-file count must
   not shrink while a worker is `DECOMMISSIONING`/`DECOMMISSIONED`. Deleting
   bytes is an explicit operator action, not a side effect.

## Matrix

Ports/worker addresses assume the layout in
[worker-state-topology.md](worker-state-topology.md): workers `.10`–`.14`.

| # | Fault | How | Expected | Signals to assert |
| - | ----- | --- | -------- | ----------------- |
| S1 | cordon, then fail the leader | `cv node cordon .12`, wait ≥3 heartbeats, `kill -9` the active master | stays `cordoned`; no new placement; reads OK | `cv node list` after failover; new file's `fs blocks` avoids `.12`; `worker_state_transition_count{from="normal",to="cordoned"}=1` |
| S2 | decommission a worker that holds data | `cv node decommission <w>` | `decommissioning` → `decommissioned`; blocks move; reads never fail | `worker_drain_blocks_remaining` falls to 0; `fs blocks` no longer lists the worker; read + `cmp`; worker disk **unchanged** |
| S3 | drain with no target | cordon every other worker, then decommission the holder | stalls with a reason, does not spin silently | `worker_drain_stalled{reason="no_target_worker"}=1`, `worker_drain_single_replica_blocks>0`; log `block(s) have no writable worker left`; after uncordon it completes within seconds |
| S4 | force retirement | same as S3 with `worker_drain_timeout=20s`: wait past the deadline, then `cv node force-decommission <w>` | the deadline alone only reports the stall; the **operator** force turns it `decommissioned`, drops the remaining locations **and deletes those blocks on the node** | past the deadline: still `decommissioning`, `worker_drain_stalled{reason="deadline_exceeded"}=1`, one WARN about forcing; after the force: `decommissioned`, log `Force-retired <host>: N block(s) deleted, M lost their last copy`, the worker's block files are gone; `recommission` cannot restore them (bytes deleted) |
| S5 | worker `kill -9`, recovery inside the lost window | `kill -9` at t0, restart before `worker_lost_interval` | `normal+stale` → `normal`; locations kept; **0** replication | `cv node list` at t+15s; `fs blocks` still lists the worker's copies; replication delta 0 |
| S6 | worker `kill -9`, recovery after the lost window | wait > `worker_lost_interval`, then restart | `normal+lost`; locations dropped; repair runs; restart reconciles extras | replication delta > 0; `fsck` OK; restart deletes the extra block files (disk count drops) with delta 0 |
| S7 | graceful shutdown | `kill -TERM <worker>` | `normal+shutdown`; locations kept for `worker_shutdown_grace` | locations retained; **0** replication during the grace; after grace → `normal+lost` + repair |
| S8 | decommissioned worker restarts with old blocks | restart a worker whose intent is `decommissioned` | report ignored, no `DeleteBlock`, disk unchanged | `Ignore block report from decommissioned/running worker ... (N blocks)`; block-file count identical before/after |
| S9 | recommission | `cv node recommission <w>`, restart the worker (full report) | `normal`; extra copies deleted; under-replicated ones re-added | `fs blocks` counts return to the replica target; read + `cmp`; disk shrinks by the extras. After a **forced** retirement the bytes are gone, so the file stays unreadable unless another replica exists — assert that difference |
| S10 | operator action on a stale node | kill a worker, wait until `stale`, `cv node decommission` it, restart | intent only while unreachable; recovery goes **straight** to `decommissioning` (never `normal`) | `decommissioning+stale` → `decommissioning` → `decommissioned`; drain runs after recovery |
| S11 | fail the leader while draining | decommission a worker with data, `kill -9` the active master mid-drain | new leader keeps the intent and the drain continues | intent survives; drain completes; **`DECOMMISSIONED` worker holds no location**; promotion cleanup + repair visible in the log |
| S12 | lose quorum, then recover | `kill -9` two of three masters, wait > `worker_lost_interval`, restart them | writes fail with a clear error; **no** stale/lost storm after recovery | write error mentions `Not a leader master`; after recovery every worker keeps its pre-outage state; no `trigger=StaleTimeout/LostTimeout` for the outage window; replication only from the promotion cleanup |
| S13 | block worker ↔ master (network) | `iptables -I INPUT/OUTPUT` on the worker IP + master ports for 60s | `stale` then recovery; reads degrade but succeed | needs sudo; skip when the host is shared |
| S14 | single data dir read-only | `mount -o remount,ro <worker data dir>` | that dir unusable, worker stays `normal` if another dir exists, new writes avoid it | needs a worker with ≥2 data dirs; health metrics, not state metrics |
| S15 | all workers cordoned, then write | cordon every worker, `cv fs put` | rejected with a clear error; undo restores writes | error `No eligible workers`; after uncordon the same put succeeds |
| S16 | stale from a non-`normal` admin state | cordon a worker, `kill -9` it, wait past the stale threshold, restart | `cordoned+stale` → `cordoned` (the admin axis is untouched by liveness) | transition log `cordoned/running -> cordoned/stale, trigger=StaleTimeout` then `-> cordoned/running, trigger=HeartbeatOk` |
| S17 | illegal transitions are refused | `uncordon`/`recommission` on a normal node, `force-decommission`/`cancel-decommission` without a drain, `decommission` twice | each refused with a reason, no state change | `already in state` for the idempotent ones, `not allowed in this state` otherwise |

`S13`/`S14` need privileges. `S11`/`S12` are the leader-failover rows: run them
after `S1`/`S2` so both journaled intent and block locations are non-trivial.

## Known limitation to watch for (CV-70)

Block locations are **not journaled**: followers only learn them from journaled
allocation, so after any leader change the new leader may serve a stale location
set (reads pointing at a node that no longer holds the block, repair jobs failing
with `block ... not exists`). CV-60 mitigates the part the state machine must
guarantee (a `DECOMMISSIONED` host holds no location) and repairs what it drops.

When a round shows:

- `NoAvailableWorker` in the client, yet `find <data dirs> -name 'blk_<id>'`
  finds copies elsewhere;
- `Errors on block replication ... block <id> not exists`;
- `fsck` under-replicated while the physical copies are ≥ target;

then restart every worker once (each sends a full report), re-check, and record
it as location drift rather than a state-machine failure. CoMind **CV-70** holds
the root cause and the fix options.

## Assertion helpers

```bash
BASE=/data/cvtest/worker-state      # harness root (any dir with a few GB free)
CV="$BIN/curvine-cli --conf $BASE/conf/m1.toml"

# state
$CV node list | grep <worker-ip>

# drain progress / stall reason
curl -s http://127.0.0.1:$(leader_web)/metrics | grep -E '^worker_(drain|state_transition|num|retained)'

# replication activity for the round (must be 0 when it must be 0)
grep -h 'Successfully replicated' $BASE/logs/master-m*.log | wc -l

# transition log (one line per change, with admin+liveness on both sides)
grep -h 'Worker state changed' $BASE/logs/master-m*.log | tail

# bytes retained on a drained worker
du -sh $BASE/data/h<N>; find $BASE/data/h<N> -name 'blk_*' | wc -l
```

## Coverage check

A run only counts as "all states and triggers exercised" if every trigger name
appears in the transition log and every state appears on one side of an arrow:

```bash
# 1. all 12 triggers
for t in Cordon Uncordon Decommission CancelDecommission DrainCompleted \
         ForceDecommission Recommission HeartbeatOk StaleTimeout LostTimeout \
         GracefulShutdown ShutdownGraceExpired; do
  printf '%-22s %s\n' "$t" "$(grep -h 'Worker state changed' $BASE/logs/master-m*.log | grep -c "trigger=$t")"
done

# 2. states (to-state of every transition)
grep -h 'Worker state changed' $BASE/logs/master-m*.log | grep -oP '\-> \K[a-z+/]+' | sort | uniq -c

# 3. distinct edges
grep -h 'Worker state changed' $BASE/logs/master-m*.log | grep -oP '[a-z+/]+ -> [a-z+/]+' | sort | uniq -c
```

Expected: 12/12 triggers non-zero; the seven states `normal`, `cordoned`,
`decommissioning`, `decommissioned`, `normal/stale`, `normal/lost`,
`normal/shutdown` all present (plus combos such as `cordoned/stale` and
`decommissioning/stale`).

## Reporting

Record for every round: the exact command, `cv node list` before/after, the
replication delta, the read-back result, and the relevant master log lines. A
row without the read-back check is not evidence.
