# Linux durability verifier

Build the pinned Bog revision once on Linux. From the verifier checkout, pass
its executable and commit SHA to the producer.

```sh
go run ./verify/linux \
  -binary /absolute/path/to/bog -revision "$REVISION" \
  -fixture linux-release-v1 -report /absolute/path/to/durability.json
```

The Linux release workflow checks the candidate's VCS build metadata. This
producer hashes the supplied executable and writes schema-version 1 case
evidence. A case failure produces a failing report entry and a nonzero exit.
Run as a user who can mount and unmount a private tmpfs using either mount
privileges or noninteractive `sudo -n`. The verifier creates fresh data
directories and binds the candidate to loopback. The disk-full case creates
an 8 MiB tmpfs, allocates its remaining blocks, and requires an extra write
to return kernel `ENOSPC`. Bog opens a data directory through a symlink.
The verifier then atomically retargets the symlink across filesystems while
Bog is serving.

- With a healthy directory open and the path pointing at the full tmpfs,
  `/ready` must continue reporting `ready`. Ingest must return HTTP 204.
  A fresh walk after restart confirms the batch was committed to the
  original healthy directory.
- With the full tmpfs open and the path pointing at the healthy directory,
  `/ready` must continue reporting `low_disk_headroom`. Ingest must return
  HTTP 503 and readiness must report `wal_rollback` with category `enospc`.
  The verifier checks the live walk, restores the launch path, and checks
  replay at capacity. After freeing space, it acknowledges another batch,
  kills the candidate, and checks a complete walk after restart.

Each retarget direction samples readiness for six seconds before ingest, with
a ten-second deadline for each observation. This outlasts cached readiness
evidence and spans multiple disk samples. The `enospc` case reports
`healthy_to_full` and `full_to_healthy` with the observed readiness status
and reason and the ingest HTTP status. It also records replay success for
`healthy_to_full` and the rollback reason and category for
`full_to_healthy`. Candidate cleanup precedes unmount on every exit path.

Every candidate runs in its own process group. The verifier terminates the
group on startup failure or cancellation and before reaping an exited leader.
Shutdown and output drainage have bounded deadlines, including when a
descendant inherits the candidate's output pipes.

The fixture records ingest outcomes independently of Bog. A complete cursor
walk must match its ledger, including append order within timestamp ties.
The SIGKILL case sends a complete request after HTTP 100 and kills the server
without reading the final response. Recovery must return that batch in full
or omit it entirely. The recovery case injects a partial frame after the
committed WAL, tests tied backfill and the original snapshot, and then corrupts
a committed payload byte. Startup must fail without changing the corrupted WAL
or the commit marker. This injection uses the `BOGWAL01` frame layout and
`bog.commit` used by both historical candidates. Every successful restart
checks that the previous process's cursor returns HTTP 400 with the exact
body `invalid cursor\n` and no continuation header or trailer. Fresh and
continuation pages must return HTTP 200 with `application/x-ndjson`, fully
terminated rows, and a valid continuation if present. The verifier accepts
both length-delimited and legacy chunked pages.

## Historical correctness and benchmark

All three correctness cases passed on `f727b8f676845a8058d1ecf114442d8963cbbbae`
and on the implementation base `b797128b09c278bc547ef73e25d7d67c97315966`.

Use `-benchmark` with the same four arguments and a separate report path on
each run. Alternate the two supplied binaries on the same Linux host and
filesystem. Each run uses seed 33003, 128,000 rows, 128 rows per ingest,
and 256 rows per query page. Ingest timing covers HTTP request through the
durable acknowledgement. Query p95 covers individual cursor pages after
replay, including response decoding. Restart time covers launch through
the first successful health probe, sampled every 5 ms. Peak RSS is the
larger Linux process high-water mark across the two starts.

Recorded on `lima-bog-final-qa`, Linux arm64 kernel `6.8.0-100-generic`, ext4,
with Go 1.27.0 and `-trimpath` builds from the pinned sources.

| Revision | Binary SHA-256 |
| --- | --- |
| `f727b8f` | `86cfc2016bb58b1109ed56eb2a0cdbfe2116ecc0b19a51d60d6e5a022d8b70f7` |
| `2c26412` | `8a120cebb49946c05cfba3602b5940b59f2cafe885e1d7fe4f5b275d19da77af` |

| Run | Revision | Durable rows/s | Ingest p99 (ms) | Query p95 (ms) | Restart (ms) | Peak RSS (KiB) |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| 1 | `f727b8f` | 100,975 | 2.335 | 0.425 | 56.258 | 45,056 |
| 2 | `2c26412` | 115,888 | 2.367 | 0.479 | 50.882 | 41,216 |
| 3 | `f727b8f` | 103,609 | 2.624 | 0.686 | 55.782 | 41,216 |
| 4 | `2c26412` | 120,233 | 2.227 | 0.621 | 76.779 | 41,600 |
| 5 | `f727b8f` | 106,743 | 4.033 | 0.401 | 56.318 | 41,600 |
| 6 | `2c26412` | 125,292 | 1.663 | 0.561 | 51.457 | 43,264 |

Durable ingest ranged from 101k to 107k rows/s on `f727b8f` and from 116k
to 125k on `2c26412`. Ingest p99 varied by 1.698 ms and 0.704 ms respectively.
The query p95 ranges overlapped and the longest restart, 76.779 ms, occurred
on the fourth run.

## Cutover diagnostics

The AgentSet/bog Linux qualification owner owns the producer evidence and
triages the pinned #53 run in Slipway. Match the revision, binary SHA-256,
and fixture across the admission, durability, capacity, and parity reports.

The durability report's additive `diagnostics` object is keyed by case.
Candidate entries are in launch order, including failed starts and the
intentional corruption refusal. They include start-to-`/healthz` time,
exit code or signal, peak RSS where available, and the first 32 KiB of
combined candidate output. A null start-to-health time means the probe
never succeeded or the case expected the candidate to refuse startup.
Replay-only duration is unavailable because the candidate has no separate
replay timing signal. Query evidence groups fresh pages, continuations,
and stale-cursor checks. It retains counts by client-observed admission,
maximum phase durations, the latest attempt, and the first query failure
when present. Case evidence still carries the ledger and retarget checks.

For a fresh backfill deadline, read the pinned admission report in sequence.
The release probe holds a continuation at zero receive window, verifies
prompt rejection of a competing query and HTTP 204 for ingest, then
releases the slow connection. It retries the held cursor before issuing a
fresh query for the acknowledged row. Both of those last steps share the
same 12-second context. Compare the capacity report's
`after_publication_continuation` and `fresh_backfill` phases to determine
whether the held walk completed and how far the fresh request progressed
under its own budget. Missing headers leave admission unknown. Headers
without a complete body place the interruption in response delivery.
Once the body is read and closed, malformed pages and unrelated HTTP 400
responses are recorded as validation failures even though both response
timings are available. A completed rejection identifies the HTTP refusal
when available. Use candidate exit, logs, and memory counters to correlate
process failure with the query. [Release admission fixture](../release/README.md#admission-fixture)
