# Retained-history capacity verifier

`capacity.py` exercises a single pinned Linux binary over its public HTTP API. It emits `bog.retained-history.capacity.v1` JSON with `status: pass` only after the complete snapshot, backfill and restart walks reconcile. A failure returns exit code 1 and an `errors` array. This is evidence for the #37 gate, not a threshold decision: gate owners must compare the measurements with their agreed limits.

Build from a clean checkout of the desired revision with Go's VCS metadata enabled, then run inside a **finite cgroup v2 memory limit** on the target Linux host and filesystem. Python 3 and Go (for `go version -m`) are required. For example, with a writable, delegated cgroup whose memory and I/O controllers are enabled:

```sh
git checkout <full-commit>
go build -buildvcs=true -o /tmp/bog ./cmd/bog
sha256sum /tmp/bog
mkdir /path/on/target/device/empty-data
python3 bench/retained-history/capacity.py --binary /tmp/bog \
  --commit <full-commit> --sha256 <sha256sum> \
  --data-dir /path/on/target/device/empty-data --report /tmp/capacity.json
```

The binary must contain the supplied full VCS revision and `vcs.modified=false`; the supplied digest must match its bytes. Use the same binary digest across gate verifiers. The data directory must be empty, private, and on the intended device; it is populated and left in place. The runner creates a fresh child cgroup and runs both server instances in it; the parent must delegate cgroup creation and the memory and I/O controllers. It records the minimum finite `memory.max` across the visible ancestor hierarchy. Use a cgroup namespace exposing all relevant ancestors to capture the actual effective cap. Avoid competing workloads when comparing measurements. The report records fixture parameters, effective cgroup limit, kernel and architecture, digest and revision. `--rows`, `--batch-size`, `--writers`, and `--page-size` select the workload; rows must exceed the page size by at least eight, and page size must be 2–1000 and not divisible by seven so a timestamp tie crosses a page boundary.

The reusable `capacity-v1` fixture maps integer `i` to line `capacity-%012d` and timestamp `2020-01-01T00:00:00Z` plus `(i//7*7919)%86400` seconds. Seven successive rows share a timestamp; the permutation produces out-of-order historical inserts. All rows except the final one are ingested in concurrent batches while queries run. The final row is inserted after the first snapshot page with a timestamp strictly after the cursor timestamp and before the latest fixture timestamp; its line retains the final index. The runner fails if the selected fixture cannot provide such a timestamp. The verifier checks exact row multiplicity, order, snapshot isolation, and the complete walk after restart. Concurrent batch commit order can vary; tie order is checked against the snapshot and across restart rather than assumed from row IDs. The fixture definition is usable by #4 without this runner.

Peak memory is the fresh child cgroup's `memory.peak`, including charged page cache and both server instances. Device I/O uses the child's `io.stat` read and write bytes for the data directory's filesystem device (major:minor), with separate live and restart deltas. The verifier fails if that device has no cgroup I/O counters; use a filesystem whose device is represented in `io.stat` (not an overlay or unsupported virtual filesystem). Buffered writeback may continue after sampling, and stacked devices can account I/O at a different layer. Disk headroom is `statvfs` available bytes after the live workload. Ingest p99 is nearest-rank over successful request durations, including the single backfill request. Restart-to-ready measures launch through the first successful `/healthz`; restart I/O includes the subsequent reconciliation walk. Results are host- and workload-dependent.
