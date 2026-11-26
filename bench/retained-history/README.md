# Retained-history Linux baseline

`run.py` builds and exercises the supplied clean checkout as a black box. It requires Linux, Python 3, Git, Go matching the checkout, and an **empty writable delegated cgroup v2 parent** with the memory and I/O controllers enabled. Run with privileges appropriate to that delegation. Put `--work-dir` on a local filesystem with sufficient space; the script rejects common network filesystem types, but the operator must verify the actual backing filesystem and durability guarantees. The temporary data directory is removed after the run. Do not run other processes in the experiment's cgroup. Device I/O counters depend on kernel and filesystem accounting; zero reads can reflect the page cache, not absence of logical reads.

```sh
python3 bench/retained-history/run.py \
  --checkout /path/to/clean-checkout \
  --revision 5421dc5fad38df84f1f185cd6afb89ef83d89057 \
  --work-dir /local/experiment --cgroup-parent /sys/fs/cgroup/delegated \
  --memory-max 536870912 --rows 2000 --seconds 5 --timeout 180 > result.json
```

The full revision must equal HEAD and `go version -m` must report that revision and `vcs.modified=false`. Build output and data are outside the checkout. The default fixture alternates ordered and backfilled batches with timestamp ties and duplicates. After durable seeding it restarts the server, walks all pages and compares exact ordered rows and multiplicity, then runs two ingest and two broad query workers concurrently. JSON includes errors, log tail, timeouts, cgroup memory and device I/O deltas, free disk before and after, restart latency, request counts and p99s. A nonzero exit indicates failure. I/O and peak memory cover the whole server lifecycle, including seed and restart; disk headroom after includes workload writes. p99 is the nearest-rank completed request latency. Rows are limited to 1 million, workload to 300 seconds, and timeout to 3600 seconds. The overall deadline is checked between operations; build and network requests have individual timeouts. Setup, filesystem copying, and cleanup can extend wall time beyond it.

## Prebuilt checkpoint fixture

Use a checkpoint-capable revision and a stopped, self-contained fixture directory from that same pin. Supply a complete expected snapshot NDJSON file (at most 1 million rows), in timestamp and original append order for ties. The fixture may be up to 100 GiB and is copied into the temporary local work directory. Provide the checkpoint file path relative to the fixture and the byte position of its little-endian uint64 covered WAL offset according to that pin's documented checkpoint format:

```sh
python3 bench/retained-history/run.py --checkout /path/to/pinned-checkout \
  --revision FULL_40_CHARACTER_COMMIT --work-dir /local/experiment \
  --fixture /local/stopped-fixture --expected /local/expected.ndjson \
  --checkpoint checkpoint-file --offset-at BYTE_POSITION --covered-offset OFFSET
```

Before starting the server or writing the workload, the harness checks the offset stored at that location against the supplied value, checks the legacy commit marker and WAL size, and reports the checkpoint hash, committed offset and `committed_uncovered_wal_bytes`. The operator must choose the real coverage field and verify the pin's format and checkpoint integrity; this generic harness cannot validate a future checkpoint format or prove that the server loaded the checkpoint. Confirm checkpoint use with that pin's own diagnostics. The supplied expected file must cover the entire committed snapshot, including the uncovered tail. This allows a large checkpointed history and independently controlled committed tail, subject to the bounds above.

`legacy-result.json` is one smoke measurement of commit `5421dc5fad38df84f1f185cd6afb89ef83d89057`, using 2,000 rows, a 512 MiB cgroup limit and three seconds of traffic in a privileged Linux arm64 Docker VM. It demonstrates the procedure, not a larger-than-RAM qualification or a portable performance claim. Use a dedicated host, repeat runs, and record host, kernel, filesystem and storage details for comparisons.
