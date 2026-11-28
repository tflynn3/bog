# bog

Bog is a durable raw-log service for Linux. Build with Go 1.27:

```sh
go build ./cmd/bog
mkdir -m 700 ./data
./bog -data-dir ./data -addr 127.0.0.1:8080
```

`-data-dir` must be an existing directory. `-addr` defaults to `127.0.0.1:8080`. The standalone server has no authentication or per-stream isolation. Anyone who can reach it can ingest and query all data. When binding to a non-loopback address, restrict access to a trusted network or use an authenticated access layer. A process holds an exclusive lock on the directory until exit. `GET /healthz` returns 200 once the WAL has been replayed and the server is listening.

```sh
printf '%s\n' '{"ts":"2025-01-01T00:00:00.123456789Z","line":"hello"}' | curl -X POST localhost:8080/ingest --data-binary @-
curl 'localhost:8080/query?start=2025-01-01T00:00:00Z&end=2025-01-02T00:00:00Z&contains=hello&limit=10'
```

Ingest accepts at most 4 MiB of UTF-8 NDJSON. Each nonblank line must be an object containing exactly one string `ts` and one string `line`; a final newline is optional. Timestamps use RFC3339 with an explicit zone and at most nine fractional digits; UTC instants must fall in years 0001–9999. The final nanosecond of year 9999 is reserved for an exclusive query end. JSON Unicode escapes must use paired UTF-16 surrogates; unpaired surrogate escapes anywhere in a batch are rejected. Literal U+FFFD and escaped backslashes are accepted. Historical WAL records retain their original decoding behavior. The whole batch is validated before writing. Invalid or empty batches return 400, oversized bodies return 413, and successful durable batches return 204.

Query requires `start` and `end` timestamps and accepts a literal case-sensitive `contains` and decimal `limit` (default 500, range 1–1000). The interval is `[start,end)`; equal bounds return no rows. Output is UTF-8 `application/x-ndjson`, in UTC, ordered by timestamp then commit and row order. Duplicate records remain distinct. Queries see complete batches.

Bog validates and opens writable storage before serving. Existing ordinary
directories use `bog.wal` and its `bog.commit` marker. Startup streams validation
of the committed frames, and larger ordinary WALs migrate to retained history
before listening. Each frame preserves the original NDJSON payload. The marker
records the durable byte boundary. A populated ordinary WAL requires a valid
marker.

During normal ingest, Bog advances checkpoints once the uncovered suffix reaches
16 MiB or 4,096 batches. The durable `bog.authority` record selects the checkpoint
generation and its writable WAL tail together. Queries read the selected
checkpoint segments and committed tail from disk. Keep the entire directory
together. Restart verifies the selected files and committed frames before
discarding uncommitted trailing bytes. Previous generations and tails can stay
on disk until their readers drain.

Concurrent admitted batches can share a WAL sync and one durable commit record
update. Bog makes each complete batch visible in admission order before returning
204. Up to 16 requests can ingest concurrently, subject to aggregate memory
admission. An append refused for disk exhaustion returns 503 with `disk full`
after rollback and sync. Health and queries remain available, and ingest can
resume when space is freed. An unconfirmed commit or rollback terminates the
process with the request outcome unknown. An admitted append can complete after
the client disconnects, and retrying a lost response can produce duplicates.

The data directory and storage files must be owned by the service user and must
be protected from writes by other users. Writable storage files must be regular
files with exactly one link. Use a trusted local filesystem and parent directory.
The storage stack must support exclusive advisory locks, atomic rename, file and
directory sync, and powersafe overwrite. Establish the directory durably before
startup. On SIGTERM the server drains HTTP, releases query captures, and drains
storage maintenance before releasing the lock.

For a stable walk, add `page=1` to the first query. If another matching row exists, the response includes `Bog-Next-Cursor`; send its value as `cursor` on the next request, repeating `start`, `end`, and `contains`. For example:

```sh
curl -i 'localhost:8080/query?start=2025-01-01T00:00:00Z&end=2025-01-02T00:00:00Z&page=1&limit=10'
curl -i 'localhost:8080/query?start=2025-01-01T00:00:00Z&end=2025-01-02T00:00:00Z&cursor=TOKEN&limit=20'
```

The cursor captures published rows at the first request; later appends do not appear in that walk. Rows with identical timestamps retain append order, including duplicates. Bounds may use equivalent time zones, `contains` must be identical (omission means empty), and `limit` may change between requests. The final page has no cursor header. Cursors are authenticated, expire when the process restarts, and invalid or mismatched cursors return 400. `page` accepts only `1`, and cannot be combined with `cursor`; an empty cursor is invalid. First-page requests must leave room within the 1 MiB request header limit for a continuation cursor and ordinary headers; oversized queries return 400. The encoded NDJSON body is limited to 32 MiB by default. Set `-query-max-response-bytes` to lower the per-response cap. Queries share an aggregate 32 MiB encoded-response budget. The limit includes JSON escaping and each record’s newline. An ordinary query may return a short 200 response without a cursor when the byte limit is reached. If the first matching record cannot fit, the server returns 422; increase the limit to retrieve it. Use `page=1` and follow `Bog-Next-Cursor` as shown above to retrieve the full snapshot. See [query response limits](docs/query-response-limits.md).

## Retained-history cutover

The default Linux server supports the [retained-history operating contract](docs/adr/0035-tail-only-retained-recovery-contract.md).

### Activate a verified restore

Use the backup engine's `Verify` and `Restore` operations to check the
bundle and publish a separate, service-owned directory on a trusted local
filesystem. Keep the original bundle for recovery. The restored directory
contains `bog.backup`, `bog.checkpoint`, its referenced segments, and
`bog.wal.tail`. Point Bog at that directory using the usual startup command. See the [backup format](internal/backup/FORMAT.md).

```sh
./bog -data-dir /path/to/restored-data -addr 127.0.0.1:8080
```

The server locks the directory, validates the restored checkpoint and
committed suffix, and durably activates writable recovery before opening
the listener. An incomplete, corrupt, or ambiguous layout fails startup.
Keep legacy WAL and commit files out of the unactivated image. After
activation, start the server on the same directory on restart and use a
fresh `page=1` request to walk the recovered view. New acknowledged
appends survive restart alongside the restored rows.

### Page through retained history

A first page pins one generation and its committed tail. Its cursor keeps
reading that view through generation replacement while the capture is live.
An expired capture returns 410. After expiry or restart, start a fresh walk
with `page=1` on the serving process. A byte-limited page resumes just after
its last emitted row. If the work budget prevents the server from preparing a
page, it returns 503 with no rows or next cursor. Retry a continuation with
the cursor from the original request while its capture is live. Lowering
`limit` can help when a page has many matches. A sparse scan may require a
larger server work budget.

### Opt in to capacity verification

Set `BOG_CAPACITY_CONTROL=1` to enable `/_capacity/compaction` on an IPv4
loopback listener. In this mode the fixture drives checkpoint cutoffs explicitly. Use a disposable local data directory for the Linux capacity fixture.

```sh
BOG_CAPACITY_CONTROL=1 ./bog -data-dir /path/to/disposable-data -addr 127.0.0.1:8080
```

The capacity runner sets the opt-in for both live starts and removes any
inherited opt-in in legacy mode. Live qualification checks asynchronous
checkpoint replacement during ingest and cursor resume, then checks
complete query walks after restart at both fixture sizes. It requires
delegated cgroup v2 and cleanup of candidate processes and disposable
cgroups. The Linux release gate collects capacity, admission, durability,
and restored-parity reports from the same pinned binary. See the
[capacity runner](bench/retained-history/README.md) and
[Linux release gate](verify/release/README.md).

## Legacy performance measurements

A same-host Linux comparison against `25d0c0a` used the verifier's seeded 1M-row backfill fixture (500 rows per batch, four writers; one run per version). Live query p95 was 44.95 → 5.66 ms for dense wide ranges and 385.19 → 374.94 ms for rare/absent matches; ingest request p99 was 414.56 → 314.63 ms, peak RSS 316,660 → 317,280 KiB, and restart-to-health 8,874 → 7,883 ms. These single-run measurements are workload- and host-dependent.

A paired Linux arm64 Docker smoke run against `4ff49aa` used the verifier's seeded 1M-row backfill fixture (500 rows per batch, four writers, one run per version). Baseline → filtered live query p95 was 0.835 → 0.582 ms for broad, 0.329 → 0.142 ms for narrow, and 44.578 → 0.987 ms for rare/absent queries; after restart it was 0.649 → 0.577, 0.257 → 0.162, and 44.803 → 0.963 ms respectively. Durable ingest was 651,832 → 596,752 rows/s with request p99 8.406 → 8.551 ms; peak RSS was 319,848 → 421,100 KiB and restart-to-health 1,569 → 1,784 ms. These are single-run, host- and workload-dependent measurements.

Cycle 11 local benchmark record (Linux ARM64, Go 1.27, 3 runs, 1s per run; times in ns/op):

| Benchmark | 81f04a2 | Implicit ordered recovery |
| --- | --- | --- |
| Replay/steady | 3,198,959; 3,362,445; 3,718,715 | 3,580,665; 3,318,212; 3,300,481 |
| Replay/shuffled | 5,947,550; 4,980,046; 4,731,645 | 4,357,969; 4,334,454; 4,529,399 |
| RestoredSingleRun/dense | 1,856; 1,966; 1,945 | 1,900; 1,923; 2,091 |
| RestoredSingleRun/rare | 13,048; 12,764; 13,817 | 15,833; 16,165; 15,097 |
| RestoredSingleRun/absent | 7,657; 7,662; 7,588 | 7,473; 7,290; 7,338 |
| RestoredSingleRun/dense_to_sparse | 14,058; 15,897; 17,009 | 13,775; 13,412; 13,775 |

Replay steady allocation fell from about 3,848,260 to 3,553,570 bytes/op; shuffled fell from about 3,848,325 to 3,635,570 bytes/op. These microbenchmarks do not measure the production p95 gate.
