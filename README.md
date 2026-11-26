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

On the legacy layout, keep the append-only `bog.wal` and its `bog.commit` marker together. They are the recovery authority for that layout. Create the data directory on a local filesystem before starting the service, and ensure its parent directory is itself durably established (including any newly created ancestors). After the eight-byte `BOGWAL01` magic, each batch has a 16-byte little-endian header: payload length, its bitwise complement, payload CRC32C, and CRC32C of the first 12 header bytes, followed by the original NDJSON payload. The 24-byte marker contains `BOGCMT01`, the little-endian committed offset, and its bitwise complement. Concurrent admitted batches can share a WAL sync and one atomically replaced and directory-synced commit marker; each retains its own frame and is published as a whole in admission order. Successful responses follow the durable marker and in-memory publication. Recovery discards any bytes beyond the committed offset; corruption within the committed prefix prevents startup. Startup rebuilds memory from the committed WAL. A missing or invalid marker on a populated WAL, or a marker without a WAL, prevents startup; only a new store or a magic-only WAL may bootstrap without a marker.

On the legacy layout, decoded rows and a derived index of immutable sorted ordinal runs remain in memory, so retained data is bounded by available RAM. Each published batch is sorted by UTC timestamp and append order, then merged into runs by batch count. Queries seek within each run and merge matching rows in order, including across wide time ranges. On startup, recovery represents a timestamp-ordered WAL prefix as an implicit run without an ordinal array; otherwise it builds one explicitly sorted ordinal run; live batches remain in separate merge levels. The index does not change the on-disk format. Up to 16 ingest requests can be processed concurrently, and additional requests receive 503. A failed append returns 503 only after rollback is synced and publishes no rows. If a commit or rollback cannot be confirmed, the process terminates and the request outcome is unknown. Once admitted, an append completes even if its client disconnects. A lost response leaves the outcome unknown and retrying can produce duplicates. The data directory must be owned by the service user and must not be group- or world-writable; existing WAL and marker files must also be owned by that user, not group- or world-writable, and have exactly one link. All parent directories must be trusted against replacement or renaming by other principals. The local filesystem and storage stack must support exclusive advisory locks, atomic rename, file sync, directory sync, and powersafe overwrite: a write interrupted by power loss must not damage bytes outside the written range, even within the same sector. Consecutive WAL frames may share a sector, so this guarantee is required to preserve previously acknowledged batches.

Sealed groups of 128 append ordinals also have derived Bloom filters over overlapping four-byte grams of each decoded line. Each bitmap uses 4 KiB (about 32 bytes per row, plus small metadata); blocks with more than 64 KiB of decoded text and the unsealed tail remain unfiltered. Wide literal searches may use these filters to skip blocks, but always check candidate lines exactly. Filters are rebuilt on replay and do not alter durable files or snapshot pagination.

For a stable walk, add `page=1` to the first query. If another matching row exists, the response includes `Bog-Next-Cursor`; send its value as `cursor` on the next request, repeating `start`, `end`, and `contains`. For example:

```sh
curl -i 'localhost:8080/query?start=2025-01-01T00:00:00Z&end=2025-01-02T00:00:00Z&page=1&limit=10'
curl -i 'localhost:8080/query?start=2025-01-01T00:00:00Z&end=2025-01-02T00:00:00Z&cursor=TOKEN&limit=20'
```

The cursor captures published rows at the first request; later appends do not appear in that walk. Rows with identical timestamps retain append order, including duplicates. Bounds may use equivalent time zones, `contains` must be identical (omission means empty), and `limit` may change between requests. The final page has no cursor header. Cursors are authenticated, expire when the process restarts, and invalid or mismatched cursors return 400. `page` accepts only `1`, and cannot be combined with `cursor`; an empty cursor is invalid. First-page requests must leave room within the 1 MiB request header limit for a continuation cursor and ordinary headers; oversized queries return 400. The encoded NDJSON body is limited to 32 MiB by default; set `-query-max-response-bytes` to change it. The limit includes JSON escaping and each record’s newline. An ordinary query may return a short 200 response without a cursor when the byte limit is reached. If the first matching record cannot fit, the server returns 422; increase the limit to retrieve it. Use `page=1` and follow `Bog-Next-Cursor` as shown above to retrieve the full snapshot. See [query response limits](docs/query-response-limits.md).

## Retained-history cutover

The Cycle 42 cutover serves immutable checkpoints and a bounded committed
tail under one writer. The pinned base still serves the legacy layout and
rejects restored images during startup. The steps below require a
cutover binary qualified by the pinned Linux release gate. See the
[cutover decision](docs/adr/0004-retained-history-cutover.md).

### Activate a verified restore

Use the backup engine's `Verify` and `Restore` operations to check the
bundle and publish a separate, service-owned directory on a trusted local
filesystem. Keep the original bundle for recovery. The restored directory
contains `bog.backup`, `bog.checkpoint`, its referenced segments, and
`bog.wal.tail`. Point a cutover-capable binary at that directory using the
usual startup command. See the [backup format](internal/backup/FORMAT.md).

```sh
./bog -data-dir /path/to/restored-data -addr 127.0.0.1:8080
```

The server locks the directory, validates the restored checkpoint and
committed suffix, and durably publishes writable recovery authority
before opening the listener. Keep legacy `bog.wal` and `bog.commit` files
out of the unactivated image. With a valid active authority, restart on
the same service directory to recover the checkpoint, restored suffix,
and subsequent acknowledged appends. Keep that whole directory together.
Use a fresh `page=1` walk after restart.

If activation is interrupted, restart on that directory with the same
cutover binary so it can validate and recover the publication state.
On a startup error for an incomplete, corrupt, or ambiguous layout,
preserve the service directory and diagnose the reported error. Restore
the original bundle into a separate destination if a fresh copy is
needed. The original bundle represents its captured cutoff, so replacing
an active directory with it can lose subsequent acknowledged appends. See
the [writable-restore decision](docs/adr/0005-writable-restore-authority.md).

### Page through retained history

A first page pins one generation and its committed tail. Its cursor keeps
reading that view through generation replacement while the capture is live.
An expired capture on its issuing process returns 410. A cursor from
before restart returns 400 because the signing key changes. Start a fresh
walk with `page=1` in either case. A byte-limited page resumes just after
its last emitted row. If the work budget prevents the server from preparing a
page, it returns 503 with no rows or next cursor. Retry a continuation with
the cursor from the original request while its capture is live. Lowering
`limit` can help when a page has many matches. A sparse scan may require a
larger server work budget.

### Opt in to capacity verification

Set `BOG_CAPACITY_CONTROL=1` on a cutover binary to enable
`/_capacity/compaction` on a loopback-bound listener. The control defaults
to off. Use a disposable local data directory for the Linux capacity fixture.

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
