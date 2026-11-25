# bog

Bog is a durable raw-log service for Linux. Build with Go 1.27:

```sh
go build ./cmd/bog
mkdir ./data
./bog -data-dir ./data -addr 127.0.0.1:8080
```

`-data-dir` must be an existing directory; `-addr` defaults to `127.0.0.1:8080`. A process holds an exclusive lock on the directory until exit. `GET /healthz` returns 200 once the WAL has been replayed and the server is listening.

```sh
curl -X POST localhost:8080/ingest --data-binary $'{"ts":"2025-01-01T00:00:00.123456789Z","line":"hello"}\n'
curl 'localhost:8080/query?start=2025-01-01T00:00:00Z&end=2025-01-02T00:00:00Z&contains=hello&limit=10'
```

Ingest accepts at most 4 MiB of UTF-8 NDJSON. Each nonblank line must be an object containing exactly one string `ts` and one string `line`; a final newline is optional. Timestamps use RFC3339 with an explicit zone and at most nine fractional digits; UTC instants must fall in years 0001–9999. The final nanosecond of year 9999 is reserved for an exclusive query end. JSON Unicode escapes must use paired UTF-16 surrogates; unpaired surrogate escapes anywhere in a batch are rejected. Literal U+FFFD and escaped backslashes are accepted. Historical WAL records retain their original decoding behavior. The whole batch is validated before writing. Invalid or empty batches return 400, oversized bodies return 413, and successful durable batches return 204.

Query requires `start` and `end` timestamps and accepts a literal case-sensitive `contains` and decimal `limit` (default 500, range 1–1000). The interval is `[start,end)`; equal bounds return no rows. Output is UTF-8 `application/x-ndjson`, in UTC, ordered by timestamp then commit and row order. Duplicate records remain distinct. Queries see complete batches.

A single append-only `bog.wal` and its `bog.commit` marker are the source of truth; keep them together. Create the data directory on a local filesystem before starting the service, and ensure its parent directory is itself durably established (including any newly created ancestors). After the eight-byte `BOGWAL01` magic, each batch has a 16-byte little-endian header: payload length, its bitwise complement, payload CRC32C, and CRC32C of the first 12 header bytes, followed by the original NDJSON payload. The 24-byte marker contains `BOGCMT01`, the little-endian committed offset, and its bitwise complement. Successful responses follow WAL sync, an atomically replaced and directory-synced commit marker, and in-memory publication. Recovery discards any bytes beyond the committed offset; corruption within the committed prefix prevents startup. Startup rebuilds memory from the committed WAL. A missing or invalid marker on a populated WAL, or a marker without a WAL, prevents startup; only a new store or a magic-only WAL may bootstrap without a marker.

Decoded rows and a derived UTC minute-bucket index remain in memory, so retained data is bounded by available RAM. Queries touching at most 32 minute buckets use the index; wider ranges scan all rows. The index is rebuilt from the WAL on startup and does not change the on-disk format. Up to 16 ingest requests can be processed concurrently, and additional requests receive 503. A failed append returns 503 only after rollback is synced and publishes no rows. If a commit or rollback cannot be confirmed, the process terminates and the request outcome is unknown. A lost response can result in duplicates on retry. The data directory must be owned by the service user and must not be group- or world-writable; existing WAL and marker files must also be owned by that user, not group- or world-writable, and have exactly one link. All parent directories must be trusted against replacement or renaming by other principals. The local filesystem and storage stack must support exclusive advisory locks, atomic rename, file sync, directory sync, and powersafe overwrite: a write interrupted by power loss must not damage bytes outside the written range, even within the same sector. Consecutive WAL frames may share a sector, so this guarantee is required to preserve previously acknowledged batches.
