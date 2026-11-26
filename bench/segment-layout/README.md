# Checkpoint block layout benchmark

Run from the Bog checkout on Linux with the Go version in `go.mod`.

```sh
go run ./bench/segment-layout -rows 50000 -block-rows 512 -dir /path/to/test-filesystem
go vet ./bench/segment-layout
go test -race ./bench/segment-layout
```

Omit `-dir` to use the OS temporary directory. The command creates a private
temporary directory there and removes it on exit. Record the host, kernel,
filesystem, and storage device with the JSON output when comparing runs.

The deterministic `synthetic-retained-log-v1` fixture contains repeated
HTTP-style log lines with shuffled timestamps, tied timestamps, duplicate rows,
UTF-8 text, both codec timestamp extremes, and a singleton line decoded from a
maximum-size Bog batch. The large line exceeds the summary budget and has a
literal at its end. Every layout gets the same append ordinals and block
boundaries. The fixture's repetitive text favors compression. Absolute sizes
and timings depend on the log mix.

Each query is compared row for row with an independent fixture scan sorted by
UTC instant and append ordinal. Time ranges are half-open and literal matching
uses bytes. The full verifier runs after each layout's measured queries. It
decodes every block, checks persisted summaries against the decoded rows, and
compares the complete block contents with the fixture.

## Measurements

- **Retained bytes** is the synced file's logical size. **Rewrite bytes** counts
  bytes accepted by writes during one complete materialization of the retained
  fixture into a fresh file. Both include framing and summaries. Filesystem
  allocation, device write amplification, and incremental compaction costs are
  outside this measurement.
- **Blocks read** counts payloads fetched and decoded. Every query reads and
  validates the framing and summaries, including for skipped payloads.
- **First-read latency** is one query per layout and probe, timed from before
  reading the file header through result sorting. File creation, sync, cache
  preparation, fixture comparison, and full verification are outside the timer.
  Before each query Linux amd64 and arm64 request `POSIX_FADV_DONTNEED` on the
  synced benchmark file. Success is advisory and the report marks residency
  unverified. Failed and unavailable advice are reported as potentially warm.
  The command leaves system-wide caches alone.
- **Peak tracked decode bytes** is the largest per-block sum of the stored
  buffer, a separate uncompressed buffer when needed, decoded row structs, and
  decoded line bytes. A query that prunes every payload reports zero. Only one
  block is decoded at a time. The scanner separately uses fixed-size framing
  buffers and a 1 KiB summary buffer. The counter excludes zlib workspace, the
  fixture, retained query results, allocator overhead, and OS cache. For
  checkpoint block limit `B`, the counter is bounded by the stored cap
  `B + 64 KiB`, plus `B` for decompression output, at most `B` copied line
  bytes, and at most `floor((B - 44) / 24)` decoded row structs.

## Benchmark file format

The benchmark envelope is versioned by the 8-byte `BOGBEN01` magic. All integers
are unsigned 32-bit little-endian. The file header stores magic, flags (bit 0
for zlib and bit 1 for summaries), block count, and CRC32C of the preceding
header fields. Every block stores the following fields in order:

1. Stored payload length, decoded payload length, summary byte length, stored
   payload CRC32C, then metadata CRC32C.
2. Summary bytes. The metadata checksum covers the four preceding fields and
   these bytes. Summaries contain the sorted, distinct four-byte windows of
   each line, interpreted as little-endian integers. Zero length represents an
   empty summary. A length of `0xffffffff` represents no summary and consumes
   zero bytes. A block exceeding 256 distinct windows uses this marker.
3. Stored payload. This contains a raw Bog checkpoint block or a single zlib
   stream containing that block, encoded with Go zlib’s default compression
   level. The Bog codec supplies the row metadata,
   inclusive time extrema, timestamp precision, and its own checksums.

The reader bounds summary and payload lengths against format caps and remaining
file bytes before reading. It checks metadata CRCs before pruning and validates
the stored payload CRC before allocating decompression output. Compressed
payloads must reach the declared length and end with a valid zlib trailer at the
end of the stored bytes. A summary may skip a payload only when a probe of at
least four bytes has a window missing from the summary. Candidate rows always
undergo exact matching.

## Example run

One default run on Ubuntu 24.04.5 arm64, Linux 6.8.0-100-generic, Go 1.27.0,
and container overlayfs produced the following results for 50,001 rows in 99
blocks. File-local cache advice succeeded and page residency was unverified.
The timings are single observations.

| Layout | Retained and rewrite bytes (each) | Absent payloads read | Absent first read (ms) | Absent tracked decode bytes |
| --- | ---: | ---: | ---: | ---: |
| Raw | 13,100,383 | 99 | 18.37 | 8,388,646 |
| Compressed | 343,964 | 99 | 18.34 | 8,397,968 |
| Summary | 13,138,135 | 1 | 6.43 | 8,388,646 |
| Compressed + summary | 381,716 | 1 | 4.74 | 8,397,968 |

The oversized block accounts for the one absent payload read after pruning and
sets the tracked decode peak for all four layouts. The rare probe read three
payloads with summaries, including the oversized block.
