# Checkpoint backup

Build and run the backup command on Linux.

```sh
go build -o bog-backup ./internal/backup/cmd/bog-backup
./bog-backup export /srv/bog /srv/backups/bog-001
./bog-backup verify /srv/backups/bog-001
./bog-backup restore /srv/backups/bog-001 /srv/bog-restored
```

Use a destination whose parent already exists and is under your control. The
destination may be absent or empty. Keep the export directory immutable while
verifying or restoring it. Export reads a running or stopped store using its
on-disk commit protocol.

## Recovery point and format

The reported committed offset is the exclusive byte boundary of the captured
`bog.commit` marker. It identifies whole WAL frames, including any durable ingest
whose HTTP response was still in flight. The manifest digest identifies the
selected checkpoint generation. The checkpoint's covered offset separates its
rows from the uncovered WAL suffix. Offsets include the eight-byte WAL magic.

The export contains the exact manifest and its referenced immutable segments,
the commit marker, and the WAL through the captured boundary. The v1 checkpoint
binds itself to the SHA-256 of its covered WAL prefix. That prefix remains in the
export because the pinned store's startup reads the legacy WAL and the v1
checkpoint validator checks the prefix. A checkpoint-aware reader uses the
covered offset to select the suffix when combining WAL frames with segments.

The fixed 80-byte `bog.backup` index contains the eight-byte `BOGBAK01` version
magic, the committed offset as an unsigned little-endian 64-bit integer, the
32-byte WAL SHA-256, and the 32-byte manifest SHA-256. A zero manifest digest
represents a legacy WAL-only snapshot. The manifest checksum covers segment
names, sizes, and digests. The verifier checks segment contents and ordinals,
the WAL header and payload CRC32C checks, the covered-prefix digest, and the
exact end-of-file boundary. Restore copies the store files and omits the
export index.

## Publication

The exporter pins the source directory and opened files. It samples the atomic
commit marker, syncs the source directory, and checks that the WAL and manifest
names still identify the sampled generation. It relies on the v1 writer's
append-only committed WAL prefix and immutable segments. If a replacement
retires a segment during export, an observed manifest change triggers a bounded
retry. A missing segment in the same manifest is an integrity failure. Orphan
source files are ignored.

Export and restore sync each output file and the staging directory before
atomically installing the complete directory. They then sync the parent. A
failure before install leaves the destination reusable. If install or its
parent sync fails, stop and inspect the destination. The attempted rename
may already have installed the full directory. A crash can leave a hidden
`.bog-backup-stage-*` directory. Treat it as an orphan only after confirming no
backup or restore is still using it.

## Memory

Validation walks one segment at a time without a cache or merge. Peak live
working memory is the capped 256 KiB manifest plus its decoded metadata, a
bitmap of at most `ceil(MaxSegments * floor((MaxBlockSize - 44) / 24) / 8)`
bytes, and the larger of one block plus its decoded rows or one 4 MiB WAL
payload plus its decoded rows and decoder scratch. The manifest allows up to
1,024 segments. The bitmap is at most about 22 MiB. Verification hashes the
covered prefix and the remainder in one pass. Memory stays bounded as the WAL
grows and when the input is shuffled. Store restart at this revision still uses
the legacy in-memory recovery builder.

## Linux smoke measurements

One Linux arm64 container run with Go 1.27 used a 32,768-row checkpoint
followed by 512 durable HTTP batches of 128 rows with four concurrent
clients. Export ran during those appends and captured 33,024 rows, including
256 rows beyond the checkpoint. The restored store answered 100 literal
queries limited to 100 rows.

| Measurement | Observed |
| --- | ---: |
| Restart to first successful health probe | 202.3 ms |
| Peak RSS at restart | 18,816 KiB |
| Peak RSS during CLI restore | 18,432 KiB |
| Durable ingest throughput over the export and append run | 62,793 rows/s |
| Durable ingest p99 | 18.4 ms |
| Restored query p95 | 2.24 ms |
| Bog process write bytes per accepted payload byte | 1.21× |
| Logical archive bytes per committed WAL byte | 1.83× |

Linux procfs supplied the restart memory and ingest write-byte counters.
The restore peak is the child process's maximum RSS. These are smoke
measurements for this fixture and environment.
