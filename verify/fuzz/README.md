# Backup and retained-history corruption harness

Run from the repository root on Linux with Go 1.27. The harness uses the
public storage APIs. It publishes two checkpoint runs, loads them through
a history catalog, exports a backup with two tail frames, verifies and
restores it, and checks the ordered rows and recovery point.

```sh
go vet ./verify/fuzz
go test -race -count=1 ./verify/fuzz
go test -run '^$' -fuzz '^FuzzBackup$' -fuzztime 30s ./verify/fuzz
go test -run '^$' -fuzz '^FuzzHistory$' -fuzztime 30s ./verify/fuzz
```

The deterministic corpus checks typed integrity rejection, restore into
absent and empty destinations, sibling staging cleanup, and preservation of
an existing restore. The history cases check that a rejected replacement
leaves the current generation and an acquired reader usable and that the
generation drains when the last reader releases it. Some cases repair the
manifest SHA-256, bundle metadata SHA-256, and frame or block CRCs to reach
length, count, and row validation inside these envelopes.

Fixtures allow up to eight files of at most 256 KiB plus one byte each,
including the oversized manifest case. Each deterministic corpus suite
runs in a subprocess with a 45-second parent deadline, a hard address-space
limit 256 MiB above its startup mappings, and a 1 MiB hard per-file write limit.
The relative address-space limit accommodates the race runtime's initial
virtual mappings. The same valid and malformed inputs seed the fuzz targets,
which cap the bytes they materialize on disk.

The architecture registry handoff is `bog/verify/fuzz` with the **test-only**
role and no production imports. The architecture track owns the registry
entry. Send parser regressions, with the corpus name or fuzz reproducer, to
bog-storage.

## WAL recovery and interrupted publication

Run these entry points on each native Linux release runner and retain the
verbose output with the verifier race log.

```sh
go test -race -count=1 -v -timeout=3m -run '^(TestStorageRecovery|TestCheckpointSegmentCeiling|TestInterruptedBackupRetries|TestBackupRetryRestrictiveUmask|TestRetainedExportLifecycle|TestRestoreReplayReservations)$' ./verify/fuzz
go test -run '^$' -fuzz '^FuzzWALRecovery$' -fuzztime 30s -timeout 1m -parallel 2 ./verify/fuzz
go test -run '^$' -fuzz '^FuzzCheckpointPublication$' -fuzztime 30s -timeout 1m -parallel 2 ./verify/fuzz
```

The WAL fixtures come from acknowledged appends. The corpus distinguishes
committed corruption from a suffix beyond the marker, checks recovery offsets
and ordered rows, and checks that failed opens leave disk and query state
intact. The checkpoint corpus covers frame and ordinal rejection, typed staging
admission, and SIGKILL after successor segments are installed but before the
manifest rename. Recovery validates the previous authority, removes losing
files, and then publishes the successor. Both fuzz targets cap suffixes at
8 KiB and share the deterministic frame seeds.

Export and restore each receive three SIGKILLs during staged copying. The log
records the staged files at the interruption point. Competing retries produce
one verified image while preserving the source bundle, the published sibling,
and a separate abandoned claim. These are interruption tests, separate from
the ENOSPC verifier in `verify/linux`.

The umask case runs with an unprivileged effective UID even when launched by
root. Under umask 0777 the baseline records the #69 `EACCES` when reopening a
mode-zero guard. It checks that the abandoned stage survives the refusal, then
repairs the guard and verifies recovery. Once the guard reopens successfully,
the same case verifies recovery without repair.

The deterministic corpus suites use the subprocess limits above. Each
interruption worker has a 12-second deadline and its own address-space
and per-file limits. The test owns the temporary root and reaps the worker
after each SIGKILL.

## Pinned restore and bounded WAL replay

`TestRestoreReplayReservations` runs in the existing `verify/fuzz` race
step on native Linux amd64 and arm64. It uses the two-frame v1 restore
fixture with a deadline and cancels when replay first requests the second
frame, after decoding the first. Cancellation returns no partial result,
restores the budget to its prior usage, and preserves the restore's file
set and contents. A retry on the same budget and reader checks both
logical frame ranges, append-order rows, and the next ordinal.

The caller owns the directory root and keeps the image stable while
`backup.VerifyPinnedRestore` borrows the root. The returned tail reader
has its own lifetime. The caller keeps the tail contents stable through
replay and closes the reader when done. Replay borrows the reader at
absolute WAL offsets and owns its charges until success, when each
frame's reservation passes to the caller with its rows. A retained
reference carries the first frame's charge after the caller releases both
frame reservations. Releasing the final reference returns usage to the
baseline. Send production handoff defects to `bog-storage`, the #53
runtime owner.

## Pinned export across live replacement

`TestRetainedExportLifecycle` runs in the Linux verifier's `verify/fuzz` race
suite. It pauses export after the original manifest reaches staging and before
the first segment opens. While the export holds a history reader and WAL
capture, the owner acknowledges a tied row and a backfill, then publishes and
activates an incremental successor. A fresh history reader sees both later
rows. The original generation continues to occupy its budget slot until the
export returns and the caller releases its references.

The resume case verifies and restores the export, comparing the recovery point,
manifest bytes, WAL tail bytes, and ordered rows against the original capture.
The cancel case requires context cancellation and checks that the empty
destination survives. Both cases check that the stage and its claim records
are removed. The worker uses the same 45-second parent deadline and resource
limits as the deterministic corpus.

## Checkpoint publication at the segment ceiling

`TestCheckpointSegmentCeiling` runs in the existing `verify/fuzz` race
step on native Linux amd64 and arm64. Run the focused case with verbose
output to retain the per-attempt evidence.

```sh
go test -race -count=1 -v -timeout=1m -run '^TestCheckpointSegmentCeiling$' ./verify/fuzz
```

Seed `ceiling-v1` starts with 1,024 valid one-row blocks and a checksummed
manifest bound to an acknowledged WAL frame. Each three-row backfill adds
two identical tied rows and one older row. Publication at the ceiling
must coalesce multiple original blocks with the backfill. Five advances
cover two such repacks, with incremental publications in between.

The test cancels after the first block is staged, exhausts the staging
budget at manifest admission, kills two subprocesses after segment
installation and before the authority rename, and forces the manifest
rename to return ENOENT at the same boundary. Each attempt preserves the
exact predecessor namespace after cleanup. Uncertain attempts fence off
publication and catalog replacement until recovery has checked the old
authority against the reopened or still-live WAL. Logs identify the seed,
phase, file count, bytes, staging usage, pin usage, and published cutoffs.

An acquired reader and a paused scan keep the predecessor readable through
retry and all five advances. The three-capture budget allows both predecessor
readers and one successor reader at a time. Reclamation preserves the
predecessor's segments until the admitted scan finishes, even after both
readers close.
The test compares exact rows and append ordinals, checks that offsets and
ordinals advance, and reopens the WAL and catalog after final reclamation
to verify the settled generation and file set.

The fixture caps each artifact at 256 KiB plus one byte and inventories at
most 1,036 files and 1 MiB of logical file contents, including installed
losing outputs and staging. Publication has a 256 KiB staging budget and
an 8 KiB cumulative segment-write ceiling across the successful advances.
The parent enforces the 45-second corpus deadline. Kill workers reuse the
12-second phase barrier and SIGKILL reaping, and all workers use the
existing relative address-space and per-file write limits. This focused
fixture runs alongside the separate 128 MiB capacity verifier fixtures.

## Peak memory for concurrent retained scans

Run on native Linux amd64 and arm64 and retain both
`retained_scan_memory` JSON records. The test runs one reader and then eight
readers in fresh subprocesses. The record identifies the architecture, Go
version, and race build.

```sh
go test -race -count=1 -v -timeout=2m -run '^TestRetainedScanMemory$' ./verify/fuzz
```

Set `BOG_TEST_CGROUP_PARENT` to a delegated cgroup v2 directory to collect
an isolated `memory.peak`. When omitted, the harness tries the current
cgroup. Unavailable automatic delegation is recorded with a reason.
Explicit delegation must work. The parent seeds the fixture outside the
worker cgroups, so cached fixture pages can be charged elsewhere.

The 10 MiB fixture contains 1,024 overlapping segments, including one line
that fills a maximum-size WAL payload and forces an overflow read. Other
segments contain late appends, duplicates, and tied backfills. The process
has a 45-second parent deadline, a 1 MiB per-file write cap, and bounded
diagnostic output. The scan worker leaves virtual address space available
for the race runtime's shadow mappings. It reports startup virtual size,
peak RSS, and post-GC catalog heap, and verifies a separate 128 MiB startup
Go allocation allowance. Startup ends after the worker loads the catalog
and confirms that it holds one generation pin and two fixture descriptors.
A child failure before readiness is attributed to harness startup. After
readiness, the parent distinguishes reported workload budget violations from
other worker exits and malformed evidence.

Each of four phases starts from a post-GC baseline and has its own 12-second
work deadline and two-second join allowance. With eight readers, each
phase permits 128 MiB of incremental live Go heap at the post-GC barrier,
160 MiB of sampled incremental Go heap, and 192 MiB of cumulative Go
allocations. The single-reader limits are one eighth of those values.
These are Go memory measurements. Process peak RSS and the optional cgroup
peak include startup and are reported separately.

Every reader must reach the first overflow row before proceeding. Success
and retry require all 3,070 rows per reader in exact timestamp, line, and
append-ordinal order. Cancellation requires exactly one row per reader
followed by context cancellation. The parser-limit phase funds the segment
seeks, initial row headers, and first line exactly. Each reader must then
return the scanner's work-exhaustion error before delivering a second
row. Success, cancellation, and retry use 128 Mi work units per reader.

The barrier closes all reader handles while scans still hold their capture
reservations. With eight captures still charged, a ninth must be refused.
Every phase joins the workers and verifies two fixture descriptors, one
catalog pin, and zero remaining captures. Catalog close must drain the
generation, leave zero fixture descriptors and pin reservations, and
restore the fixture's original namespace.

The portable eight-reader race test exercises the same fixture, outcomes,
and capture accounting on macOS. The Linux invocation supplies the procfs
descriptor checks, subprocess startup evidence, and process memory metrics.
A macOS arm64 race run with Go 1.27.1 passed all four phases. The maximum
phase measurements were about 97 MiB of incremental live heap at the barrier,
130 MiB of sampled incremental heap, and 132 MiB of cumulative Go
allocations. Native Linux race evidence is collected by the Linux verifier.

## Bounded replay oracle

The checkpoint recovery and retained export lifecycle suites use a
committed-batch ledger populated from synthetic fixture input and acknowledged
WAL writes. It frames the expected WAL bytes independently and uses the
append-log query model for timestamp and ordinal order. The cases check
duplicate rows, backfill, export and restore bytes, the predecessor after
interrupted publication, and the exact prefix selected by a stale WAL marker.
A truncated checkpoint or tail and stale backup metadata paired with a newer
tail require typed integrity rejection.

The oracle admits at most eight batches, 64 rows, and 16 KiB of framed
input. Artifact observation has a five-second deadline, caps individual files
at 32 KiB and total file bytes at 64 KiB, and verifies rows as they are read.
The Linux worker deadlines and resource limits also apply to these cases.
Portable oracle unit and reader tests run with `go test -race ./verify/fuzz`.

## Malformed retained recovery

Run the deterministic seed 92 corpus with verbose output to collect one
`retained_recovery` JSON record per case.

```sh
go test -race -count=1 -v -run '^TestRetainedMalformedRecovery$' ./verify/fuzz
```

The corpus builds a predecessor and successor over the same committed WAL.
A valid successor is the positive control. Pinned restore rejects truncated
metadata, an end before the checkpoint, mismatched generations, missing
segments, and an invalid length in the second committed frame. A huge
declared tail must be refused by replay admission. Offline verification and
restore must also reject those images. The case with the malformed second
frame additionally checks that suffix replay returns no partial output and
releases its reservations after decoding the first frame.

Interrupted checkpoint recovery can keep the validated predecessor when a
damaged successor manifest has yet to become authoritative. It removes the
owned losing segment and staging and preserves the unclaimed segment. A
damaged or missing committed manifest, or a truncated ownership intent,
requires rejection and preservation of the candidate namespace. The oracle
checks the predecessor's checkpoint rows, exact WAL suffix frames, and
merged timestamp and append-ordinal order. Restore refusals also exercise an
attempt to overwrite the prior valid image and verify its bytes and replay.

The recovery-view cases feed the same pinned manifest, segments and suffix
to checkpoint advancement and backup export. They shorten the manifest or
second frame, corrupt its length or CRC, append bytes past the committed end,
and change the declared suffix digest. The two APIs report their own integrity
categories, and a physically short suffix stream reports unexpected EOF.
The damaged disk images also exercise pinned backup verification and offline
open. Deterministic cancellation after the second frame header and an injected
suffix close error exercise reader ownership. Each failure is followed by a
valid read using the same publisher or export destination. The successful
checkpoint advance and backup walk must both match the ledger's exact order.
Workers check staging, reservation usage and transferred descriptors, and
Linux workers also inventory fixture descriptors through procfs.

Each case owns a fresh worker and private fixture. The worker has a five-second
case deadline, a 16 MiB cumulative Go allocation ceiling and a 16 KiB replay
admission limit. The parent enforces a ten-second subprocess deadline and
removes its scratch directory after reaping the worker. Linux workers also
use the corpus address-space and per-file limits. Each verdict records the
validated generation and replay boundary, row and suffix-frame counts, time,
allocation, and fixture cleanup. In rejection cases the reported generation
belongs to the independently replayed predecessor. Recovery-view verdicts
record the common input generation and both reader rejection categories.
