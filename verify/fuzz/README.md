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

Run on native Linux amd64 and arm64 and retain the
`retained_scan_worker` and `retained_scan_memory` JSON records from
each run.

```sh
BOG_TEST_CGROUP_PARENT=/sys/fs/cgroup/path/to/delegation \
  go test -race -count=1 -v -timeout=2m -run '^TestRetainedScanMemory$' ./verify/fuzz
```

Each race run starts three fresh processes: one with one reader and two
with eight readers. The worker sends a bounded readiness record before
loading the fixture. The parent verifies its PID, architecture, Go version,
race mode and concurrency, then admits the worker and releases it through
a start pipe. The parent enforces separate startup and process deadlines
and waits for the process to exit on every path. Startup, sanitizer mapping,
reader budget and other reader failures have distinct outcomes.

The cgroup v2 parent must have the memory controller enabled for its
children and permit process migration. If the variable is omitted, the
harness attempts to use its current cgroup and reports why an isolated peak
is unavailable when delegation or `memory.peak` is missing. An explicit
parent is required to work. Each child cgroup contains only the worker from
reader admission through exit. The parent reads its peak after reaping the
worker and checking that the group is empty. Fixture pages charged during
seeding can remain outside the group.

The fixture caps disk at 10 MiB and uses 1,024 overlapping segments. The
first ordered row forces every worker to materialize an overflow line.
Other segments contain late appends and identical tied backfills. Each scan
has a query-work budget, the catalog bounds live captures and generation
pins, and the report checks the sampled heap increment against 16 MiB per
admitted reader. The catalog baseline and the retained-heap samples use
forced collections. The sampled heap peak can include garbage awaiting GC.

The race scan worker allows sanitizer mappings to grow during the workload.
Its startup record captures process virtual size and RSS before cgroup
migration, and the workload report captures both again after scan
cancellation, alongside the process-wide `VmHWM`. The isolated cgroup
peak measures charges to the child cgroup, including any instrumentation
allocated there. The composed service has its separate 128 MiB,
swap-disabled release qualification. Ordinary scan workers and the other
corpus workers retain their address-space and file-size limits.

The scan barrier closes reader handles while the active scans still hold
their reservations. Cancellation joins the scans under a deadline. The
worker checks capture and descriptor baselines, retries the query in exact
timestamp and append-ordinal order, then probes capture exhaustion, query
work exhaustion and cancellation partway through reader acquisition.
Closing the catalog must drain the generation and return pin and fixture
descriptor counts to their initial values.

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
