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

Run on native Linux amd64 and arm64 and retain the two
`retained_scan_memory` JSON records from the verbose test output.

```sh
BOG_TEST_CGROUP_PARENT=/sys/fs/cgroup/path/to/delegation \
  go test -race -count=1 -v -timeout=2m -run '^TestRetainedScanMemory$' ./verify/fuzz
```

The parent must have the cgroup v2 memory controller enabled for its
children and permit creating and entering a child cgroup. If the variable
is omitted, the harness tries the current cgroup as the parent. On hosts
without delegation or `memory.peak`, the report records a null cgroup peak
and a reason. An explicitly requested cgroup must work. Each of the two
fresh workers enters its own cgroup before loading the catalog. The
parent seeds the shared fixture outside those cgroups, so filesystem
cache charged during seeding can remain outside the measured groups.
The harness removes each group after its worker exits.

The fixture uses 1,024 overlapping segments within a 10 MiB disk budget.
Its first ordered row contains the largest unescaped line that fits a
single WAL batch. It spans the scan read page and forces overflow on each
worker. The other segments contain late appends and identical tied
backfills. Each concurrency case uses the same on-disk generation,
measuring once with a single scan and once with the server's configured
default concurrency. All visitors hold the maximum-size line together.

Process peak RSS comes from `VmHWM` and the isolated cgroup peak comes
from `memory.peak`, both read after cancellation and before the exact
retry. These high-water marks include catalog load. The heap baseline is
the loaded catalog after a forced GC. The harness samples Go's
`HeapAlloc` during scan startup and at the overflow barrier and reports
the largest observed increment above baseline. In between collections,
`HeapAlloc` can include objects awaiting collection. The separate
post-GC heap samples at the barrier and after cancellation show retained
heap with and without active scan state.

The barrier closes reader handles while scan reservations are still
charged. Cancellation joins the scans under one deadline and checks that
capture reservations and fixture descriptors return to their catalog
baseline. A fresh reader verifies every timestamp, line and append
ordinal in order. Closing the catalog releases the generation pin and
its reservations and descriptors.
