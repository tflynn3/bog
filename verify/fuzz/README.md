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
including the oversized manifest case. Each deterministic suite runs in
a subprocess with a 45-second parent deadline, a hard address-space limit
256 MiB above its startup mappings, and a 1 MiB hard per-file write limit.
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
go test -race -count=1 -v -timeout=3m -run '^(TestStorageRecovery|TestInterruptedBackupRetries|TestBackupRetryRestrictiveUmask|TestRetainedExportLifecycle)$' ./verify/fuzz
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

The deterministic suites use the subprocess limits above. Each interruption
worker has a 12-second deadline and its own address-space and per-file limits.
The test owns the temporary root and reaps the worker after each SIGKILL.

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
