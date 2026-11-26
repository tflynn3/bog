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
