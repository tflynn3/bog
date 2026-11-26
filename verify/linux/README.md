# Linux durability verifier

`durability.py` exercises an already-built Linux Bog binary through HTTP. It requires Python 3, `stat`, and an **empty, dedicated tmpfs mount** owned and writable by the service user. The verifier fills that entire filesystem and removes its own files; never point it at a shared mount. Run on local storage with the filesystem guarantees described in the repository README. For example, in a Linux container with a private 16 MiB tmpfs mounted at `/disk`:

```sh
python3 verify/linux/durability.py --binary /path/to/bog \
  --revision FULL_40_CHARACTER_COMMIT_SHA --sha256 EXPECTED_BINARY_SHA256 \
  --enospc-mount /disk --report /path/to/durability.json
```

Build the executable once from the pinned revision and calculate the expected SHA-256 independently; use the same binary and digest for all release-gate verifiers. The revision is a caller-supplied attestation, not something the verifier can derive from an arbitrary executable. The release workflow must verify the checkout/build revision and compare the report's `revision` and `binary_sha256` with its own pinned values. The verifier does not build or replace the binary.

The report is JSON, also printed to stdout. Its top-level `revision`, `binary`, `binary_sha256`, `host`, `filesystem`, `status`, and `cases` identify the run. Required cases are `sigkill`, `recovery`, and `enospc`; each has a `status` of `pass` or `fail` and failed cases include `error`. A setup error produces top-level `error`. A successful exit and top-level `pass` require every case to pass; missing or failed reports must fail the release gate. Invalid arguments can prevent report creation and must also be treated as failure. This verifier does not cover the separate query-admission, scan-budget, or capacity gate cases.

The recovery case checks acknowledged rows, rejected invalid input, tied backfill and ordered cursor walks, stale cursors after restarts, torn uncommitted WAL suffix recovery, and byte-for-byte preservation of corrupted committed WAL and marker files. The SIGKILL case checks an interrupted batch is recovered entirely or not at all, and that a confirmed response agrees with recovery. The ENOSPC case fills tmpfs using actual writes until the kernel returns ENOSPC, checks an explicitly rejected batch publishes no rows, frees space, and verifies resumed durable ingest and recovery. The WAL mutation cases target the documented `bog.wal`/`bog.commit` format and must be updated if the pinned implementation changes its recovery format.
