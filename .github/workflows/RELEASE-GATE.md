# Release verification contract

Run `Revision-pinned release gate` on Linux ARM64 with the full candidate commit SHA.
The fixture job only tests the validator; its success does not approve a release.
The live job builds the checked-out candidate using Go 1.27.0, checks Go's VCS
build metadata for the exact clean revision, and identifies the binary by SHA-256.
The optional baseline is recorded separately; it is never candidate evidence.
The output artifact retains the build metadata, revision, baseline, toolchain,
producer logs, reports and any producer artifacts even when the gate fails.

The following executable entry points are the release integration contract. They
are not yet present at this revision, so the live gate deliberately fails closed.
The owners of #3, #32 and #19 must implement these adapters around their
verifiers when those verifiers land:

* `bench/durability/verify-release` (#3)
* `bench/retained-history/verify-release` (#32)
* `bench/query-admission/verify-release` (#19)

Each is invoked with `--binary PATH --revision SHA --binary-sha256 SHA256
--output PATH --artifacts DIR`. It must use that binary as the candidate, write
its JSON report to the specified output path, put supporting logs and measurements
in the artifacts directory, and return nonzero on failure. It must not substitute
a baseline binary. Reports identify the producer, verifier name and version,
candidate revision and binary SHA-256, and fixture ID and SHA-256, and a nonempty configuration object. Fixture hashes
identify the actual seed/configuration used, not this validator's test fixture.
The JSON shape and required case names are illustrated in `fixtures/pass.json`;
each actual report has the fields in `common` at its top level, plus `producer`,
`candidate_revision`, `candidate_binary_sha256`, and its own `cases` object.
Each required case has a `status` of `pass`; missing, failed, and skipped cases
fail the gate. Producers should include measurements and configuration in their
reports or artifacts. Additional reported cases must also pass.

The durability producer must use a disposable local capacity-limited filesystem
and actually observe ENOSPC, then free space and verify recovery. Its cases cover
acknowledged writes, torn suffixes, committed corruption, backfill cursor walks,
and stale cursors. The capacity producer records the retained-history workload,
resource limits, and measurements. The admission producer must hold the only
query slot, establish prompt 503 with no NDJSON rows or cursor for a continuation,
check health and ingest during the hold, disconnect, and successfully retry the
original cursor. It must also pass the `work_budget` case. Its report must affirm each observation independently. These
assertions are producer obligations, not a replacement for their black-box tests.
