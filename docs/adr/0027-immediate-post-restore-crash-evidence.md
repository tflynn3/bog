# ADR 0027 — Immediate post-restore crash evidence

**Status** — Standalone Linux probe implemented. Strict gate activation and
native qualification are pending.

## Decision

Release Verification owns the probe. #53 remains the release blocker. The
composed Storage runtime owns writable recovery, append durability, and
publication under one WAL owner. The probe exercises the shipped process
through HTTP and Linux process lifecycle. It uses storage libraries in test
code to build the fixture and derives the expected replay from the independent
append-log oracle. [Retained-history runtime cutover](0015-retained-history-runtime-cutover.md),
[Release evidence process boundary](0013-release-evidence-process-boundary.md),
[Retained storage boundary](0021-retained-release-evidence-boundary.md)

Each run constructs a fresh verified restore with a checkpoint and
committed tail. It starts the pinned candidate, checks health and listener
ownership using the child PID, and checks ownership immediately before
posting a two-row batch. Both rows tie with restored rows, and the second
row duplicates a row from the checkpoint. On observing HTTP 204, the
verifier sends SIGKILL before reading or closing the response body or
performing another query, readiness, or listener probe. It requires the
matching kernel wait status, process-group cleanup, and reaping within the
candidate lifecycle deadlines. Any other append status fails the case.

Only after teardown does the verifier restart the same binary with the
same flags on the same data directory. A fresh paginated walk must match
all eight expected rows in timestamp and append order through terminal
exhaustion, including ties and duplicate multiplicity. The verifier
checks listener ownership for the restarted process and records its
reaping and fixture removal. The run is bounded by probe and cleanup
deadlines and the existing parity page and response byte caps. The event
under test is a Linux process crash at the HTTP acknowledgment boundary.
[Candidate listener ownership](0017-release-candidate-listener-ownership.md),
[Pinned writable recovery authority](0005-pinned-writable-recovery-authority.md)

## Standalone native run

Supply the same clean composed revision on both native runners. Each
runner uses its own already-built, SHA-256-pinned Linux candidate. Run
from the verifier checkout on each runner with the output path outside the
disposable fixture.

```sh
BOG_RELEASE_BINARY=/absolute/path/to/bog \
BOG_RELEASE_REVISION="$candidate_revision" \
BOG_RELEASE_FIXTURE=linux-release-v1 \
BOG_RELEASE_POST_RESTORE_ACK_CRASH_REPORT=/absolute/path/to/post-restore-ack-crash.json \
go test -count=1 -run '^TestReleasePostRestoreAckCrash$' ./verify/release
```

The report uses the schema-v1 envelope with the full revision, binary
SHA-256, and fixture name. The `post_restore_ack_crash` outcome records the
verified restore point and the observations available at the end of the
probe, including on failure. A passing result contains `append_status` of
204, `observed_signal` of 9 (SIGKILL), `replay_matched` of true,
`replay_rows` of 8, and true values for `crash_candidate_reaped`,
`restart_candidate_reaped`, and `fixture_removed`.
[Standalone probe](../../verify/release/post_restore_ack_crash_linux_test.go)

## Activation handoff

At the lease handoff, the existing Release Verification owner calls the
probe from `TestReleaseRestoredParity` and writes its independent outcome
into the candidate-bound `parity.json`. Keep the held-cursor
`restored_parity` scenario with its own restore and lifecycle. It verifies
cursor continuity before crash and cursor rejection after restart. Keep
the ordinary WAL reopen case alongside it.

Extend `validate -require-cutover` to require a passing
`post_restore_ack_crash` case and check its typed evidence, including the
verified checkpoint-and-tail point, acknowledgment, observed signal, exact
replay verdict and row count, and cleanup. Preserve historical schema-v1
standard validation, including its acceptance of parity reports without this
case. Require the combined parity producer's explicit passing test event
and matching report on both native Linux amd64 and arm64 for the same
composed #53 revision. [Release gate](0008-release-verifier-composition.md),
[Native workflow](../../.github/workflows/linux-release.yml)
