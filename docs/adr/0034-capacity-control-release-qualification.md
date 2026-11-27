# ADR 0034 — Capacity control release qualification

**Status** — Implemented in the release gate. Native qualification of the
composed #53 revision is pending.

## Decision

Release Verification requires a passing `TestReleaseCapacityControlScope`
event on each native Linux release job. The test uses each job's pinned
candidate and a separate disposable data directory for each launch.
A healthy candidate must return HTTP 404 for both compaction status and
start requests when bound to loopback with `BOG_CAPACITY_CONTROL` unset,
and when bound to the wildcard with `BOG_CAPACITY_CONTROL=1`. Each
launch commits one row before probing, so the requests name an available
cutoff. The verifier dials loopback directly and checks actual listener
addresses against child-owned Linux socket inodes. A fallback to a
loopback bind fails the wildcard case.

The #53 runtime owns route registration. It exposes
`/_capacity/compaction` only with the explicit opt-in and a loopback
listener. The live capacity workload supplies the positive proof using
that configuration, including replacement and restart at the existing
128 MiB cap on both fixture sizes.
[Runtime cutover](0015-retained-history-runtime-cutover.md),
[Candidate listener ownership](0017-release-candidate-listener-ownership.md)

## Cost

Each architecture adds two short-lived server launches, two fixture
directories, and Linux procfs inspections for bind evidence. A missing
passing event or a failure in the HTTP, bind, lifecycle, or fixture
cleanup checks blocks qualification. The evidence artifact records the
test events, producer log, and exit status. Historical schema-v1 reports
remain valid under standard validation. The composed revision still
requires native amd64 and arm64 runs.
[Release verifier](../../verify/release/README.md#capacity-control-scope)
