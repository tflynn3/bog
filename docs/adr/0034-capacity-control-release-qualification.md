# ADR 0034 — Capacity control release qualification

**Status** — Implemented in the release gate. Native qualification of the
composed #53 revision is pending.

## Decision

Release Verification requires a passing `TestReleaseCapacityControlScope`
event on each native Linux release job. The test uses each job's pinned
candidate and a separate disposable data directory for each of three
launches. Each child acknowledges one seed row before the verifier probes
the control.

- A loopback listener with `BOG_CAPACITY_CONTROL` unset must return HTTP
  404 to both status and start at cutoff 1.
- A wildcard listener with `BOG_CAPACITY_CONTROL=1` must also return HTTP
  404 to both requests.
- A loopback listener with `BOG_CAPACITY_CONTROL=1` must report a complete
  predecessor at cutoff 0 with zero covered rows and one committed row.
  The verifier starts compaction at cutoff 1 and polls status within a
  deadline. It requires a complete generation newer than the predecessor,
  with exactly one covered row and one committed row.

The verifier dials loopback directly and checks actual listener addresses
against child-owned Linux socket inodes for each child. It checks health
before and after the workload, reaps each child, and removes each fixture
directory.

The #53 runtime owns route registration. It exposes
`/_capacity/compaction` only with the explicit opt-in and a loopback
listener. The live capacity workload also verifies replacement and
restart at the existing 128 MiB cap on both fixture sizes.
[Runtime cutover](0015-retained-history-runtime-cutover.md),
[Candidate listener ownership](0017-release-candidate-listener-ownership.md)

## Cost

The scope test uses three short-lived server launches and three fixture
directories per architecture. The positive case adds one launch with its
own procfs inspections and a publication wait bounded to five seconds. A
missing passing event or a failure in the HTTP, bind, lifecycle, or
fixture cleanup checks blocks qualification. The evidence artifact
records the test events, producer log, and exit status. Historical
schema-v1 reports remain valid under standard validation. The composed
revision still requires native amd64 and arm64 runs.
[Release verifier](../../verify/release/README.md#capacity-control-scope)
