# ADR 0034 — Capacity control release qualification

**Status** — Implemented in the release gate. Native qualification of the
composed #53 revision is pending.

## Decision

Release Verification requires a passing `TestReleaseCapacityControlScope`
event on each native Linux release job. The test uses each job's pinned
candidate and a separate disposable data directory for each of four
launches. Each child acknowledges a one-row batch so cutoff 1 falls on a
committed batch boundary.

- A loopback listener with `BOG_CAPACITY_CONTROL` unset or explicitly
  set to `0`, and a wildcard listener with `BOG_CAPACITY_CONTROL=1`,
  must each return HTTP 404 to status and start at cutoff 1 and to capture
  retirement. Each launch acknowledges a second one-row batch so a
  one-row query page issues a live continuation. The verifier submits that
  token for retirement and confirms the same token still returns the
  exact remaining row and ends the walk.
- A loopback listener with `BOG_CAPACITY_CONTROL=1` must report a complete
  predecessor at cutoff 0 with zero covered rows and one committed row.
  The verifier starts compaction at cutoff 1 and polls status within a
  deadline. It requires a complete generation newer than the predecessor,
  with exactly one covered row and one committed row.

The verifier dials loopback directly and checks the expected bind through
`verify/listener.OwnsBind`, which applies the shared ownership check to the
Linux sockets that can accept the IPv4 probe. IPv4 and dual-stack
wildcards satisfy the wildcard expectation. IPv6-only sockets cannot
serve the probe. The verifier checks health before and after the workload,
reaps each child, and removes each fixture directory.

The #53 runtime owns route registration. It exposes
`/_capacity/compaction` only with the explicit opt-in and a loopback
listener. The live capacity workload also verifies replacement and
restart at the existing 128 MiB cap on both fixture sizes.
[Runtime cutover](0015-retained-history-runtime-cutover.md),
[Candidate listener ownership](0017-release-candidate-listener-ownership.md)

## Cost

The scope test uses four short-lived server launches and four fixture
directories per architecture. The positive case has a publication wait
bounded to five seconds. A missing passing event or a failure in the
HTTP, bind, lifecycle, or fixture cleanup checks blocks qualification.
The evidence artifact records the test events, producer log, and exit status. Historical
schema-v1 reports remain valid under standard validation. The composed
revision still requires native amd64 and arm64 runs.
[Release verifier](../../verify/release/README.md#capacity-control-scope)
