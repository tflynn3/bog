# ADR 0017 — Release candidate listener ownership

**Status** — Implemented. Native qualification of the composed candidate is pending.

The native release jobs pin one candidate binary per architecture. Its
HTTP responses count as evidence only when the probes can attribute the
listener to the launched candidate. The verifier briefly reserves an
ephemeral IPv4 loopback port, then releases it so the child can bind it.
A different process can claim that port during startup or a restart.
[Release verifier composition](0008-release-verifier-composition.md),
[Release evidence process boundary](0013-release-evidence-process-boundary.md)

## Decision

`verify/listener.Owns` takes a candidate PID and a port on `127.0.0.1`.
On Linux it reads the verifier's network namespace TCP tables and compares
matching listener socket inodes with the candidate's file descriptors.
The release targets are Linux amd64 and arm64 in the same network
namespace as their candidate. Matching entries include IPv4 wildcard,
IPv6 wildcard, and IPv4-mapped IPv6 listeners. An unbound port can be
polled within the startup deadline. A foreign listener or a procfs
inspection error fails evidence collection.

`verify/release` checks around `/healthz` and after its probes. Capacity
waits for ownership before sending `/ready`, rechecks after HTTP 200,
and requires ownership at workload completion. It repeats these checks
with the new PID and port on restart. Each producer continues to own its
process lifecycle, probe cancellation, and cleanup. Live capacity still
requires compaction control. Legacy runs retain their separate baseline
verdict.

Both native jobs require a successful parity test invocation, an explicit
pass event for `TestReleaseRestoredParity`, and a nonempty parity report
before recording that producer as successful. Ordinary tests can still
skip parity when release inputs are absent. The report validators continue
to check evidence content and candidate identity.

## Dependencies and ownership

Only `verify/release` and `bench/retained-history` may reach the helper
through Linux production imports. The helper uses the Go standard library
and has no dependency on the server or operator. The Linux fitness suite
records both direct edges and rejects additional direct or transitive
consumers. Release Verification owns the producer integration and
coordinates registry and diagram edits with the Architecture owner.
[System architecture](../architecture/SYSTEM.md),
[Candidate listener fitness check](../../internal/archtest/listener_boundary_test.go)
