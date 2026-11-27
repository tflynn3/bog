# ADR 0032 — Operator export boundary

**Status** — Cycle 134 integration decision for #98. Command registration
and fitness enforcement land with the command after the #53 lease clears.

## Decision

`cmd/bog-export` is a separate operator process that streams NDJSON from
the shipped server's existing HTTP query API. Its only direct root-module
production import is `internal/client`. The client may use `internal/store`
for shared row types. `cmd/bog` owns the listener, WAL, pinned data directory,
and retained query view. The command gets its view by starting a snapshot
query and following opaque continuations against the issuing server with
the same half-open time range and literal filter.
[System architecture](../architecture/SYSTEM.md),
[Fixed-budget query continuations](0006-fixed-budget-query-continuations.md)

Use the client's bounded walk to accept fully validated pages under one
overall timeout and a finite page budget, including terminal pages. Emit
rows in server order, preserving append order for timestamp ties and
preserving duplicate rows. Exit successfully after consuming the terminal
page and writing its rows. A walk failure, including page budget exhaustion
while a continuation remains, returns nonzero with a diagnostic on stderr.
Previously emitted complete rows remain on stdout and constitute a partial
export. An output write failure also returns nonzero.

## Command governance

Register `cmd/bog-export` as an operator command in the Linux production
graph. Keep it separate from the release evidence roots. Export results
describe the requested query at the supplied endpoint. Release Verification
qualifies the pinned candidate with its own producers and independent query
oracle. The read-only backup authority granted to `cmd/bog-verify` belongs
to that command.
[Command root boundaries](0012-command-root-boundaries.md),
[Read-only operator verification](0011-operator-verification-boundary.md),
[Release evidence boundary](0021-retained-release-evidence-boundary.md)

Fitness must require the export root and its exact direct edge to
`internal/client`. Walk the registered Linux production imports from the
export root and reject direct or transitive paths to the following packages:

- Server command and runtime authority in `cmd/bog`, `internal/httpapi`,
  `internal/readiness`, and `internal/wal`.
- Backup and retained storage in `internal/backup`, `internal/checkpoint`,
  `internal/history`, and `internal/idempotency`.
- Evidence producers and support at or under `verify` and `bench`.

Apply the exclusions by package path through any registered intermediary,
regardless of role. Cover both a direct violation and a violation through
a registered relay, along with the permitted client and shared row types.
Exact-import checks compare the registry with both Linux production
inventories. Test imports remain outside the production boundary.
[Architecture fitness suite](../../internal/archtest/architecture_test.go)

## Integration and qualification

Within Core Engineering, Architecture owns this ADR and the governance
handoff. CLI integration owns the export command integration and the
[Linux pagination walkthrough](../examples/pagination/README.md). The
opt-in check runs an already built export binary against the walkthrough's
owned loopback server. It checks exact one-row pages through completion,
including ties and duplicates, then checks the nonzero exit, diagnostic,
and exact first complete row with a one-page budget.

After the #53 lease clears, the Architecture governance owner adds the
registered root and boundary rule, the Mermaid edge from `cmd/bog-export`
to `internal/client`, and the operator package-table entry in `SYSTEM.md`
in the same merge as the #98 command. Keep the export node outside the
server and evidence subgraphs. Run `go test -count=1 ./internal/archtest`
on that composed tree and check both Linux production inventories
(amd64 with `GOAMD64=v1` and arm64 with `GOARM64=v8.0`, cgo disabled).

#53 remains on the release critical path. Release Verification qualifies
the retained-history candidate on native Linux amd64 and arm64 at both
128 MiB workload scales (196,608 and 393,216 seed rows), with swap disabled.
The export walkthrough checks operator integration independently of that
release gate.
[Native Linux release evidence](../architecture/SYSTEM.md#release-evidence)
