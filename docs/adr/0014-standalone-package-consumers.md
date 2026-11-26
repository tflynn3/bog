# ADR 0014 — Production consumers for standalone packages

**Status** — Accepted for Cycle 20.

## Context

The package registry checks the direct imports of Linux production packages.
Under ADR 0012, wired libraries are reachable from `cmd/bog`, and standalone
libraries sit outside its dependency tree. The server reachability check
cannot distinguish a standalone library used by another command from a
registered library with no command consumer. For example, `internal/client`
is shared by the operator command `cmd/bog-verify` and the release evidence
command `verify/release`. [ADR 0012](0012-command-root-boundaries.md)

## Decision

Every registered standalone library must be reachable through direct or
transitive root-module production imports from at least one registered
command root. Both the server command and verifier command roles define
roots. The verifier role covers the operator and the evidence commands.
The independent oracle and other verifier support libraries can be on an
import path from a command. They do not establish command reachability on
their own. Test imports are outside the production graph.

Staged libraries still await server wiring and may have no command consumer.
Operator or evidence use alone leaves a staged library staged. Test-only
packages still have Linux test files and no Linux production files. Verifier
support retains its existing role and independence checks. The server and
operator import restrictions in ADR 0012 continue to apply.

The companion fitness check uses the registered production imports and
reports each standalone library outside all command dependency trees. The
existing inventory test validates those roles and imports against Linux
amd64 and arm64 with cgo disabled. Run `go test -count=1 ./internal/archtest`
from a Go host to exercise both inventories and the command reachability
fixtures. When removing a library's last command consumer, review whether
the library should be removed or whether a different registry role describes
its purpose. [Standalone consumer check](../../internal/archtest/standalone_consumers_test.go)

## SYSTEM.md handoff for #53

In the leased #53 update, link this ADR from the package registry and the
companion check from the fitness section. Use this wording for the standalone
role alongside the measured imports on the composed revision.

> Standalone libraries sit outside the server dependency tree and must be
> reachable by Linux production imports from a registered command root.
> Direct and transitive paths from the operator or an evidence command
> satisfy the requirement. The Go HTTP client is used by `cmd/bog-verify`
> and `verify/release`.

[Package registry](../architecture/SYSTEM.md#package-registry),
[Fitness and registry ownership](../architecture/SYSTEM.md#fitness-and-registry-ownership),
[#53 retained-history cutover](https://github.com/tflynn3/bog/issues/53)
