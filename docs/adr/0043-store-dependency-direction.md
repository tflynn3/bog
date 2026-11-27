# ADR 0043 — Store dependency direction

**Status** — Proposed for Cycle 184 review.

## Decision

`internal/store` must have no direct or transitive Linux production import
path to another package in the root module. The rule follows imports through
local and external relays. Standard-library and external production imports
are allowed when they have no path back into root-module production packages.
Test imports are outside the production graph.

Store owns row decoding, publication, and ordering. WAL, retained storage,
HTTP, and the offline reader share that foundation. Keeping the dependency
pointing toward store lets these consumers reuse row semantics without also
coupling their storage authority, transport, or lifecycle. Place behavior
that needs to combine store with another component in the consumer or at
the command composition boundary. [Runtime storage composition](0007-runtime-storage-composition.md),
[Command-owned retained storage composition](0026-command-owned-retained-storage-composition.md)

## Fitness and exceptions

The storage authority check applies the same reachability rule to the
declared registry and the measured Linux amd64 and arm64 production graphs.
The measured walk includes external production dependencies. Registering
an edge cannot grant an exception. [Storage authority check](../../internal/archtest/storage_authority_test.go),
[Store dependency fixtures](../../internal/archtest/store_dependency_test.go)

Core Architecture owns the rule with Storage review. To propose an
exception, identify the exact dependency path, explain why the behavior
cannot live in a consumer or the command, and document the impact on store's
other consumers. Obtain Core Architecture approval and Storage review of
an ADR amendment or successor before introducing the dependency. Include
the approved scope and update the fitness rule and its fixtures with the
change.

Storage owns the leased SYSTEM.md and registry updates in the #108 command
wiring PR. Link this decision from SYSTEM.md in that PR and run
`go test -race -count=1 ./internal/archtest` on the composed revision.
Release Verification qualifies that revision on native Linux amd64 and
arm64. [Fitness and registry ownership](../architecture/SYSTEM.md#fitness-and-registry-ownership),
[Native release evidence](../architecture/SYSTEM.md#release-evidence)
