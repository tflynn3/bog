# ADR 0053 — Retained serving constructor calls

**Status** — Accepted for the #108 architecture fitness check. The Storage
wiring, role promotions, and qualification on both native Linux targets are
pending.

## Decision

Require a direct typed call to each active retained serving constructor in
the selected `cmd/bog` production Go files on Linux amd64 and arm64
independently. This supersedes ADR 0040's rule that a function value
satisfies a required constructor. The architecture suite uses the same
target settings as its inventory and resolves callees by the defining
import path and package-level function name. Calls inside command-local
wrappers qualify, including through aliased and dot imports. A function
assignment or a call through a function variable leaves the direct call
requirement unsatisfied. Report missing calls with the target and excluded
legacy references with their source positions.
[Original constructor boundary](0040-retained-runtime-constructor-boundary.md),
[Constructor fitness check](../../internal/archtest/retained_runtime_wiring_test.go)

Activate each requirement when the corresponding package has the runtime
library role or is reachable from `cmd/bog` in that target's production
inventory. Continue rejecting every typed reference to the excluded
constructors, including function values.

| Activation | Required direct call | Excluded references |
| --- | --- | --- |
| Any of `internal/checkpoint`, `internal/history`, or `internal/backup` | `readiness.NewHandlerForDirectory` | `readiness.NewHandler` |
| `internal/history` | `httpapi.NewWithPageSource` | `httpapi.New`, `httpapi.NewWithQueryMaxResponseBytes`, `httpapi.NewWithQueryLimits` |

The fully staged baseline permits compatibility constructors. Checkpoint
or backup activation alone allows compatibility paging alongside pinned
readiness. History activation requires both retained constructors.

## Limits and handoff

The check establishes that each required call exists in the target's
command source. It counts a call inside an unused wrapper or an unexecuted
branch. Source review establishes that the serving path invokes the call,
installs its result, and supplies the intended inputs. Storage traces the
`PageSource` to a validated generation and matching durable, query-visible
WAL suffix pinned under owner synchronization. Core Engineering reviews
the HTTP paging and capture-release lifecycle. Storage traces the readiness
handle to an independent, unlocked open relative to the pinned WAL
directory and reviews probe cancellation, startup cleanup, API close,
storage drain, and lock retention on a failed drain.
[Runtime interfaces](0015-retained-history-runtime-cutover.md#runtime-interfaces),
[Shutdown and delivery](0039-query-delivery-and-retained-cutover.md#retained-composition-and-shutdown)

Coordinate role changes with Storage in the later #108 wiring PR. Base
each promotion on both Linux inventories of the reviewed Storage SHA and
the activation evidence in ADR 0049. Update the registry, staging
declarations, and system graph and role table together, then rerun the
architecture suite on the combined revision. Release Verification
qualifies the same composed revision on native Linux amd64 and arm64,
including retained shutdown against each runner's pinned binary.
[Retained runtime promotion](0049-retained-runtime-promotion.md),
[Native retained shutdown gate](0048-release-gate-evidence-checks.md#retained-shutdown)
