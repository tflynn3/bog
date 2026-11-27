# ADR 0030 — Fixed-budget evidence for live generation replacement

**Status** — Cycle 124 integration decision. Native qualification is pending
on the composed #53 candidate.

## Decision

Release Verification within Core Engineering owns this evidence. Query
reviews the issuing boundary and the continuation verdict. The verifier
uses the pinned release candidate, the production Go HTTP client, and the
independent append-log model. It observes publication through the existing
loopback compaction control. [Fixed-budget continuations](0006-fixed-budget-query-continuations.md),
[Retained-history runtime cutover](0015-retained-history-runtime-cutover.md)

For each tied, duplicate, and sparse byte-trimmed fixture, preserve the
ordinary-path budget search and repeat it with a seed generation observed
as complete before opening the cursor. Each probe starts a fresh process and
data directory with the fixture's response byte and row caps. The search
starts at the historical reference budget and bisects through the existing
2,048-unit ceiling. An issuing probe must exhaust exactly. Only a clean
opening refusal moves the lower bound. Record the lowest observed exhausting
budget and the adjacent clean rejection when the budget is above one.

Use the retained issuing budget for a new process with an equivalent
published seed. After the first page issues a cursor and ingest acknowledges
the first backfill with HTTP 204, start compaction at the exact acknowledged
row cutoff. Observe a complete generation strictly newer than the seed,
with both covered and committed rows equal to that cutoff, before sending
the first continuation. Resume at limit one, acknowledge the second backfill
before the terminal resume, and compare the entire held walk with the
original seed through terminal exhaustion. Any failure at that budget fails
the fixture. The regression test verifies that a continuation refusal after
publication fails qualification even when the held walk can exhaust at a
higher budget.

In a separate launch at the search ceiling, replay the acknowledged batches
and the same publication cutoffs. Open a fresh cursor and compare the
augmented history, including both backfills, through terminal exhaustion.
This check gives the larger capture room to certify its own walk while
preserving the measured budget of the held trial.

## Runtime cost and qualification

The extra retained search adds at most 12 disposable candidate launches and
12 seed publications per fixture. With the ordinary search, replacement
trial, and fresh check, the bound is 26 launches and 16 publications per
fixture. Each publication has a five-second deadline and the entire fixture
has a one-minute deadline. The stage retains the native workflow's
three-minute test invocation limit. Actual timings are recorded by the Go
test events.

The existing `fixed-budget-backfill-events.jsonl` artifact records the
candidate revision, binary SHA-256, and release fixture for each query
fixture. It includes sampled budgets and outcomes, both search boundaries,
publication observations with generations and row counts, and the held and
fresh walk outcomes. The Ordered PR carries the verifier and this decision.
Qualify the same clean composed revision through the existing native Linux
amd64 and arm64 jobs. Both jobs must record a passing
`TestReleaseFixedBudgetBackfill` event as part of the full release gate.
[Native Linux release workflow](../../.github/workflows/linux-release.yml),
[Release evidence process boundary](0013-release-evidence-process-boundary.md)
