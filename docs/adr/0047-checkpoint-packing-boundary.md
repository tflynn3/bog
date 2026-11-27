# ADR 0047 — Checkpoint packing boundary

**Status** — Proposed for Core Architecture review.

## Decision

Keep checkpoint packing private to `internal/checkpoint`. The merge
builder passes the packer a context, decoded rows, and payload capacities.
Whole-block repacking takes a per-block capacity and a range of output
counts. Slack placement takes capacities in descending order and returns
groups corresponding to the prefix of gaps it used. Both routines group
rows in place and return slices in placement order. If a layout fails,
the caller receives the existing capacity error or the context error.
The input row order may change even when packing fails.

The builder ranks originals by size and name, applies the whole-block
coalescing fast paths, and tries increasingly large prefixes of at most
four originals for repacking. Repacking considers rows by descending
encoded size and then ascending append ordinal, tries the smallest
viable output count first, and uses the tightest fitting output with
ties resolved by output index. Slack placement visits originals and
eligible staged slots in descending room order. Among equal gaps, ranked
originals come first, followed by staged slots in slot order. Placement
opens another gap only when the widest used gap cannot fit the next row,
breaking ties between equally wide used gaps by their order in the
supplied capacities.
Both placement loops poll cancellation every 1,024 rows.

The builder maps groups back to publication inputs and sorts each finished
block by timestamp and append ordinal before staging. The publication
attempt owns validated reads, staging writes, reservations, slot
replacement, and settlement. As in ADR 0016, slack placement decodes
one selected block at a time. A staged slot can be rewritten once, and
its old reservation remains charged until retirement is durable. The
existing intent, install, and recovery protocol governs publication.
[Bounded staging reuse](0016-bounded-checkpoint-staging-reuse.md),
[Staged replacement recovery](0035-tail-only-retained-recovery-contract.md#83-staged-checkpoint-replacement-recovery)

## Trade-offs

The boundary lets tests exercise placement using rows and capacities alone.
In-place grouping keeps auxiliary storage to per-row assignment indexes
and per-bin metadata, with a four-input limit on decoded original rows
held during repacking. The bounded heuristics can refuse a flush even
when total free bytes would suffice under a different layout. The
publication attempt still needs budget for the overlap between a staged
block and its replacement.
