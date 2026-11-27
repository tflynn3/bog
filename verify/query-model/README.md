# Query model

The append-log oracle records whole acknowledged batches in append order and
checks every emitted row against the applicable timestamp-ordered prefix.
The seeded trace runs the same operations through the legacy HTTP handler
and the retained `PageSource` fixture. It exercises sparse matches, timestamp
ties, exact duplicates, late backfill, and a 256-byte page cap. The retained
fixture activates a successor at the first backfill cutoff while the opening
cursor is held. Both paths prove clean opening rejection one work unit below
the minimum issuing budget. Every issuing trial exhausts and replays its
terminal request at the issuing budget. A separate trial allows enough work
for the fresh view to certify the augmented history.

Adapters supply `AcknowledgeBatch` and `FetchWirePage`. Retained trials also
supply a publication hook that returns after observing the exact activated
cutoff. Optional lifecycle controls exercise whole-batch refusal, retirement,
and restart with the same fixture journal. Restart evidence includes the
committed row count, encoded bytes decoded, and the peak encoded input per
replay batch in the fixture. Native #108 capacity evidence measures candidate
memory and retirement against the pinned Linux runtime.

## Release handoff

`WirePage.EncodedBody` holds the exact bytes consumed by the HTTP validator,
including the final newline. Supply a non-nil empty slice on an accepted
empty body. `BodyBytes` equals its length. Capture bytes during the actual
bounded read and return them only after framing validation and close
succeed. Re-encoding decoded rows loses byte differences the replay check
must detect. The original terminal witness is copied before the replay.
`RunFixedBudgetCase` accepts adapters without witnesses and requires both
terminal witnesses whenever either is supplied. `RunStrictFixedBudgetCase`
always requires both.

Each native release job runs this package with the race detector from the
gate checkout. It passes its pinned executable as `BOG_RELEASE_BINARY` and
the full candidate SHA as `BOG_RELEASE_REVISION` to
`TestPinnedQueryModelReplay`. The ordinary trial runs without activating
compaction. The retained trial uses the opt-in compaction control and
requires exact cutoff activation.

The amd64 and arm64 jobs each require explicit passing Go test events for
`TestPinnedQueryModelReplay/ordinary` and `TestPinnedQueryModelReplay/retained`.
A skip, including the skip for a candidate without compaction control,
fails qualification. A failed or missing event or an identity mismatch
also fails qualification. The gate verifies the executable against
`binary.sha256` before and after the race run and checks the test's logged
revision and digest against the pinned build. Each job uploads
`query-model-events.jsonl` with its existing release evidence, including
the separate `TestReleaseFixedBudgetBackfill` wire replay events.

To inspect just the pinned query check on native Linux, set
`BOG_RELEASE_BINARY` to the pinned executable and `BOG_RELEASE_REVISION`
to its full clean build revision, then run
`go test -json -race -count=1 ./verify/query-model -run '^TestPinnedQueryModelReplay$'`.
Check the individual subtest events because a skipped subtest can leave
`go test` with a zero exit status.
