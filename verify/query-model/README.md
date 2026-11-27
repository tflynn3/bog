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

## Issue #104 HTTP handoff

The regular race suite exercises a real loopback TCP response with a delivered
cursor and truncated body. The fixture flushes headers and a body prefix,
then forces the next socket write to fail. It checks the original suffix and
exact terminal replay after backfill and, for the retained fixture, publication.
The retained retirement check fills all 64 capture slots, holds two admitted
resumes during retirement, and checks that capacity returns only when both
handlers finish. The native Linux startup regression holds a foreign listener
through two ownership polls and checks that the first HTTP probe runs after
the child owns the port.

Run the strict #104 handler suite on the HTTP candidate:

```sh
BOG_QUERY_VERIFY_104=1 go test -race -count=1 ./verify/query-model -run '^TestQuery104'
```

On each native Linux runner, qualify the same clean candidate used for release
replay. Set `BOG_RELEASE_BINARY` and `BOG_RELEASE_REVISION` to the executable
and full revision before running:

```sh
BOG_QUERY_VERIFY_104=1 go test -json -race -count=1 ./verify/query-model \
  -run '^(TestQuery104.*|TestPinnedQueryModel104|TestPinnedQueryCandidate.*)$'
```

Require passing subtest events for both page limits in both pinned modes.
The retained pinned trial requires compaction control and verifies the exact
published cutoff. Socket fault injection and the paused resumes run against
the real HTTP handlers in the fixture process.

On base revision `447e996525352ef5dd44d5eae1de09b4cbd53d53`, the strict handler
run observed these failures:

- With 63 live captures, the faulted request delivered zero response bytes and
  the server observed a socket write timeout. The next opening returned
  `503 query capacity exhausted` on both issuers.
- At 64 live legacy captures, terminal replay succeeded before retirement.
  The legacy constructor returned an `*http.ServeMux` with no
  `RetireQueryCapture` method.
- With a 256-byte response cap, both issuers returned 422 on the opening
  request for both row limits. The diagnostic reported a 748-byte record
  and the wire walker accepted zero fitting rows.
