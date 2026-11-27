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

## Concurrent retained-query demand (#123 to #129)

`TestConcurrentRetainedQueryDemand` runs the real HTTP handler over loopback
with the retained PageSource fixture. At a four-query ceiling it holds dense
and sparse terminal resumes at the first body write after header flush, and
holds two additional opening scans with their row buffers live. A fifth query
must get a clean 503. While all four are held, ingest acknowledges a backfill
and a later tie, and replacement activates their exact whole-batch cutoff
with a nonempty carried suffix. The test cancels both opening scans, retries
both terminal requests byte for byte, retires both held cursors, cancels the
dense delivery, then drains the sparse delivery. It requires the predecessor
pin count to fall from four to zero at the observed steps, and checks both
fresh walks against the augmented append log.

The `concurrent_query_demand` JSON record gives samples and high-water marks.
Use these observation points when comparing runs.

| Resource | Observation |
| --- | --- |
| Prepared NDJSON bytes | Sum of HTTP 200 Content-Length values from entry to WriteHeader through handler return. This counts encoded payload bytes. The meter covers retries as well as the paused responses. |
| Positioned scan-buffer bytes | Sum of live positioned-row slice capacities times element size after the store scan and through fixture PageSource return. This covers the observed row backing arrays. |
| Source captures | Successful fixture registration through Release. The report gives the maximum live count and each completed lifetime in nanoseconds, identified by acquisition order. |
| Release after cancellation or final drain | Elapsed nanoseconds from the cancellation signal or permission to finish the held body through joined completion and the confirming resource sample. |

Run the portable report with:

```sh
go test -json -race -count=1 ./verify/query-model -run '^TestConcurrentRetainedQueryDemand$'
```

One Darwin arm64 fixture run with four query slots, a 256-byte page cap,
and 32,768 work units observed peaks of 570 prepared bytes, 1,728
positioned scan-buffer bytes, and four live captures. Six captures
released, with a longest observed lifetime of 8.01 ms. Cancellation-to-release
observations were 69 µs for the sparse scan, 62 µs for the dense scan, and
58 µs for the dense delivery. Final drain took 127 µs to observe. The
elapsed times describe that run.

The fixture reports an observed working set at these boundaries. Whole-process
memory, encoded scratch buffers, other storage scan internals, and production
capture lifetimes require their own runtime observations.

Once #108 supplies the composed binary, set `BOG_RELEASE_BINARY` and
`BOG_RELEASE_REVISION` as above. On each native Linux release runner run:

```sh
go test -json -race -count=1 ./verify/query-model -run '^TestPinnedQueryGenerationReplacement$'
```

Require the matching revision and binary digest and a passing test event.
The `pinned_query_replacement` JSON record includes byte-identical retries of
one continuation observed before and after each of two exact-cutoff
replacements, terminal replay, retired-pin samples, and elapsed time from
the idle retirement request to the first observed zero retired pins. The
`unavailable` object records the candidate metrics that the HTTP and
capacity-control interfaces do not expose. Use both reports for #129 and keep
the instrumented fixture measurements attributed to the fixture and its
recorded host. The cgroup workload results come from the native capacity
producer.
