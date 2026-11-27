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

## Comparable query costs

`VerifyQueryCosts` uses the seeded trace launcher, production Go HTTP
client in the fixtures and pinned checks, and the independent append-log
oracle. Every budget probe starts with an empty issuer and acknowledges the
same 112 rows in the same batches and order, including timestamp ties and
exact duplicates. Retained trials publish at that exact cutoff before querying.
The `empty` (absent literal), `sparse` (five matches for `needle [wire]`), and
`broad` (110 matches for the dense `[all]` prefix) queries share the time range,
the row limit sequence of 4, 1, and 7, and the 256-byte response cap.

The search assumes monotonic opening refusal for the same request on
equivalently seeded issuers. It requires a complete, framed HTTP 503 with
the exact work-budget refusal body and no rows or cursor at the adjacent
lower positive budget. At the issuing budget the ledger checks every page
through exhaustion. The runner replays the terminal request and compares
exact encoded bytes. The absent query ends on its opening page and the
replay repeats the explicitly framed empty body. The two adjacent budgets
are per-request configuration settings.

Separate sparse and broad trials at the work ceiling keep the opening
cursor while acknowledging a two-row backfill. The retained adapter waits
for a successor at exactly 114 covered and committed rows. Both paths then
acknowledge one more in-range row beyond that cutoff. The ledger checks
the held walk against the original prefix and the fresh walk against all
115 acknowledged rows. Both walks require byte-identical terminal replay.

Each `page_observations` entry records its ordinal, row limit, returned
rows, and validated NDJSON body bytes, including the final newline. An
optional `FetchObserved` adapter supplies scan input bytes and successful
scan-work charges from that same request, with a source for each counter.
The walk totals sum accepted pages through exhaustion. Refusal probes and
terminal replay are outside these totals. Missing counters carry an
`unavailable` reason at both page and walk scope. A measured zero remains
a counter value. The scan-bytes-per-returned-row ratio requires complete
byte observations and at least one returned row.

The instrumented retained fixture uses an immutable, timestamp- and
append-ordinal-ordered input rebuilt from acknowledged rows on replacement.
It counts the UTF-8 byte length of each line handed to literal matching,
including nonmatches and lookahead. It counts repeated scan input again
when the handler certifies the opening or re-scans on continuations.
The bounded request-local meter counts successful charges for scan setup,
seek, candidate visits, line matching, and rows collected. It passes each
charge to the handler's work meter and observes only the PageSource side
of that budget. The report's response bytes come from the bounded client
read. Cursor work, response encoding and delivery charges, disk traffic,
and on-disk encoded bytes fall outside the scan counters. These counters
characterize the fixture's scanner and the HTTP serving pattern at that
boundary. The legacy handler and pinned candidate expose wire evidence
and budget thresholds, with both scan counters marked unavailable.

The Darwin arm64 race run for seed 2709 produced these seed-walk results.
The retained held walks produced the same totals across replacement.

| Query | Legacy refused / issuing units | Retained fixture refused / issuing units | Pages | Rows | NDJSON bytes | Fixture opening scan bytes / charged units | Fixture walk scan bytes / charged units |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Absent | 2147 / 2148 | 2140 / 2141 | 1 | 0 | 0 | 2,022 / 2,141 | 2,022 / 2,141 |
| Rare | 2404 / 2405 | 3930 / 3931 | 3 | 5 | 443 | 3,423 / 3,637 | 6,871 / 7,309 |
| Dense | 4191 / 4192 | 5142 / 5143 | 40 | 110 | 6,833 | 2,063 / 2,301 | 77,489 / 86,346 |

At the 16,384-unit ceiling the retained fresh rare walk returned eight rows
in four pages, with 665 NDJSON bytes, 8,237 scan bytes and 8,751 charged
scan units. The fresh dense walk returned 113 rows in 42 pages, with 7,055
NDJSON bytes, 85,305 scan bytes and 94,856 charged scan units.

```sh
go test -race -count=1 -v ./verify/query-model -run '^TestQueryCosts'
```

After #108, use the same reviewed revision as the segment layout run. On
both native Linux architectures, set `BOG_RELEASE_BINARY` and
`BOG_RELEASE_REVISION` to the clean pinned candidate and its full revision.

```sh
go test -json -race -count=1 ./verify/query-model -run '^TestPinnedQueryModelCosts$'
```

Require passing events for both `TestPinnedQueryModelCosts/ordinary` and
`TestPinnedQueryModelCosts/retained`. The retained check requires the
candidate's compaction control. Hand Storage the `pinned_query_cost`
records with their revision, binary SHA-256 and architecture, alongside the
fixture results and the segment-layout matrix for both profiles and
architectures. Compare threshold and wire evidence at the same workload
settings. Keep fixture scan observations attributed to their measuring
boundary and host when reviewing them with the layout results.
[Segment layout candidate handoff](../../bench/segment-layout/README.md#retained-release-candidate-handoff)

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

## Selective query admission beside live cursors (#136)

`TestPinnedQueryModelSelectiveAdmission` uses the pinned candidate launcher
and the ordinary legacy pagination issuer. It records the full candidate
revision, binary SHA-256, architecture, and workload settings. The launcher
sets four query slots, 16,777,216 work units per request, and a 256-byte
response cap. The legacy issuer supplies the 24 MiB aggregate scan budget and
64-capture ceiling by default.

- **Alongside cursor.** A broad opening over 400,000 acknowledged rows leaves
  its cursor live. Concurrent requests require a framed empty body, an exact
  sparse walk with three matches, and the next broad page. The sparse walk
  includes tied duplicates, a changed row limit, and byte-identical
  terminal replay. The independent ledger checks every accepted page.
- **Scan exhaustion.** A fresh issuer opens broad cursors over a separate
  65,536-row seed. The workload reaches the aggregate scan limit with capture
  slots still free and no concurrent requests. It requires an exact, complete
  `503 query scan capacity exhausted` response twice, with no rows or
  cursor header or trailer. It then verifies a live cursor still resumes.
  Slot, response, and per-request work refusals fail this check.
- **Cancellation and expiry.** The portable race fixture sets a smaller scan
  budget and pauses an opening during allocation. A competing broad opening
  must get the scan-capacity refusal while selective requests and a resume fit.
  The fixture cancels the paused request, verifies its clean 408, joins the
  handler, and requires the same broad opening to succeed. Its virtual-clock
  expiry test exercises the real issuer timer. The separate pinned expiry run
  waits for the real 15-minute deadline, verifies the cursor and scan pressure
  shortly before it, and requires a new broad opening before probing the
  expired cursor for a clean 410. Recovery before the stale lookup witnesses
  timer-driven release.

Run the portable checks with:

```sh
go test -race -count=1 ./verify/query-model -run '^TestSelective'
```

Set `BOG_RELEASE_BINARY` and `BOG_RELEASE_REVISION` to the clean native Linux
candidate and its full revision. Run the admission and wall-clock expiry
checks in separate invocations on each native architecture:

```sh
go test -json -race -count=1 ./verify/query-model -run '^TestPinnedQueryModelSelectiveAdmission$'
BOG_QUERY_VERIFY_136_EXPIRY=1 go test -json -race -count=1 -timeout=17m \
  ./verify/query-model -run '^TestPinnedQueryModelSelectiveAdmissionExpiry$'
```

Require passing events for both admission subtests and the expiry test. The
`pinned_selective_admission` records contain acknowledged row counts, checked
pages and refusals, and the duration from the first opening request to
recovered capacity in the expiry run.
