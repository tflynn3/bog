# Query admission and work limits

Bog applies these limits to each `GET /query`, including first pages and
continuations. All three flags require positive values.

| Flag | Default | Unit |
| --- | ---: | --- |
| `-query-concurrency` | 8 | In-flight query handlers per server |
| `-query-work-budget` | 134217728 | Work units per request |
| `-query-max-response-bytes` | 33554432 | Encoded NDJSON bytes per response |

A query acquires a slot immediately or returns 503. It holds the slot through
the response write, including time spent waiting on a slow reader. When the
handler returns, the server releases the slot. Ingest and health requests
use independent paths and can run while all query slots are occupied.

## Work accounting

Each admitted request has one work budget tied to its request context. Every
scan in that request draws from it, including pagination lookahead and the
second scan needed to recover a cursor when the byte limit shortens a page.
Index runs, binary seeks, snapshot visibility probes, and blocks considered
for Bloom pruning each cost work. The scan charges one unit per candidate,
plus one per decoded line byte for candidates eligible for exact matching.
Cursor checks and revisited candidates also consume work.

The handler charges for continuation token validation and filter
authentication. It charges one unit per decoded line byte inspected during
response encoding, then reserves one unit per 16 encoded NDJSON bytes before
committing the response, rounding up for a nonempty body. The delivery charge
includes escapes and each record's terminating newline. The handler buffers the
encoded page within the response byte cap and escapes large records in bounded
chunks.

## HTTP responses

| Status | Plain-text body | Meaning |
| --- | --- | --- |
| 503 | `query capacity exhausted` | All query slots are in use. |
| 503 | `query work budget exhausted` | The request could not reserve the work needed to finish. |
| 408 | `request cancelled` | The handler observed cancellation before committing the response. |
| 422 | `query record exceeds response byte limit` | The first matching record on this request exceeds the response byte cap. |

These responses carry neither NDJSON rows nor a `Bog-Next-Cursor` header.
After a 503, callers can retry a continuation using its original cursor. A
lower `limit` can reduce work for a continuation that returns rows. A 422
requires a larger response byte cap to retrieve that record.

Successful requests return the ordered prefix that fits the row and byte
limits. A continuation advances past the last emitted row and keeps its
original snapshot, including append order for duplicate timestamps. See the
[query response limits](query-response-limits.md) for pagination details.
