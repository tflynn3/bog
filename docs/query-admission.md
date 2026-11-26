# Query admission and work limits

Bog applies these limits to each `GET /query`, including first pages and
continuations. All three flags require positive values.

| Flag | Default | Unit |
| --- | ---: | --- |
| `-query-concurrency` | 8 | In-flight query handlers per server |
| `-query-work-budget` | 134217728 | Work units per request |
| `-query-max-response-bytes` | 33554432 | Encoded NDJSON bytes per response |

A query acquires a slot immediately or returns 503. It holds that slot through
body delivery, including time spent waiting on a slow reader. Ingest and health
requests use independent paths. Retained-history snapshot paging also allows
up to 64 captures per handler, including first requests preparing a plan and
expired captures still draining.

## Work accounting

Each admitted request has one work meter tied to its context. The page source
charges capture, index traversal, positioning, seek, merge, visibility, filter,
and lookahead work. The in-memory source charges one unit per candidate, plus
one per decoded line byte for candidates eligible for matching. Revisiting a
candidate costs work again.

HTTP charges one unit per decoded line byte for response encoding, then
reserves one unit per 16 encoded NDJSON bytes for delivery, rounded up.
Delivery includes JSON escapes and each record's terminating newline.

The legacy in-memory server meters each page independently, charging one unit
per cursor byte on resume, a validation unit, and one unit per filter byte.
A legacy continuation can exhaust its work budget even when the first page fit.

With the retained-history page source, HTTP charges for cursor signing and
authentication in fixed-size blocks and one unit per literal byte when
hashing the filter. Before returning the first cursor, HTTP certifies the rest
of the captured walk through exhaustion using the first request's meter. It measures
each remaining record and stores up to 512 scan anchors and row counts. The
plan retains no row bodies. A continuation reuses the certified source limits
and exclusive anchors, and can start within a scan or span scans. Its source
calls, encoding, delivery, and cursor work fit within the issuing budget for
any valid client row limit, including a smaller one. A fixed-budget walk may
still encounter temporary admission saturation or expire.

For the composed #53 release gate, the retained-history capacity fixtures need
a work budget sized for full-walk certification. The larger fixture uses
393216 seed rows with 1024-byte lines, so measuring the complete restart walk
alone exceeds the default budget. Keep the deliberately small budgets in the
fixed-budget release probe.

## HTTP responses

| Status | Plain-text body | Meaning |
| --- | --- | --- |
| 400 | `invalid cursor` | The token is malformed, came from a different issuer, or the filter changed. |
| 408 | `request cancelled` | The handler observed cancellation before committing the response. |
| 410 | `query capture expired` | The authenticated capture expired or became unavailable. Start a fresh walk. |
| 422 | `query record exceeds response byte limit (cap … bytes, requires … bytes)` | A record cannot fit on its own in a page being prepared or certified. |
| 503 | `query capacity exhausted` | All query slots or capture slots are in use. |
| 503 | `query work budget exhausted` | The request could not finish within its work or replay plan bounds. |

These responses carry neither NDJSON rows nor a `Bog-Next-Cursor` header.
A continuation can retry a temporary failure using its original cursor while
the capture is live. See [query response limits](query-response-limits.md) for
pagination details.
