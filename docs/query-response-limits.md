# Query response byte limits

`-query-max-response-bytes` sets the maximum encoded NDJSON body size for `/query`. It must be positive and defaults to 33554432 bytes (32 MiB). The limit counts JSON escaping and each record's terminating newline. HTTP headers and error bodies are excluded.

Queries return the longest ordered prefix of complete records that fits, subject also to `limit`. If the first matching record cannot fit, the server returns HTTP 422 with an error body and no continuation cursor. Increase the limit if that record must be retrieved.

For snapshot pagination (`page=1`), a successful byte-limited page includes `Bog-Next-Cursor` whenever more records remain in the captured snapshot. Supply it as `cursor` with the same time bounds and filter to continue. A terminal page has no cursor. Records appended after snapshot capture, including backfilled records, do not appear in subsequent pages. Use snapshot pagination to retrieve all matching records across byte-limited responses.

## Query work and cursor lifetime

`-query-work-budget` caps work for each query. Before issuing a cursor, the
server reserves work for the immediate continuation.
The reservation uses the same capture, filters, row limit, and response byte cap.
It covers
token validation, seek and scan work (including lookahead), NDJSON encoding,
and delivery of the bytes that fit. This work counts toward the current
request's budget and starts after the last emitted row, even when the byte cap
trims rows the scan already found. Each subsequent request applies the same
check before issuing its own next cursor.

If the work budget runs out during page preparation or reservation, the server
returns HTTP 503 with the plain-text body `query work budget exhausted` and
no NDJSON rows or next cursor.
HTTP 503 with `query capacity exhausted` means the concurrent query or capture
slots are occupied. Retry a failed continuation using its original cursor. A smaller
`limit` may help with dense matches. Sparse matches can require a higher work
budget to reach the next match or prove the end of the walk.

Route continuations to the running handler that issued the cursor. Captures
expire at a fixed deadline set when the walk starts. A cursor lasts at most
15 minutes, and a retained-history source can expire its capture sooner.
HTTP 410 with `query capture expired` means the signed cursor's capture is
unavailable. A restart changes the signing key and returns HTTP 400 with
`invalid cursor` for an old token. In either case, start a fresh walk with
`page=1`, which takes a new capture.
