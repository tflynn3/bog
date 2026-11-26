# Query response byte limits

`-query-max-response-bytes` sets the maximum encoded NDJSON body size for `/query`. It must be positive and defaults to 33554432 bytes (32 MiB). The limit counts JSON escaping and each record's terminating newline. HTTP headers and error bodies are excluded.

Queries return the longest ordered prefix of complete records that fits, subject also to `limit`. If the first matching record cannot fit, the server returns HTTP 422 with an error body and no continuation cursor. Increase the limit if that record must be retrieved.

For snapshot pagination (`page=1`), a byte-limited page includes `Bog-Next-Cursor` whenever more records remain in the captured snapshot. Supply it as `cursor` with the same time bounds and filter to continue; a terminal page has no cursor. Records appended after snapshot capture, including backfilled records, do not appear in subsequent pages. Ordinary queries have no continuation cursor; use snapshot pagination to retrieve all matching records across byte-limited responses.

## Routing a snapshot walk

Route each continuation to the same running handler that issued its cursor.
The issuer signs the cursor with its own key and interprets the snapshot
high-water mark against its local append order. Preserve that routing for the
entire walk, including during a rolling deployment. The page size can change
between requests.

For example, send `GET /query?start=2025-01-01T00:00:00Z&end=2025-01-02T00:00:00Z&page=1&limit=5`
to instance A. If the response has `Bog-Next-Cursor: TOKEN`, send
`GET /query?start=2025-01-01T00:00:00Z&end=2025-01-02T00:00:00Z&cursor=TOKEN&limit=7`
to the same running instance A. Carry forward the original `contains` value
when the first request included one.

If that continuation goes to instance B or to A after a restart, the
replacement handler returns HTTP 400 with `invalid cursor` and no
`Bog-Next-Cursor`. If the original issuer is still running, the client can
retry the same continuation there. After issuer loss, discard the cursor and
send a new `page=1` request to a healthy instance. This starts a new snapshot
from the beginning, so it can return previously delivered rows and include
newly appended rows.

See [#45][store-identity] for store identity and [#46][masking-revision] for
masking revision.

[store-identity]: https://github.com/tflynn3/bog/issues/45
[masking-revision]: https://github.com/tflynn3/bog/issues/46
