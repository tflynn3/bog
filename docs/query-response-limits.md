# Query response byte limits

`-query-max-response-bytes` sets the maximum encoded NDJSON body size for `/query`. It must be positive and defaults to 33554432 bytes (32 MiB). The limit counts JSON escaping and each record's terminating newline. HTTP headers and error bodies are excluded.

Queries return the longest ordered prefix of complete records that fits, subject also to `limit`. If the first matching record cannot fit, the server returns HTTP 422 with an error body and no continuation cursor. Increase the limit if that record must be retrieved.

For snapshot pagination (`page=1`), a byte-limited page includes `Bog-Next-Cursor` whenever more records remain in the captured snapshot. Supply it as `cursor` with the same time bounds and filter to continue; a terminal page has no cursor. Records appended after snapshot capture, including backfilled records, do not appear in subsequent pages. Ordinary queries have no continuation cursor; use snapshot pagination to retrieve all matching records across byte-limited responses.

## Routing snapshot continuations

Route every continuation to the same running HTTP handler that issued its
cursor. For example, if handler A returns `Bog-Next-Cursor: TOKEN` for
`GET /query?start=2025-01-01T00:00:00Z&end=2025-01-02T00:00:00Z&contains=error&page=1&limit=5`,
send
`GET /query?start=2025-01-01T00:00:00Z&end=2025-01-02T00:00:00Z&contains=error&cursor=TOKEN&limit=7`
to A. The token is opaque. The time bounds and literal filter must stay the
same, and the row limit may change on each request.

If failover sends that request to handler B, or A has restarted during a
rolling deployment, the replacement returns HTTP 400 with the plain-text
body `invalid cursor` and no `Bog-Next-Cursor`. The signing key belongs to
the issuing handler, even when another handler serves the same recovered data.
Malformed, modified, and query-mismatched cursors use the same error.

If A is still available, retry the original continuation there. If the issuer
is gone, remove `cursor` and submit the query again with `page=1`. The restart
captures a new snapshot from the beginning of the requested time range and
can return rows from the earlier walk. Cross-instance resume depends on the
store identity and masking revision work tracked in [#45] and [#46].

If a continuation returns 503, retry with the same cursor on its issuer. A
smaller `limit` can reduce the work for the retry. Verify the entire page
against its declared `Content-Length` before recording its next cursor. If
the connection ends early, discard the incomplete page and retry from the
original cursor.

[#45]: https://github.com/tflynn3/bog/issues/45
[#46]: https://github.com/tflynn3/bog/issues/46
