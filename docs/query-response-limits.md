# Query response byte limits

`-query-max-response-bytes` sets the maximum encoded NDJSON body size for `/query`. It must be positive and defaults to 33554432 bytes (32 MiB). The limit counts JSON escaping and each record's terminating newline. HTTP headers and error bodies are excluded.

Queries return the longest ordered prefix of complete records that fits, subject also to `limit`. If the first matching record cannot fit, the server returns HTTP 422 with an error body and no continuation cursor. Increase the limit if that record must be retrieved.

For snapshot pagination (`page=1`), a byte-limited page includes `Bog-Next-Cursor` whenever more records remain in the captured snapshot. Supply it as `cursor` with the same time bounds and filter to continue; a terminal page has no cursor. Records appended after snapshot capture, including backfilled records, do not appear in subsequent pages. Ordinary queries have no continuation cursor; use snapshot pagination to retrieve all matching records across byte-limited responses.

## Routing snapshot continuations

Send each continuation to the same running HTTP handler that returned its cursor. The handler signs cursors with a local key and pins the walk to its store's append high-water mark. A load balancer needs affinity for the duration of the walk. Another handler, including one serving the same data, has a different signing key.

For example, start with `GET /query?start=2025-01-01T00:00:00Z&end=2025-01-02T00:00:00Z&contains=needle&limit=5&page=1` on handler A. If it returns `Bog-Next-Cursor`, send its value as `cursor` to handler A with the same bounds and filter. Omit `page` on that request. You may change `limit`. Continue until a successful response omits the cursor.

If handler A is lost or restarted, a continuation sent to its replacement returns HTTP 400 `invalid cursor` with no page or next cursor. A changed bound or filter and a malformed or modified cursor also produce HTTP 400. To recover after issuer loss, discard the old cursor and repeat the original request with `page=1`. The new walk captures a fresh snapshot and may return rows from the previous walk again. Related work tracks [store identity (#45)](https://github.com/tflynn3/bog/issues/45) and [masking revision (#46)](https://github.com/tflynn3/bog/issues/46).
