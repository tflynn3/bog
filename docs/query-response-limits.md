# Query response byte limits

`-query-max-response-bytes` sets the maximum encoded NDJSON body size for `/query`. It must be positive and defaults to 33554432 bytes (32 MiB). The limit counts JSON escaping and each record's terminating newline. HTTP headers and error bodies are excluded.

Queries return the longest ordered prefix of complete records that fits, subject also to `limit`. If the first matching record cannot fit, the server returns HTTP 422 with an error body and no continuation cursor. Increase the limit if that record must be retrieved.

For snapshot pagination (`page=1`), a byte-limited page includes `Bog-Next-Cursor` whenever more records remain in the captured snapshot. Supply it as `cursor` with the same time bounds and filter to continue; a terminal page has no cursor. Records appended after snapshot capture, including backfilled records, do not appear in subsequent pages. Ordinary queries have no continuation cursor; use snapshot pagination to retrieve all matching records across byte-limited responses.
