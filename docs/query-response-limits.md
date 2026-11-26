# Query response byte limits

`-query-max-response-bytes` sets the maximum encoded NDJSON body size for
`/query`. It must be positive and defaults to 33554432 bytes (32 MiB).
The limit counts JSON escaping and each record's terminating newline. HTTP
headers and error bodies are excluded.

Queries return the longest ordered prefix of complete records that fits,
subject also to `limit`. A too-large first match returns HTTP 422. Snapshot
paging also checks later matches before issuing a cursor and returns 422
if any of them cannot fit on a page by itself. The error reports the byte
cap and the size of the record that exceeded it.

Start snapshot pagination with `page=1`. If more matches remain,
`Bog-Next-Cursor` identifies the last row sent, including its append order
among rows with duplicate timestamps. Supply the opaque token as `cursor`
with the same time bounds and literal filter. Equivalent time zone offsets
are accepted. You can change `limit` on each request. The terminal page
omits the cursor, and earlier cursors remain retryable until expiry.
Records appended after capture, including backfill, are excluded from the
walk. An ordinary query returns one page without a cursor.

Successful responses advertise the exact `Content-Length`. If a page is
interrupted, discard the partial body and retry the cursor used for that
request. A walk expires at the earlier of the source deadline and Bog's
15-minute limit. An authenticated expired cursor returns HTTP 410. Begin
again with `page=1` to get a fresh view.

## Positive time zone offsets

Encode the plus sign in an RFC3339 offset as `%2B`. This request starts a
one-row walk over January 1, 2025 UTC.

```sh
curl -i 'http://127.0.0.1:18080/query?start=2025-01-01T05:30:00%2B05:30&end=2025-01-02T05:30:00%2B05:30&page=1&limit=1'
```

Invalid time bounds and row limits return field-specific errors without
including the supplied values. See [query admission](query-admission.md) for
the fixed-budget contract.
