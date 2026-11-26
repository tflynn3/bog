# Query admission

The `-query-concurrency` flag limits concurrent GET `/query` requests independently
of ingest. It defaults to 8 and must be positive. Admission is nonblocking: when
all query slots are occupied, the server responds with HTTP 503 and
`query capacity exhausted` (followed by a newline). A request whose cancellation
is observed at admission receives HTTP 408 and `request cancelled` (followed by
a newline). Neither response contains NDJSON or a continuation cursor; callers
may retry with their original cursor. Other methods on `/query` return 405.

This is the first admission slice of #19.
