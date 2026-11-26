// Package client provides a reusable Bog HTTP client. Construct a Client with
// New, set MaxPageBytes before concurrent use if the server permits larger
// pages, and call Walk to consume a stable snapshot. WalkFrom resumes at the
// cursor returned when a request or page consumer fails; use the same Query.
// FetchPage exposes a fully validated page and continuation token together.
// A short body, incomplete chunked response, invalid NDJSON, empty page with
// a next cursor, or repeated request cursor yields no page. Walk and WalkFrom also
// withhold any page whose next cursor repeats an earlier cursor in that call,
// including the starting cursor. They return the input cursor for the failed
// page. Query HTTP 503 errors are retryable with the cursor from the failed walk.
// The consumer must not commit partial page effects before returning an error.
//
// # Routing and restarting a walk
//
// Route every continuation to the Bog process that issued the cursor. Bog uses
// a process-local signing key and an instance-local snapshot. Use a direct
// instance endpoint or configure the proxy to keep the entire walk on its
// issuing process across connections. Client.CloseIdleConnections releases
// idle HTTP connections. Routing must still work when a new connection opens.
//
// For example, if instance A answers the first page, send the next cursor
// and the same time bounds and filter to A. During a rolling restart, the
// walk can continue while that issuing process and its snapshot are available.
//
// Cursors expire when their issuing process restarts. If A restarts or the
// proxy sends the continuation to B, the receiving Bog rejects the cursor
// with HTTP 400 and the body "invalid cursor\n", without delivering rows or
// a next cursor. HTTPError.CursorRejected identifies that response. If A is
// still available, retry a misrouted continuation there. A lost issuer may
// instead surface as a transport error. If the cursor has expired or the
// issuer is gone, start a new snapshot on a live instance by calling
// Client.Walk or sending a query with page=1 and no cursor.
// The new walk starts at the beginning and may include later writes. Discard
// partial results from the old walk if you need one consistent snapshot.
//
// This routing contract is scoped to the issuing process and snapshot. See
// [issue #45] for store identity and [issue #46] for masking revision.
//
// # Ingest
//
// Ingest sends one bounded batch without automatic replay. IngestCommitted
// indicates a durable 204, IngestRejected a rejection, and IngestUncommitted a
// confirmed Bog 503. IngestUnknown means the batch may have committed: retrying
// it can create duplicates. Errors retain HTTP status or the underlying cause.
//
// [issue #45]: https://github.com/tflynn3/bog/issues/45
// [issue #46]: https://github.com/tflynn3/bog/issues/46
package client
