// Package client provides a reusable Bog HTTP client. Construct a Client with
// New, set MaxPageBytes before concurrent use if the server permits larger
// pages, and use Walk or BoundedWalk to consume a stable snapshot. WalkFrom
// resumes at the cursor returned when a request or page consumer fails. Use
// the same query bounds and filter when resuming.
//
// FetchPage validates every page for direct callers, Walk, WalkFrom, and
// BoundedWalk. It exposes rows and the continuation together after checking
// HTTP framing, the byte ceiling, NDJSON records, the cursor header, and body
// close. Complete NDJSON records are decoded as the body arrives. The client
// holds those rows until the transport confirms the frame boundary, including
// when the encoded body exactly fills the byte ceiling.
// FetchPage errors from an HTTP response retain its status in HTTPError. Use
// errors.Is to distinguish ErrPageBytes for declared or streamed overflow,
// ErrMalformedPage for protocol violations, ErrPageInterrupted for body
// delivery failures, and the standard context cancellation and deadline
// errors. A failure can retain multiple causes, such as overflow and a body
// close error. HTTPError.Retryable identifies a query HTTP 503. A
// positive Query.Limit caps the row count. Zero omits the limit from the
// request and uses the server default. A fully framed empty page can carry
// a fresh valid cursor under the client transport contract. The shipped Bog
// server emits at least one row on every page carrying a cursor. Continue
// through accepted empty pages until a page arrives without a cursor.
//
// Walk and WalkFrom withhold a page whose next cursor repeats any request
// cursor in that call, including the starting cursor. They advance when the
// consumer returns nil and return the failed page's request cursor on error.
// Consumers must commit each page atomically. Query HTTP 503 errors can be
// retried at the returned cursor. BoundedWalk keeps constant cursor history
// and detects deterministic cycles with an anchor that can require several
// pages to detect a repeat. Its page budget includes every accepted page,
// including empty and terminal pages. Failed fetches leave its cursor and
// budget intact. Use NextPageWithRetry for bounded retries of query HTTP 503
// and interrupted delivery. Exhaustion reports ErrPageRetryExhausted along
// with the last failed attempt.
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
// If A restarts or the proxy sends the continuation to B, Bog rejects the cursor
// with HTTP 400 and the body "invalid cursor\n", without delivering rows or
// a next cursor. HTTPError.CursorRejected and ErrInvalidCursor identify that
// response. HTTP 410 "query capture expired\n" on a continuation indicates an
// authenticated cursor with an expired or unavailable capture and is exposed
// as ErrCaptureExpired. If A is still available, retry a misrouted continuation
// there. A lost issuer may
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
