// Package client provides a reusable Bog HTTP client. Construct a Client with
// New, set MaxPageBytes before concurrent use if the server permits larger
// pages, and call Walk to consume a stable snapshot. WalkFrom resumes at the
// cursor returned when a request or page consumer fails; use the same Query.
// FetchPage exposes a fully validated page and continuation token together.
// A short body, incomplete chunked response, invalid NDJSON, or next cursor
// that repeats the request cursor yields no page.
// Query HTTP 503 errors are retryable with the cursor from the failed walk.
// The consumer must not commit partial page effects before returning an error.
//
// Ingest sends one bounded batch without automatic replay. IngestCommitted
// indicates a durable 204, IngestRejected a rejection, and IngestUncommitted a
// confirmed Bog 503. IngestUnknown means the batch may have committed: retrying
// it can create duplicates. Errors retain HTTP status or the underlying cause.
package client
