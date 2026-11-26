// Package idempotency coordinates retries of validated ingest batches. Each
// scoped key identifies a single durable batch during its retry window. A
// committed batch can still contain intentional duplicate rows.
//
// The backend is the authority for replay, expiry, and commit recovery across
// coordinators and process restarts. A Coordinator only serializes overlapping
// operations for the same key within one process. It keeps no replay cache.
//
// Callers must authenticate, authorize the canonical scope, and validate the
// original batch before calling Submit. See docs/ingest-retries.md for the
// integration contract.
package idempotency
