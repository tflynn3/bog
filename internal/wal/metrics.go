package wal

import "sync/atomic"

// DiagnosticsDropped counts events discarded on overflow or shutdown.
var DiagnosticsDropped atomic.Uint64

// DiagnosticsDeliveryFailures counts downstream sink panics recovered by workers.
var DiagnosticsDeliveryFailures atomic.Uint64

// DiagnosticsSuppressed is available to per-WAL rate limiters.
var DiagnosticsSuppressed atomic.Uint64
