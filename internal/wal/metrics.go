package wal

import "sync/atomic"

// DiagnosticsDropped counts events discarded because a diagnostics queue is full
// or has been stopped. It never invokes the diagnostics sink.
var DiagnosticsDropped atomic.Uint64

// DiagnosticsDeliveryFailures counts panics in a diagnostics consumer.
var DiagnosticsDeliveryFailures atomic.Uint64
