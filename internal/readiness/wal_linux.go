package readiness

import (
	"context"
	"net/http"
	"syscall"

	"bog/internal/store"
	"bog/internal/wal"
)

// Appender is the WAL operation used by the ingest handler.
type Appender interface {
	Append([]byte, []store.Row) error
}

type walAppender interface {
	Appender
	LastCommitOutcome() wal.CommitOutcome
}

// NewHandler starts sampling the replayed WAL's data directory and adds
// an uncached GET /ready route to the command's API. Its JSON response is
// 200 when writes are ready or 503 with an operator-facing reason. Cancel ctx
// to stop sampling. If Statfs is blocked in the kernel, cancellation prevents
// subsequent probes and lets shutdown proceed without waiting for that call.
func NewHandler(ctx context.Context, dir string, log walAppender, newAPI func(Appender) http.Handler) http.Handler {
	m := start(ctx, sampleInterval, func() (bool, error) { return diskHeadroom(dir) })
	return handlerForMonitor(m, log, newAPI)
}

func handlerForMonitor(m *monitor, log walAppender, newAPI func(Appender) http.Handler) http.Handler {
	return withReady(newAPI(log), m, func() outcome {
		return readinessOutcome(log.LastCommitOutcome())
	})
}

func diskHeadroom(dir string) (bool, error) {
	var fs syscall.Statfs_t
	if err := syscall.Statfs(dir, &fs); err != nil {
		return false, err
	}
	// A write may allocate both a WAL block and a temporary commit marker.
	// Use blocks available to unprivileged processes. Filesystems with no
	// reported inode count (such as some network filesystems) omit that check.
	return fs.Bavail >= 2 && (fs.Files == 0 || fs.Ffree > 0), nil
}

func readinessOutcome(snapshot wal.CommitOutcome) outcome {
	result := outcome{at: snapshot.At}
	switch snapshot.State {
	case "durable":
		result.state = durable
		return result
	case "rolled_back":
		result.state = rolledBack
	case "uncertain":
		result.state = uncertain
	default:
		return outcome{}
	}
	switch snapshot.Category {
	case "enospc", "eio", "other":
		result.cause = snapshot.Category
	default:
		result.cause = "other"
	}
	return result
}
