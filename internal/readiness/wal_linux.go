package readiness

import (
	"context"
	"errors"
	"net/http"
	"os"
	"sync"
	"syscall"
	"time"

	"bog/internal/store"
	"bog/internal/wal"
)

// Appender is the WAL operation used by the ingest handler.
type Appender interface {
	Append([]byte, []store.Row) error
}

// NewHandler samples a directory path and adds an uncached GET /ready route.
// Use NewHandlerForDirectory when the WAL has already pinned its directory.
func NewHandler(ctx context.Context, dir string, log Appender, newAPI func(Appender) http.Handler) http.Handler {
	return newHandler(ctx, func(fs *syscall.Statfs_t) error { return syscall.Statfs(dir, fs) }, log, newAPI)
}

// NewHandlerForDirectory samples the filesystem of an already-open WAL
// directory and adds an uncached GET /ready route. The caller supplies an
// independently opened, unlocked handle to the pinned directory and owns its
// lifetime. In particular, use a fresh open relative to the pinned directory,
// since duplicating the WAL's locked descriptor would share its lock.
//
// Cancel ctx before closing dir. Cancellation stops subsequent samples and
// discards an in-flight result without waiting for the syscall. The probe
// holds a reference to dir's descriptor until the syscall returns.
func NewHandlerForDirectory(ctx context.Context, dir *os.File, log Appender, newAPI func(Appender) http.Handler) http.Handler {
	return newHandler(ctx, func(fs *syscall.Statfs_t) error { return statOpenDirectory(dir, fs) }, log, newAPI)
}

func newHandler(ctx context.Context, statfs func(*syscall.Statfs_t) error, log Appender, newAPI func(Appender) http.Handler) http.Handler {
	m := start(ctx, sampleInterval, func() (bool, error) {
		var fs syscall.Statfs_t
		if err := statfs(&fs); err != nil {
			return false, err
		}
		return hasHeadroom(fs), nil
	})
	return handlerForMonitor(m, log, newAPI)
}

func handlerForMonitor(m *monitor, log Appender, newAPI func(Appender) http.Handler) http.Handler {
	if logWithOutcome, ok := log.(interface{ LastCommitOutcome() wal.CommitOutcome }); ok {
		return withReady(newAPI(log), m, func() outcome {
			return readinessOutcome(logWithOutcome.LastCommitOutcome())
		})
	}
	return withReady(newAPI(&observedAppender{wal: log, monitor: m}), m, nil)
}

func statOpenDirectory(dir *os.File, fs *syscall.Statfs_t) error {
	conn, err := dir.SyscallConn()
	if err != nil {
		return err
	}
	// Hold an independent reference while entering the syscall. Release the
	// original descriptor's Control reference first so Close can complete
	// even when statfs itself is blocked.
	var duplicate uintptr
	var dupErr syscall.Errno
	if err := conn.Control(func(fd uintptr) {
		duplicate, _, dupErr = syscall.Syscall(syscall.SYS_FCNTL, fd, syscall.F_DUPFD_CLOEXEC, 0)
	}); err != nil {
		return err
	}
	if dupErr != 0 {
		return dupErr
	}
	return errors.Join(syscall.Fstatfs(int(duplicate), fs), syscall.Close(int(duplicate)))
}

func hasHeadroom(fs syscall.Statfs_t) bool {
	// A write may allocate both a WAL block and a temporary commit marker.
	// Use blocks available to unprivileged processes. Filesystems with no
	// reported inode count (such as some network filesystems) omit that check.
	return fs.Bavail >= 2 && (fs.Files == 0 || fs.Ffree > 0)
}

type observedAppender struct {
	mu      sync.Mutex
	wal     Appender
	monitor *monitor
}

func (a *observedAppender) Append(payload []byte, rows []store.Row) error {
	// Without a WAL outcome snapshot, caller returns can arrive out of commit
	// order. Serialize appends through observation to preserve that order.
	a.mu.Lock()
	defer a.mu.Unlock()

	err := a.wal.Append(payload, rows)
	result := outcome{at: time.Now()}
	switch {
	case err == nil:
		result.state = durable
	case errors.Is(err, wal.ErrUncertain):
		result.state = uncertain
	case errors.Is(err, os.ErrClosed):
		return err
	default:
		result.state = rolledBack
	}
	if err != nil {
		switch {
		case errors.Is(err, syscall.ENOSPC):
			result.cause = "enospc"
		case errors.Is(err, syscall.EIO):
			result.cause = "eio"
		default:
			result.cause = "other"
		}
	}
	a.monitor.observe(result)
	return err
}

func readinessOutcome(snapshot wal.CommitOutcome) outcome {
	result := outcome{at: snapshot.At}
	switch snapshot.State {
	case wal.CommitDurable:
		result.state = durable
		return result
	case wal.CommitRolledBack:
		result.state = rolledBack
	case wal.CommitUncertain:
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
