package readiness

import (
	"context"
	"errors"
	"net/http"
	"os"
	"reflect"
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

// NewHandler starts sampling the replayed WAL's data directory and adds
// an uncached GET /ready route to the command's API. Its JSON response is
// 200 when writes are ready or 503 with an operator-facing reason. Cancel ctx
// to stop sampling. If
// Statfs is blocked in the kernel, cancellation prevents subsequent probes
// and lets shutdown proceed without waiting for that single call.
func NewHandler(ctx context.Context, dir string, log Appender, newAPI func(Appender) http.Handler) http.Handler {
	m := start(ctx, sampleInterval, func() (bool, error) { return diskHeadroom(dir) })
	return handlerForMonitor(m, log, newAPI)
}

func handlerForMonitor(m *monitor, log Appender, newAPI func(Appender) http.Handler) http.Handler {
	if latest := walOutcomes(log); latest != nil {
		return withReady(newAPI(log), m, latest)
	}
	return withReady(newAPI(&observedAppender{wal: log, monitor: m}), m, nil)
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

// Issue #33 adds LastCommitOutcome in a separate ordered change. Discover the
// method so the command also builds at the base revision before that API exists.
func walOutcomes(log Appender) func() outcome {
	method := reflect.ValueOf(log).MethodByName("LastCommitOutcome")
	if !method.IsValid() || method.Type().NumIn() != 0 || method.Type().NumOut() != 1 {
		return nil
	}
	result := method.Type().Out(0)
	if result.Kind() != reflect.Struct {
		return nil
	}
	state, hasState := result.FieldByName("State")
	at, hasTime := result.FieldByName("At")
	category, hasCategory := result.FieldByName("Category")
	if !hasState || state.Type.Kind() != reflect.String ||
		!hasTime || at.Type != reflect.TypeFor[time.Time]() ||
		!hasCategory || category.Type.Kind() != reflect.String {
		return nil
	}
	return func() outcome {
		snapshot := method.Call(nil)[0]
		result := outcome{at: snapshot.FieldByIndex(at.Index).Interface().(time.Time)}
		switch snapshot.FieldByIndex(state.Index).String() {
		case "durable":
			result.state = durable
		case "rolled_back":
			result.state = rolledBack
		case "uncertain":
			result.state = uncertain
		}
		if result.state == rolledBack || result.state == uncertain {
			switch cause := snapshot.FieldByIndex(category.Index).String(); cause {
			case "enospc", "eio", "other":
				result.cause = cause
			default:
				result.cause = "other"
			}
		}
		return result
	}
}
