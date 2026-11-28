package httpapi

import (
	"context"
	"net/http"
	"time"
)

// queryAdmission bounds active calls. If maxWait is positive, up to limit
// additional requests wait briefly without reserving scan or response bytes.
func queryAdmission(limit int, maxWait time.Duration, next http.Handler) http.Handler {
	if limit <= 0 {
		panic("query admission limit must be positive")
	}
	slots := make(chan struct{}, limit)
	var waiting chan struct{}
	if maxWait > 0 {
		waiting = make(chan struct{}, limit)
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		if ctx.Err() != nil {
			http.Error(w, "request cancelled", http.StatusRequestTimeout)
			return
		}
		select {
		case slots <- struct{}{}:
		default:
			if waiting == nil || !waitForQuerySlot(ctx, slots, waiting, maxWait) {
				if ctx.Err() != nil {
					http.Error(w, "request cancelled", http.StatusRequestTimeout)
				} else {
					http.Error(w, "query capacity exhausted", http.StatusServiceUnavailable)
				}
				return
			}
		}
		defer func() { <-slots }()
		// Cancellation may have happened while acquiring the slot.
		if ctx.Err() != nil {
			http.Error(w, "request cancelled", http.StatusRequestTimeout)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func waitForQuerySlot(ctx context.Context, slots, waiting chan struct{}, maxWait time.Duration) bool {
	select {
	case waiting <- struct{}{}:
		defer func() { <-waiting }()
	default:
		return false
	}
	timer := time.NewTimer(maxWait)
	defer timer.Stop()
	select {
	case slots <- struct{}{}:
		return true
	case <-timer.C:
		return false
	case <-ctx.Done():
		return false
	}
}
