package httpapi

import "net/http"

// queryAdmission limits concurrent calls to a query handler without waiting.
// The limit must be positive. Each wrapper owns its own admission slots.
func queryAdmission(limit int, next http.Handler) http.Handler {
	if limit <= 0 {
		panic("query admission limit must be positive")
	}
	slots := make(chan struct{}, limit)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Context().Err() != nil {
			http.Error(w, "request cancelled", http.StatusRequestTimeout)
			return
		}
		select {
		case slots <- struct{}{}:
			defer func() { <-slots }()
		default:
			writeQueryError(w, ErrQueryCapacityExhausted)
			return
		}
		// Cancellation may have happened while acquiring the slot.
		if r.Context().Err() != nil {
			http.Error(w, "request cancelled", http.StatusRequestTimeout)
			return
		}
		next.ServeHTTP(w, r)
	})
}
