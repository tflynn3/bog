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
		if err := r.Context().Err(); err != nil {
			writeQueryError(w, err)
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
		if err := r.Context().Err(); err != nil {
			writeQueryError(w, err)
			return
		}
		next.ServeHTTP(w, r)
	})
}
