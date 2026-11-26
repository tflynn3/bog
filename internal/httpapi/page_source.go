package httpapi

import (
	"context"
	"errors"
	"net/http"
	"time"

	"bog/internal/store"
)

var (
	ErrCaptureExpired         = errors.New("query capture expired")
	ErrQueryCapacityExhausted = errors.New("query capacity exhausted")
)

// PageSource owns storage captures. A successful initial snapshot transfers
// capture ownership to HTTP, even for an empty or terminal result. A failed
// acquisition cleans up its own references. Release is idempotent, bars new
// operations, and allows already admitted source operations to drain.
//
// Until expiry, repeating a snapshot Page request must return the same rows,
// positions and More decision within its original work cost. HTTP may replay
// a certified request to serve a smaller limit or resume after a byte cut.
// A source that expires a capture first reports ErrCaptureExpired.
type PageSource interface {
	Page(context.Context, PageRequest, *store.QueryWork) (PageResult, error)
	Release(capture string) error
}

type PageFilter struct {
	Start, End time.Time
	Contains   string
}

// PageRequest supplies an empty Capture and After for an initial snapshot.
// A continuation supplies both. An ordinary query needs neither a capture
// nor lookahead.
type PageRequest struct {
	Filter   PageFilter
	Limit    int
	Snapshot bool
	Capture  string
	After    PagePosition
}

// PagePosition is an opaque UTF-8 exclusive lower bound in timestamp and
// append-ordinal order. The empty position denotes the beginning of the view.
type PagePosition string

type PageRow struct {
	Row      store.Row
	Position PagePosition
}

// PageResult returns an opaque capture ID on initial snapshot acquisition.
// More reports that a match exists after the final returned row.
type PageResult struct {
	Capture   string
	Rows      []PageRow
	More      bool
	ExpiresAt time.Time // optional source deadline, supplied on initial acquisition
}

// NewWithPageSource installs a query source with the same limits as
// NewWithQueryLimits. After draining the HTTP server, call Shutdown before
// closing the storage owner.
func NewWithPageSource(source PageSource, w appender, maxBytes, concurrency int, workBudget uint64) *API {
	a := newSourceAPI(source, w, maxBytes, workBudget)
	a.mux = a.handler(concurrency)
	return a
}

func (a *API) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	a.mux.ServeHTTP(w, r)
}

// Shutdown retires captures and waits for admitted users and releases. A
// timed-out wait may be retried while the in-flight requests drain.
func (a *API) Shutdown(ctx context.Context) error {
	return a.captures.shutdown(ctx)
}
