package httpapi

import (
	"context"
	"errors"
	"time"

	"bog/internal/store"
)

var (
	ErrCaptureExpired         = errors.New("query capture expired")
	ErrQueryCapacityExhausted = errors.New("query capacity exhausted")
)

// PageSource is the storage side of HTTP paging. Page meters capture and scan
// work using the supplied budget. Replaying a captured request must reproduce
// its rows, positions and More decision within the original scan work.
// A failed acquisition cleans up its own resources. After a successful
// acquisition HTTP owns Release, including when response preparation or
// confirmed pre-header delivery fails. HTTP holds the capture through
// response writes and stops admitting resumes before releasing it.
// Snapshot IDs and positions are nonempty UTF-8 strings of at most 1024 bytes.
// More requires at least one returned row and proof of another match. Sources
// may return fewer rows than requested, and HTTP will fill the response using
// successive scans. After acquisition, results may omit Capture or repeat the
// requested ID. Ordinary requests return at most Limit rows and need no
// retained capture, positions, or lookahead.
// HTTP replays the same source anchor and limit even if the client changes
// limits. An ordinary source may also return a capture and positions to fill
// short scans against one view. HTTP will release it after delivery. The command injects the coordinated retained adapter for #53.
type PageSource interface {
	Page(context.Context, PageRequest, *store.QueryWork) (PageResult, error)
	// Release is idempotent. It bars new operations while admitted operations
	// keep their references until they return. HTTP calls Release once per
	// acquisition, after its final user drains. Releases of different captures
	// may run concurrently with one another and with Page calls.
	Release(capture string) error
}

type PageFilter struct {
	Start, End time.Time // UTC, half-open
	Contains   string    // case-sensitive literal
}

type PageRequest struct {
	Filter   PageFilter
	Limit    int
	Snapshot bool
	Capture  string       // empty for acquisition or ordinary queries
	After    PagePosition // exclusive, empty at the beginning
}

// PagePosition identifies an emitted row within its capture, in timestamp
// and global append ordinal order.
type PagePosition string

type PageRow struct {
	Row      store.Row
	Position PagePosition
}

type PageResult struct {
	Capture   string // nonempty for successful snapshot acquisition
	Rows      []PageRow
	More      bool      // a further match exists after the last returned row
	ExpiresAt time.Time // optional on acquisition; HTTP caps it at 15 minutes
}
