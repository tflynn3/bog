package client

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
)

// ErrPageBudget means the last allowed page still had a continuation.
var ErrPageBudget = errors.New("query page budget exhausted before a terminal page")

// BoundedWalk is a pull-based snapshot walk. It retains only the request
// cursor and one cycle-detection anchor, independent of the page budget.
// Use one caller at a time. Create a new walk to start a fresh snapshot.
type BoundedWalk struct {
	client   *Client
	query    Query
	maxPages int
	pages    int
	cursor   string
	terminal bool

	anchor   string
	window   uint64
	distance uint64
}

// NewBoundedWalk prepares a walk starting at the first page. maxPages must be
// positive and includes the terminal page. It performs no HTTP requests.
func (c *Client) NewBoundedWalk(q Query, maxPages int) (*BoundedWalk, error) {
	if maxPages < 1 {
		return nil, errors.New("query page budget must be positive")
	}
	return &BoundedWalk{client: c, query: q, maxPages: maxPages, window: 1}, nil
}

// NextPage returns a validated page, including the terminal page, then io.EOF.
// A failed fetch leaves the cursor and budget unchanged. If the last allowed
// page has a continuation, the next call returns ErrPageBudget before fetching.
func (w *BoundedWalk) NextPage(ctx context.Context) (Page, error) {
	if w.terminal {
		return Page{}, io.EOF
	}
	if err := ctx.Err(); err != nil {
		return Page{}, err
	}
	if w.pages == w.maxPages {
		return Page{}, fmt.Errorf("after %d pages: %w", w.pages, ErrPageBudget)
	}
	p, err := w.client.FetchPage(ctx, w.query, w.cursor)
	if err != nil {
		return Page{}, fmt.Errorf("fetch page %d: %w", w.pages+1, err)
	}
	if err := ctx.Err(); err != nil {
		return Page{}, err
	}
	if p.NextCursor != "" {
		if p.NextCursor == w.anchor {
			return Page{}, fmt.Errorf("page %d: %w", w.pages+1, &HTTPError{
				StatusCode: http.StatusOK,
				Cause:      errCursorCycle,
			})
		}
		// Brent's cycle detection uses exponentially spaced anchors. It
		// detects deterministic continuation loops without extra requests.
		// The page budget also bounds a stream with ever-changing tokens.
		w.distance++
		if w.distance == w.window {
			w.anchor = p.NextCursor
			w.distance = 0
			w.window *= 2
		}
	}
	w.pages++
	w.cursor = p.NextCursor
	w.terminal = p.NextCursor == ""
	return p, nil
}
