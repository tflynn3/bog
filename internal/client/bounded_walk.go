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

// ContextBoundedWalk is a pull-based BoundedWalk with one context for its
// entire lifetime. Use one caller at a time. A canceled walk cannot resume,
// and any pages already returned remain accepted.
type ContextBoundedWalk struct {
	ctx  context.Context
	walk *BoundedWalk
}

// NewBoundedWalkWithContext prepares a walk that uses ctx for all page
// requests, response reads, and retry waits. Set a deadline on ctx before
// calling this method to bound the whole walk, including time between pages.
// The caller owns cancellation. ctx must be non-nil and maxPages must be
// positive, including the terminal page. Construction performs no I/O.
func (c *Client) NewBoundedWalkWithContext(ctx context.Context, q Query, maxPages int) (*ContextBoundedWalk, error) {
	if ctx == nil {
		return nil, errors.New("nil query walk context")
	}
	walk, err := c.NewBoundedWalk(q, maxPages)
	if err != nil {
		return nil, err
	}
	return &ContextBoundedWalk{ctx: ctx, walk: walk}, nil
}

// NextPage returns the next validated page using the walk's context. It
// returns io.EOF after the terminal page. Cancellation before a page is
// accepted leaves the walk's cursor and page budget unchanged.
func (w *ContextBoundedWalk) NextPage() (Page, error) {
	return w.walk.NextPage(w.ctx)
}

// NextPageWithRetry fetches the next page and retries as described by
// BoundedWalk.NextPageWithRetry. The walk's context governs all attempts
// and waits. Use errors.Is to identify cancellation and deadline errors.
func (w *ContextBoundedWalk) NextPageWithRetry(retry PageRetry) (Page, error) {
	return w.walk.NextPageWithRetry(w.ctx, retry)
}

// NextPage returns a validated page, including the terminal page, then io.EOF.
// A failed fetch leaves the cursor and budget unchanged. If the last allowed
// page has a continuation, the next call returns ErrPageBudget before fetching.
func (w *BoundedWalk) NextPage(ctx context.Context) (Page, error) {
	if w.terminal {
		return Page{}, io.EOF
	}
	if err := withQueryCancellation(ctx, nil); err != nil {
		return Page{}, err
	}
	if w.pages == w.maxPages {
		return Page{}, fmt.Errorf("after %d pages: %w", w.pages, ErrPageBudget)
	}
	p, err := w.client.FetchPage(ctx, w.query, w.cursor)
	if err != nil {
		return Page{}, fmt.Errorf("fetch page %d: %w", w.pages+1, err)
	}
	if err := withQueryCancellation(ctx, nil); err != nil {
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
