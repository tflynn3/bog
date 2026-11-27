package client

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"math"
	"net/http"
	"sync/atomic"
)

// ExportProgressVersion is the version of the durable export record.
const ExportProgressVersion = 1

var (
	// ErrExportMismatch means the checkpoint belongs to a different query or
	// issuing endpoint. The session leaves the checkpoint untouched.
	ErrExportMismatch = errors.New("export progress belongs to a different query or target")
	// ErrExportProgress means a checkpoint is invalid or has an unsupported version.
	ErrExportProgress = errors.New("invalid export progress")
	// ErrExportRunning means Run is already active on this session.
	ErrExportRunning = errors.New("export session is already running")
)

// ExportPage is one delivery to the sink. Number starts at one and remains
// stable on replay within the same export. RequestCursor is empty for the
// opening page. Page.NextCursor is empty for a terminal page.
type ExportPage struct {
	Number        int
	RequestCursor string
	Page          Page
}

// ExportSink acknowledges a whole page by returning nil after its effects are
// durable. An error or interruption can leave the sink's outcome uncertain.
// The page may also replay if acknowledgment succeeds and progress cannot be
// saved. A sink that requires exactly-once effects must atomically deduplicate
// using the page number scoped to its own durable export identity.
type ExportSink func(context.Context, ExportPage) error

// ExportProgressStore holds one export's checkpoint. File stores acquire
// exclusive ownership for Run. Callers using custom stores or wrappers must
// arrange equivalent ownership across sessions and processes. Use a separate
// checkpoint for each independent export.
//
// Save atomically replaces the full record and returns nil only once it is
// durable. An error may leave either the old or the new record in place.
// Load returns the authoritative durable record, including after an ambiguous
// Save, or found=false when no record exists. It must enforce maxPageBytes
// when loading the opening body and report ErrPageBytes on overflow. Treat the
// supplied record and returned opening bytes as immutable across the boundary.
type ExportProgressStore interface {
	Load(ctx context.Context, maxPageBytes int64) (progress ExportProgress, found bool, err error)
	Save(ctx context.Context, progress ExportProgress) error
}

// ExportProgress describes the next unacknowledged page. Binding identifies
// the query and endpoint. Complete distinguishes a finished export from an
// opening page, which also has an empty request cursor.
type ExportProgress struct {
	Version           int                `json:"version"`
	Binding           [sha256.Size]byte  `json:"binding"`
	AcknowledgedPages int                `json:"acknowledged_pages"`
	Cursor            string             `json:"cursor"`
	Complete          bool               `json:"complete"`
	CycleAnchor       string             `json:"cycle_anchor"`
	Opening           *ExportOpeningPage `json:"opening,omitempty"`
}

// ExportOpeningPage is the full validated first page, recorded before it can
// reach the sink. Reissuing the opening request could acquire a new snapshot.
type ExportOpeningPage struct {
	Body       []byte `json:"body,omitempty"`
	NextCursor string `json:"next_cursor"`
}

// ExportSession streams an export under a finite acknowledged-page budget.
// It holds at most one page at a time and makes no requests while the sink or
// progress store is processing a page. Run can be called again after failure.
type ExportSession struct {
	client   *Client
	query    Query
	binding  [sha256.Size]byte
	maxPages int
	sink     ExportSink
	store    ExportProgressStore
	running  atomic.Bool
}

// NewExportSession prepares an export without making requests or loading
// progress. maxPages includes pages acknowledged by previous runs and the
// terminal page. A subsequent session can increase the budget and use the
// same checkpoint. Route all requests to the issuing Bog process.
func (c *Client) NewExportSession(q Query, maxPages int, progress ExportProgressStore, sink ExportSink) (*ExportSession, error) {
	if maxPages <= 0 {
		return nil, errors.New("export page budget must be positive")
	}
	if progress == nil {
		return nil, errors.New("nil export progress store")
	}
	if sink == nil {
		return nil, errors.New("nil export sink")
	}
	// Use exactly the query parameters sent over HTTP, including the page
	// row limit. The digest avoids recording endpoint credentials or filters.
	binding := sha256.Sum256([]byte(c.endpoint("/query") + "\n" + queryValues(q).Encode()))
	return &ExportSession{client: c, query: q, binding: binding, maxPages: maxPages, sink: sink, store: progress}, nil
}

// Run loads progress and exports pages until terminal acknowledgment is durable.
// On failure, call Run again with an active context to resume from the store.
// An expired capture surfaces as ErrCaptureExpired and leaves progress intact.
func (s *ExportSession) Run(ctx context.Context) (resultErr error) {
	if ctx == nil {
		return errors.New("nil export context")
	}
	if !s.running.CompareAndSwap(false, true) {
		return ErrExportRunning
	}
	defer s.running.Store(false)

	if err := withQueryCancellation(ctx, nil); err != nil {
		return err
	}
	maxBytes := s.client.MaxPageBytes
	if maxBytes == 0 {
		maxBytes = DefaultMaxPage
	}
	if maxBytes < 0 || maxBytes == math.MaxInt64 {
		return errors.New("invalid export page byte limit")
	}
	store := s.store
	if owner, ok := store.(interface {
		beginExportRun() (ExportProgressStore, func() error, error)
	}); ok {
		leased, end, err := owner.beginExportRun()
		if err != nil {
			return withQueryCancellation(ctx, err)
		}
		defer func() { resultErr = joinQueryErrors(resultErr, end()) }()
		if leased != nil {
			store = leased
		}
	}
	progress, found, err := store.Load(ctx, maxBytes)
	if err = withQueryCancellation(ctx, err); err != nil {
		return fmt.Errorf("load export progress: %w", err)
	}
	if found {
		if progress.Version != ExportProgressVersion {
			return fmt.Errorf("%w: unsupported version %d", ErrExportProgress, progress.Version)
		}
		if progress.Binding != s.binding {
			return ErrExportMismatch
		}
		if err := progress.validate(maxBytes); err != nil {
			return err
		}
	} else {
		progress = ExportProgress{Version: ExportProgressVersion, Binding: s.binding}
	}

	for !progress.Complete {
		if err := withQueryCancellation(ctx, nil); err != nil {
			return err
		}
		if progress.AcknowledgedPages >= s.maxPages {
			return fmt.Errorf("after %d acknowledged pages: %w", progress.AcknowledgedPages, ErrPageBudget)
		}
		if err := s.deliver(ctx, store, &progress, maxBytes); err != nil {
			return err
		}
	}
	return withQueryCancellation(ctx, nil)
}

func (s *ExportSession) deliver(ctx context.Context, store ExportProgressStore, progress *ExportProgress, maxBytes int64) error {
	number := progress.AcknowledgedPages + 1
	page, err := s.page(ctx, store, progress, maxBytes)
	if err = withQueryCancellation(ctx, err); err != nil {
		return fmt.Errorf("prepare export page %d: %w", number, err)
	}
	if page.NextCursor != "" && page.NextCursor == progress.CycleAnchor {
		return fmt.Errorf("export page %d: %w", number, &HTTPError{StatusCode: http.StatusOK, Cause: errCursorCycle})
	}
	if err := withQueryCancellation(ctx, nil); err != nil {
		return err
	}
	err = s.sink(ctx, ExportPage{Number: number, RequestCursor: progress.Cursor, Page: page})
	if err = withQueryCancellation(ctx, err); err != nil {
		return fmt.Errorf("acknowledge export page %d: %w", number, err)
	}

	advanced := progress.advance(page.NextCursor)
	err = store.Save(ctx, advanced)
	if err = withQueryCancellation(ctx, err); err != nil {
		return fmt.Errorf("save acknowledgment for export page %d: %w", number, err)
	}
	*progress = advanced
	return nil
}

func (s *ExportSession) page(ctx context.Context, store ExportProgressStore, progress *ExportProgress, maxBytes int64) (Page, error) {
	if progress.Opening != nil {
		return progress.Opening.replay(ctx, maxBytes, s.query.Limit)
	}
	if progress.Cursor != "" {
		return s.client.FetchPage(ctx, s.query, progress.Cursor)
	}

	var opening ExportOpeningPage
	page, err := s.client.fetchPage(ctx, s.query, "", &opening.Body)
	if err != nil {
		return Page{}, err
	}
	opening.NextCursor = page.NextCursor
	progress.Opening = &opening
	if err := withQueryCancellation(ctx, nil); err != nil {
		return Page{}, err
	}
	err = store.Save(ctx, *progress)
	if err = withQueryCancellation(ctx, err); err != nil {
		return Page{}, fmt.Errorf("journal opening page: %w", err)
	}
	return page, nil
}

func (p ExportProgress) advance(next string) ExportProgress {
	p.AcknowledgedPages++
	p.Opening = nil
	p.Cursor = next
	p.Complete = next == ""
	if p.Complete || atCycleAnchor(p.AcknowledgedPages) {
		p.CycleAnchor = next
	}
	return p
}

// Brent's cycle detection updates the anchor after 1, 3, 7, ... pages.
// The acknowledged count encodes this schedule, so one cursor survives
// across restarts and the page budget bounds even nonrepeating bad cursors.
func atCycleAnchor(pages int) bool {
	n := uint(pages)
	return n&(n+1) == 0
}

func (p ExportProgress) validate(maxBytes int64) error {
	if p.Version != ExportProgressVersion {
		return fmt.Errorf("%w: unsupported version %d", ErrExportProgress, p.Version)
	}
	switch {
	case p.AcknowledgedPages < 0:
		return fmt.Errorf("%w: negative acknowledgment count", ErrExportProgress)
	case p.AcknowledgedPages == 0:
		if p.Opening == nil || p.Cursor != "" || p.CycleAnchor != "" || p.Complete {
			return fmt.Errorf("%w: inconsistent opening record", ErrExportProgress)
		}
		if int64(len(p.Opening.Body)) > maxBytes {
			return ErrPageBytes
		}
		if p.Opening.NextCursor == "" {
			return nil
		}
		return validateExportCursor(p.Opening.NextCursor)
	case p.Opening != nil:
		return fmt.Errorf("%w: acknowledged record contains an opening page", ErrExportProgress)
	case p.Complete:
		if p.Cursor != "" || p.CycleAnchor != "" {
			return fmt.Errorf("%w: completed record contains a cursor", ErrExportProgress)
		}
	default:
		if err := validateExportCursor(p.Cursor); err != nil {
			return err
		}
		if err := validateExportCursor(p.CycleAnchor); err != nil {
			return err
		}
		if (p.Cursor == p.CycleAnchor) != atCycleAnchor(p.AcknowledgedPages) {
			return fmt.Errorf("%w: inconsistent cycle anchor", ErrExportProgress)
		}
	}
	return nil
}

func validateExportCursor(cursor string) error {
	if err := validateCursor(cursor); err != nil {
		return fmt.Errorf("%w: %w", ErrExportProgress, err)
	}
	return nil
}

func (p ExportOpeningPage) replay(ctx context.Context, maxBytes int64, maxRows int) (Page, error) {
	records := queryRecords{maxBytes: maxBytes, maxRows: maxRows}
	if err := records.add(ctx, p.Body); err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return Page{}, withQueryCancellation(ctx, err)
		}
		return Page{}, fmt.Errorf("%w: opening page: %w", ErrExportProgress, err)
	}
	if err := records.finish(); err != nil {
		return Page{}, fmt.Errorf("%w: opening page: %w", ErrExportProgress, err)
	}
	return Page{Rows: records.rows, NextCursor: p.NextCursor}, nil
}
