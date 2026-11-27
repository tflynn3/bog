package querymodel

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net/url"
	"strings"
	"time"
)

// WireLimits bounds one walk. MaxPages includes the terminal response.
// MaxPageBytes caps the encoded NDJSON body of each successful response,
// so MaxPages times MaxPageBytes bounds all accepted response bodies.
// Timeout covers the entire walk, including an optional interleaved append.
type WireLimits struct {
	MaxPages     int
	MaxPageBytes int64
	Timeout      time.Duration
}

// WireWalk specifies a fresh capture of the append log as it stands at entry.
// RowLimits cycle in request order and may change on a continuation.
type WireWalk struct {
	Seed      int64
	Filter    Filter
	RowLimits []int
	Limits    WireLimits

	// FirstPageRows optionally requires a known positive fixture page size.
	// Zero leaves the opening row count to the row and byte ceilings.
	FirstPageRows int
}

// WireRequest asks the adapter to fetch exactly one complete page. An empty
// Cursor means page=1. Otherwise send the token verbatim with the same filter.
type WireRequest struct {
	Filter       Filter
	Limit        int
	Cursor       string
	MaxPageBytes int64
}

// WirePage contains a fully validated, closed response. BodyBytes counts the
// encoded NDJSON bytes, excluding headers. An absent continuation is the empty
// string. The adapter must reject a present but empty cursor header.
type WirePage struct {
	Rows       []Row
	NextCursor string
	BodyBytes  int64
	// EncodedBody is the exact accepted NDJSON, including line endings.
	// Nil means unavailable. A non-nil empty slice witnesses an empty body.
	// If supplied, its length must equal BodyBytes. Adapters may reuse the
	// backing storage after the next fetch begins.
	EncodedBody []byte
}

// FetchWirePage is the boundary between the independent model and a transport.
// Honor ctx through body delivery, enforce MaxPageBytes before accepting the
// response (including while reading it), and validate framing before returning
// rows or a cursor. Do not retry or return partial pages. The release adapter
// uses the production client's FetchPage and checks the shipped Content-Length
// contract (ADR 0006). The oracle does not interpret the cursor's encoding.
type FetchWirePage func(context.Context, WireRequest) (WirePage, error)

// AfterCapture runs once after the first nonterminal page has been verified,
// before any continuation request. A walk that supplies this hook must issue
// a first-page continuation. The hook can acknowledge appends into the same
// ledger and retain the opaque token for a separate issuer-rejection probe.
type AfterCapture func(context.Context, string) error

// WireStats describes pages accepted by the oracle, including the terminal
// page. ByteTrimmedPages counts nonterminal pages below the requested row
// ceiling. The adapter enforces the byte ceiling independently of this count.
type WireStats struct {
	Pages            int
	Rows             int
	BodyBytes        int64
	ByteTrimmedPages int
}

// ErrWirePageBudget reports an incomplete walk at the configured page limit.
var ErrWirePageBudget = errors.New("page budget exhausted before terminal exhaustion")

const (
	// The shipped protocol allows a 2048-byte token before base64url encoding.
	// Bound bookkeeping even for adapters that do not enforce that wire limit.
	maxOpaqueCursorBytes = 4096
	wireTracePages       = 8
	wireDiagnosticBytes  = 512
)

func (w WireWalk) validate() error {
	if w.Limits.MaxPages <= 0 || w.Limits.MaxPageBytes <= 0 ||
		w.Limits.MaxPageBytes > math.MaxInt64/int64(w.Limits.MaxPages) || w.Limits.MaxPageBytes == math.MaxInt64 {
		return errors.New("page and byte budgets must be positive and their product must fit in int64")
	}
	if w.Limits.Timeout <= 0 {
		return errors.New("walk timeout must be positive")
	}
	if !w.Filter.Start.Before(w.Filter.End) {
		return errors.New("query start must precede end")
	}
	if len(w.RowLimits) == 0 {
		return errors.New("at least one row limit is required")
	}
	for i, limit := range w.RowLimits {
		if limit <= 0 {
			return fmt.Errorf("row limit at index %d must be positive, got %d", i, limit)
		}
	}
	if w.FirstPageRows < 0 || w.FirstPageRows > w.RowLimits[0] {
		return errors.New("expected first-page row count is outside the first row limit")
	}
	return nil
}

type wireStep struct {
	limit, rows int
	bodyBytes   int64
	fetched     bool
	continued   bool
}

type wireTrace struct {
	walk      WireWalk
	highWater int
	attempts  int
	steps     [wireTracePages]wireStep
}

func (t *wireTrace) request(limit int) *wireStep {
	step := &t.steps[t.attempts%len(t.steps)]
	*step = wireStep{limit: limit}
	t.attempts++
	return step
}

type wireFailure struct {
	detail string
	cause  error
}

func (e *wireFailure) Error() string { return e.detail }
func (e *wireFailure) Unwrap() error { return e.cause }

func wireErrorDetail(cause error, sensitive ...string) string {
	detail := cause.Error()
	for _, token := range sensitive {
		if token != "" {
			detail = strings.ReplaceAll(detail, token, "<cursor>")
			if escaped := url.QueryEscape(token); escaped != token {
				detail = strings.ReplaceAll(detail, escaped, "<cursor>")
			}
		}
	}
	return detail[:min(len(detail), wireDiagnosticBytes)]
}

func redactWireFailure(cause error, sensitive ...string) error {
	return &wireFailure{detail: wireErrorDetail(cause, sensitive...), cause: cause}
}

func (t *wireTrace) failure(cause error, sensitive ...string) error {
	detail := wireErrorDetail(cause, sensitive...)
	w := t.walk
	var message strings.Builder
	fmt.Fprintf(&message, "seed=%d high_water=%d start=%s end=%s contains=%.80q max_pages=%d max_page_bytes=%d timeout=%s: %.512q",
		w.Seed, t.highWater, w.Filter.Start.UTC().Format(time.RFC3339Nano),
		w.Filter.End.UTC().Format(time.RFC3339Nano), w.Filter.Contains,
		w.Limits.MaxPages, w.Limits.MaxPageBytes, w.Limits.Timeout, detail)
	fmt.Fprintf(&message, "\nrow limits %v", w.RowLimits[:min(len(w.RowLimits), wireTracePages)])
	if n := len(w.RowLimits) - wireTracePages; n > 0 {
		fmt.Fprintf(&message, " [omitted %d limits]", n)
	}
	if w.FirstPageRows > 0 {
		fmt.Fprintf(&message, " expected first-page rows=%d", w.FirstPageRows)
	}
	fmt.Fprintf(&message, "\nrequest sequence (%d attempts)", t.attempts)
	start := max(0, t.attempts-len(t.steps))
	if start > 0 {
		fmt.Fprintf(&message, " [omitted %d earlier]", start)
	}
	for i := start; i < t.attempts; i++ {
		step := t.steps[i%len(t.steps)]
		fmt.Fprintf(&message, "\n  page %d limit=%d", i+1, step.limit)
		if step.fetched {
			fmt.Fprintf(&message, " rows=%d bytes=%d continued=%t", step.rows, step.bodyBytes, step.continued)
		}
	}
	return &wireFailure{detail: message.String(), cause: cause}
}

// VerifyWireWalk checks ADR 0006's observable continuation contract against
// an independent append-log prefix. A successful page may contain fewer rows
// than requested because the byte cap can trim it. Each page must match the
// next ordered prefix exactly, and a cursor must advance beyond at least one
// emitted row with more matches still expected. The last page must exhaust
// the captured prefix with no cursor, including for an empty result.
//
// The caller owns the ledger and serializes access. The capture's expected
// rows are computed before any fetch or hook, so acknowledged later writes
// affect subsequent walks only. On error, stats describe the accepted prefix.
func VerifyWireWalk(ctx context.Context, log *Log, walk WireWalk, fetch FetchWirePage, afterCapture AfterCapture) (WireStats, error) {
	var stats WireStats
	trace := wireTrace{walk: walk}
	if log == nil || fetch == nil {
		return stats, trace.failure(errors.New("append log and fetch adapter are required"))
	}
	trace.highWater = log.HighWater()
	if err := walk.validate(); err != nil {
		return stats, trace.failure(err)
	}
	if afterCapture != nil && walk.Limits.MaxPages < 2 {
		return stats, trace.failure(errors.New("interleaved append requires a page budget of at least two"))
	}
	if err := ctx.Err(); err != nil {
		return stats, trace.failure(err)
	}
	ctx, cancel := context.WithTimeout(ctx, walk.Limits.Timeout)
	defer cancel()

	matches, err := log.Match(trace.highWater, walk.Filter)
	if err != nil {
		return stats, trace.failure(err)
	}
	seen := make(map[string]int)
	cursor := ""
	for stats.Pages < walk.Limits.MaxPages {
		if err := ctx.Err(); err != nil {
			return stats, trace.failure(err)
		}
		req := WireRequest{
			Filter: walk.Filter, Limit: walk.RowLimits[stats.Pages%len(walk.RowLimits)],
			Cursor: cursor, MaxPageBytes: walk.Limits.MaxPageBytes,
		}
		step := trace.request(req.Limit)
		page, err := fetch(ctx, req)
		if ctxErr := ctx.Err(); ctxErr != nil {
			return stats, trace.failure(ctxErr)
		}
		if err != nil {
			return stats, trace.failure(fmt.Errorf("fetch page: %w", err), req.Cursor, page.NextCursor)
		}
		step.fetched = true
		step.rows, step.bodyBytes, step.continued = len(page.Rows), page.BodyBytes, page.NextCursor != ""
		if err := checkWirePage(req, page, matches[stats.Rows:], stats.Rows, seen); err != nil {
			return stats, trace.failure(err)
		}
		if err := ctx.Err(); err != nil {
			return stats, trace.failure(err)
		}
		if stats.Pages == 0 {
			if walk.FirstPageRows > 0 && len(page.Rows) != walk.FirstPageRows {
				return stats, trace.failure(fmt.Errorf("first page has %d rows, want %d for this fixture", len(page.Rows), walk.FirstPageRows))
			}
			if afterCapture != nil && page.NextCursor == "" {
				return stats, trace.failure(errors.New("interleaved append requires a first-page continuation"))
			}
		}
		stats.Pages++
		stats.Rows += len(page.Rows)
		stats.BodyBytes += page.BodyBytes
		if page.NextCursor == "" {
			return stats, nil
		}
		if len(page.Rows) < req.Limit {
			stats.ByteTrimmedPages++
		}
		cursor = page.NextCursor
		seen[cursor] = stats.Pages
		if stats.Pages == 1 && afterCapture != nil {
			err := afterCapture(ctx, cursor)
			if ctxErr := ctx.Err(); ctxErr != nil {
				return stats, trace.failure(ctxErr)
			}
			if err != nil {
				return stats, trace.failure(fmt.Errorf("after first capture: %w", err), cursor)
			}
		}
	}
	return stats, trace.failure(ErrWirePageBudget)
}

func checkWirePage(req WireRequest, page WirePage, remaining []Entry, offset int, seen map[string]int) error {
	n := len(page.Rows)
	if page.BodyBytes < 0 || page.BodyBytes > req.MaxPageBytes {
		return fmt.Errorf("encoded body has %d bytes, want 0 through %d", page.BodyBytes, req.MaxPageBytes)
	}
	if page.EncodedBody != nil && int64(len(page.EncodedBody)) != page.BodyBytes {
		return fmt.Errorf("encoded body witness has %d bytes, reported %d", len(page.EncodedBody), page.BodyBytes)
	}
	if (page.BodyBytes == 0) != (n == 0) {
		return fmt.Errorf("encoded body has %d bytes for %d rows", page.BodyBytes, n)
	}
	if n > req.Limit || n > len(remaining) {
		return fmt.Errorf("page has %d rows, at most %d expected at row offset %d", n, min(req.Limit, len(remaining)), offset)
	}
	for i, got := range page.Rows {
		want := remaining[i]
		if !got.Time.Equal(want.Time) || got.Line != want.Line {
			return fmt.Errorf("row %d (append ordinal %d): got (%s, %.80q), want (%s, %.80q)",
				offset+i, want.Ordinal, got.Time.UTC().Format(time.RFC3339Nano), got.Line,
				want.Time.Format(time.RFC3339Nano), want.Line)
		}
	}
	switch {
	case page.NextCursor == "" && n < len(remaining):
		return fmt.Errorf("missing continuation with %d expected rows remaining", len(remaining)-n)
	case page.NextCursor == "":
		return nil
	case n == 0:
		return errors.New("empty page issued a continuation")
	case n == len(remaining):
		return errors.New("issued a continuation after terminal exhaustion")
	case len(page.NextCursor) > maxOpaqueCursorBytes:
		return fmt.Errorf("opaque continuation exceeds %d bytes", maxOpaqueCursorBytes)
	}
	if first, repeated := seen[page.NextCursor]; repeated {
		return fmt.Errorf("repeated continuation first issued by page %d", first)
	}
	return nil
}
