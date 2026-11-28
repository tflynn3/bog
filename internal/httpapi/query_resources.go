package httpapi

import (
	"context"
	"errors"
	"math"
	"sync"

	"bog/internal/store"
)

var (
	ErrQueryResponseCapacityExhausted = errors.New("query response capacity exhausted")
	ErrQueryScanCapacityExhausted     = errors.New("query scan capacity exhausted")
	ErrQueryCaptureCapacityExhausted  = errors.New("query capture capacity exhausted")
)

// QueryResourceLimits limits aggregate resources for one issuing API. Bytes
// and captures are inclusive. Zero disables admissions that need that resource.
// ResponseBytes counts encoded record allocations, including partially built
// pages. ScanBytes counts reserved source scan buffers and legacy match plans.
// Captures counts slots from before source acquisition through final release,
// subject to the issuer's hard registry ceiling of 64.
type QueryResourceLimits struct {
	ResponseBytes uint64
	ScanBytes     uint64
	Captures      uint64
}

// DefaultQueryResourceLimits returns the compatibility issuer's limits. Allow
// one full default response, or multiple smaller bodies as in the #123
// workload. The independent scan allowance leaves room for concurrent bounded
// buffers and legacy match preparation. ADR 0002 allows about 8 MiB of retained
// seek pages and reusable line scratch in a worst-case scan, before copied
// results. The #108 adapter must include its result and merge bounds. Storage
// budgets shared tail backing, pinned generations and maintenance separately.
// These limits need native qualification with the composed adapter under the
// 128 MiB cap.
func DefaultQueryResourceLimits() QueryResourceLimits {
	return QueryResourceLimits{
		ResponseBytes: DefaultQueryMaxResponseBytes,
		ScanBytes:     24 << 20,
		Captures:      maxQueryCaptures,
	}
}

// PageSourceScanBound is implemented by PageSource adapters that declare their
// own conservative peak byte bound for a Page call. HTTP obtains the bound
// before admitting Page. Computing it must be concurrency-safe, must perform
// no scan or capture acquisition, and must remain valid if the view changes
// before Page starts. Include simultaneous source scratch and result backing,
// such as seek/merge and decode buffers, copied line and position bytes, and row
// slice capacities, including buffers borrowed until consumption. Shared data
// already budgeted by storage and state owned by a capture are covered by
// their respective owners. A zero bound promises no chargeable scan buffers.
// Use math.MaxUint64 when a safe bound cannot be expressed.
type PageSourceScanBound interface {
	PageScanBytes(PageRequest) uint64
}

// NewWithPageSourceAndQueryResources configures aggregate resources for one
// issuer. The source must implement PageSourceScanBound for Page calls to
// be admitted. NewWithPageSource also uses the bound when implemented.
func NewWithPageSourceAndQueryResources(source PageSource, w appender, maxBytes, concurrency int, workBudget uint64, limits QueryResourceLimits) *API {
	a := newAPIForSource(source, w, maxBytes, workBudget)
	a.captures.resources = newQueryResources(limits)
	a.ingestBytes = &queryByteBudget{limit: 16 << 20, err: errors.New("ingest capacity exhausted")}
	a.captures.resources.requireScanBound = true
	a.routes = a.handler(concurrency)
	return a
}

// NewWithQueryResources installs the legacy in-memory issuer with explicit
// aggregate limits and the same per-request semantics as NewWithQueryLimits.
func NewWithQueryResources(s *store.Store, w appender, maxBytes, concurrency int, workBudget uint64, limits QueryResourceLimits) *API {
	source := &legacyPageSource{store: s}
	a := NewWithPageSourceAndQueryResources(source, w, maxBytes, concurrency, workBudget, limits)
	a.legacy = source
	return a
}

type queryByteBudget struct {
	mu    sync.Mutex
	limit uint64
	used  uint64
	err   error
}

// A reservation has one owner. The budget lock protects aggregate accounting,
// and is never held across a scan, source release, or response write.
type queryReservation struct {
	budget *queryByteBudget
	bytes  uint64
}

func (r *queryReservation) grow(n uint64) error {
	if r.budget == nil || n == 0 {
		return nil
	}
	b := r.budget
	b.mu.Lock()
	defer b.mu.Unlock()
	// Subtract first so even an unrepresentably large total is refused.
	if n > b.limit-b.used {
		return b.err
	}
	b.used += n
	r.bytes += n
	return nil
}

func (r *queryReservation) release() {
	r.keep(0)
}

// Store borrows the opening request's reservation while building its match
// list, so scratch and retained capacity compete for the same scan budget.
func (r *queryReservation) ReserveQueryBytes(n uint64) error {
	return r.grow(n)
}

func (r *queryReservation) ReleaseQueryBytes(n uint64) {
	if n > r.bytes {
		panic("query reservation cannot release more than its charge")
	}
	r.keep(r.bytes - n)
}

// keep returns the excess after the allocation it covered has been consumed.
// The owner may only keep up to its current charge.
func (r *queryReservation) keep(n uint64) {
	if n > r.bytes {
		panic("query reservation cannot retain more than its charge")
	}
	if n == r.bytes {
		return
	}
	b := r.budget
	b.mu.Lock()
	b.used -= r.bytes - n
	r.bytes = n
	b.mu.Unlock()
}

type queryResources struct {
	response         queryByteBudget
	scan             queryByteBudget
	captures         uint64
	requireScanBound bool
}

func newQueryResources(limits QueryResourceLimits) *queryResources {
	return &queryResources{
		response: queryByteBudget{limit: limits.ResponseBytes, err: ErrQueryResponseCapacityExhausted},
		scan:     queryByteBudget{limit: limits.ScanBytes, err: ErrQueryScanCapacityExhausted},
		captures: limits.Captures,
	}
}

func (a *API) initResources() *queryResources {
	// The existing constructors share newAPIForSource in http.go. Initialize
	// the default here so those constructor signatures and call sites remain.
	s := &a.captures
	s.resourcesOnce.Do(func() {
		if s.resources == nil {
			limits := DefaultQueryResourceLimits()
			limits.ResponseBytes = max(limits.ResponseBytes, uint64(a.maxResponseBytes))
			s.resources = newQueryResources(limits)
		}
	})
	return s.resources
}

// Legacy positions are formatted timestamps and decimal ordinals. This
// bound includes formatting scratch and both source and converted row arrays.
func (*legacyPageSource) PageScanBytes(req PageRequest) uint64 {
	return (64 << 10) + uint64(req.Limit)*256
}

// withPage holds the scan reservation until consume returns. To retain data
// past that boundary the consumer must copy it into independently owned
// storage. Successful acquisition is handed to consume even after cancellation
// so that the capture owner can register its ID for release.
func (a *API) withPage(ctx context.Context, req PageRequest, work *store.QueryWork, consume func(PageResult) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	source := a.captures.source
	resources := a.initResources()
	var bound uint64
	if bounded, ok := source.(PageSourceScanBound); ok {
		bound = bounded.PageScanBytes(req)
	} else if resources.requireScanBound {
		return ErrQueryScanCapacityExhausted
	}
	// Existing NewWithPageSource callers without a bound retain their prior
	// scan behavior. There is no safe estimate for arbitrary adapter buffers.
	if bound == math.MaxUint64 {
		return ErrQueryScanCapacityExhausted
	}
	scan := queryReservation{budget: &resources.scan}
	if err := scan.grow(bound); err != nil {
		return err
	}
	defer scan.release()
	if err := ctx.Err(); err != nil {
		return err
	}
	result, err := source.Page(ctx, req, work)
	if err != nil {
		return err
	}
	return consume(result)
}
