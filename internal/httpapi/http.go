package httpapi

import (
	"context"
	"crypto/rand"
	"errors"
	"io"
	"net/http"
	"os"
	"syscall"

	"bog/internal/store"
)

type appender interface {
	Append([]byte, []store.Row) error
}
type API struct {
	captures         queryCaptures
	legacy           *legacyPageSource
	routes           http.Handler
	wal              appender
	fatal            func()
	slots            chan struct{}
	ingestMemory     *queryByteBudget
	maxResponseBytes int
	queryWorkBudget  uint64
	cursorKey        [32]byte
}

const (
	DefaultQueryMaxResponseBytes = 32 << 20
	DefaultQueryConcurrency      = 8
	DefaultQueryWorkBudget       = 128 << 20
)

func New(s *store.Store, w appender) http.Handler {
	return NewWithQueryMaxResponseBytes(s, w, DefaultQueryMaxResponseBytes)
}

// NewWithQueryMaxResponseBytes sets the maximum encoded NDJSON body size.
func NewWithQueryMaxResponseBytes(s *store.Store, w appender, maxBytes int) http.Handler {
	return NewWithQueryLimits(s, w, maxBytes, DefaultQueryConcurrency, DefaultQueryWorkBudget)
}

// NewWithQueryLimits limits response bytes, concurrent queries, and work per query.
// Admission returns 503 "query capacity exhausted" and the work limit returns
// 503 "query work budget exhausted". Cancellation detected before response
// commit returns 408 "request cancelled". These errors use plain text. Each
// handler has its own signing key for continuation cursors. The returned
// handler is an *API and exposes Close and RetireQueryCapture.
func NewWithQueryLimits(s *store.Store, w appender, maxBytes, concurrency int, workBudget uint64) http.Handler {
	source := &legacyPageSource{store: s}
	a := newAPIForSource(source, w, maxBytes, workBudget)
	a.legacy = source
	a.routes = a.handler(concurrency)
	return a
}

// NewWithPageSource installs the storage adapter for both ordinary and snapshot
// queries. For the retained runtime, the command coordinates acquisition with
// append and generation activation. Size workBudget for the capture, the
// certified matching prefix, and encoding. Drain HTTP and call Close before
// closing the storage owner.
func NewWithPageSource(source PageSource, w appender, maxBytes, concurrency int, workBudget uint64) *API {
	a := newAPIForSource(source, w, maxBytes, workBudget)
	a.routes = a.handler(concurrency)
	return a
}

// LimitIngestMemory reserves aggregate request scratch through WAL completion.
// Configure it before serving. Requests without a declared length reserve the
// maximum batch size, and share the usual request slot limit.
func (a *API) LimitIngestMemory(limit uint64) {
	a.ingestMemory = &queryByteBudget{limit: limit, err: errors.New("ingest memory capacity exhausted")}
}

func (a *API) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	a.routes.ServeHTTP(w, r)
}

// Close retires captures and waits for admitted capture users and source
// releases. The context limits the wait. The caller separately stops
// admission and drains the HTTP server before closing the source owner.
func (a *API) Close(ctx context.Context) error {
	return a.captures.close(ctx)
}

func newAPIForSource(source PageSource, w appender, maxBytes int, workBudget uint64) *API {
	if maxBytes <= 0 {
		panic("query response byte limit must be positive")
	}
	if workBudget == 0 {
		panic("query work budget must be positive")
	}
	a := &API{
		wal: w, fatal: func() { os.Exit(1) }, slots: make(chan struct{}, 16),
		maxResponseBytes: maxBytes, queryWorkBudget: workBudget,
		captures: queryCaptures{
			source: source, live: make(map[uint64]*queryCapture), drained: make(chan struct{}),
		},
	}
	if _, err := rand.Read(a.cursorKey[:]); err != nil {
		panic(err)
	}
	return a
}

func (a *API) handler(concurrency int) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", a.health)
	mux.HandleFunc("/ingest", a.ingest)
	query := queryAdmission(concurrency, http.HandlerFunc(a.query))
	mux.HandleFunc("/query", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", http.MethodGet)
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		query.ServeHTTP(w, r)
	})
	return mux
}
func (a *API) health(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	w.Header().Set("Content-Length", "0")
	w.WriteHeader(http.StatusOK)
}
func (a *API) ingest(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	select {
	case a.slots <- struct{}{}:
		defer func() { <-a.slots }()
	default:
		http.Error(w, "busy", http.StatusServiceUnavailable)
		return
	}
	if r.ContentLength > store.MaxBatch {
		http.Error(w, "batch too large", http.StatusRequestEntityTooLarge)
		return
	}
	scratch := queryReservation{budget: a.ingestMemory}
	if a.ingestMemory != nil {
		size := r.ContentLength
		if size < 0 {
			size = store.MaxBatch
		}
		// Include body growth, both decoded row arrays, escaped JSON
		// scratch and the encoded bytes held while the WAL queues the batch.
		if err := scratch.grow(12*uint64(size) + 8<<10); err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
		defer scratch.release()
	}
	readLimit := int64(store.MaxBatch)
	if a.ingestMemory != nil && r.ContentLength >= 0 {
		readLimit = r.ContentLength
	}
	b, err := io.ReadAll(io.LimitReader(r.Body, readLimit+1))
	if len(b) > store.MaxBatch {
		http.Error(w, "batch too large", http.StatusRequestEntityTooLarge)
		return
	}
	if err != nil || (r.ContentLength >= 0 && int64(len(b)) != r.ContentLength) {
		http.Error(w, "incomplete body", http.StatusBadRequest)
		return
	}
	rows, err := store.DecodeIngest(b)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	err = a.wal.Append(b, rows)
	if isUncertainAppend(err) {
		a.fatal()
		return
	}
	if errors.Is(err, syscall.ENOSPC) {
		http.Error(w, "disk full", http.StatusServiceUnavailable)
		return
	}
	if err != nil {
		http.Error(w, "WAL unavailable", http.StatusServiceUnavailable)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
