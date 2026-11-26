package httpapi

import (
	"crypto/rand"
	"errors"
	"io"
	"net/http"
	"os"

	"bog/internal/store"
	"bog/internal/wal"
)

type appender interface {
	Append([]byte, []store.Row) error
}
type API struct {
	pages            PageSource
	wal              appender
	fatal            func()
	slots            chan struct{}
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
// handler has its own signing key for continuation cursors.
func NewWithQueryLimits(s *store.Store, w appender, maxBytes, concurrency int, workBudget uint64) http.Handler {
	return NewWithPageSource(memoryPageSource{s}, w, maxBytes, concurrency, workBudget)
}

// NewWithPageSource serves queries from the source supplied by the composition root.
func NewWithPageSource(source PageSource, w appender, maxBytes, concurrency int, workBudget uint64) http.Handler {
	return newAPIWithPageSource(source, w, maxBytes, workBudget).handler(concurrency)
}

func newAPI(s *store.Store, w appender, maxBytes int, workBudget uint64) *API {
	return newAPIWithPageSource(memoryPageSource{s}, w, maxBytes, workBudget)
}

func newAPIWithPageSource(source PageSource, w appender, maxBytes int, workBudget uint64) *API {
	if maxBytes <= 0 {
		panic("query response byte limit must be positive")
	}
	if workBudget == 0 {
		panic("query work budget must be positive")
	}
	a := &API{
		pages: source, wal: w, fatal: func() { os.Exit(1) }, slots: make(chan struct{}, 16),
		maxResponseBytes: maxBytes, queryWorkBudget: workBudget,
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
	b, err := io.ReadAll(io.LimitReader(r.Body, store.MaxBatch+1))
	if len(b) > store.MaxBatch {
		http.Error(w, "batch too large", http.StatusRequestEntityTooLarge)
		return
	}
	if err != nil || (r.ContentLength >= 0 && int64(len(b)) != r.ContentLength) {
		http.Error(w, "incomplete body", http.StatusBadRequest)
		return
	}
	rows, err := store.Decode(b)
	if err != nil || !validSurrogates(b) {
		http.Error(w, "invalid batch", http.StatusBadRequest)
		return
	}
	err = a.wal.Append(b, rows)
	if errors.Is(err, wal.ErrUncertain) {
		a.fatal()
		return
	}
	if err != nil {
		http.Error(w, "WAL unavailable", http.StatusServiceUnavailable)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// validSurrogates examines raw JSON strings after Decode has verified JSON syntax.
// Escaped backslashes are skipped, so their following text is not an escape.
func validSurrogates(b []byte) bool {
	inString := false
	for i := 0; i < len(b); i++ {
		if !inString {
			if b[i] == '"' {
				inString = true
			}
			continue
		}
		if b[i] == '"' {
			inString = false
			continue
		}
		if b[i] != '\\' {
			continue
		}
		i++
		if b[i] != 'u' {
			continue
		}
		n := hexQuad(b[i+1 : i+5])
		i += 4
		if n >= 0xdc00 && n <= 0xdfff {
			return false
		}
		if n >= 0xd800 && n <= 0xdbff {
			if i+6 >= len(b) || b[i+1] != '\\' || b[i+2] != 'u' {
				return false
			}
			low := hexQuad(b[i+3 : i+7])
			if low < 0xdc00 || low > 0xdfff {
				return false
			}
			i += 6
		}
	}
	return true
}

func hexQuad(b []byte) uint16 {
	var n uint16
	for _, c := range b {
		n <<= 4
		switch {
		case c >= '0' && c <= '9':
			n += uint16(c - '0')
		case c >= 'a' && c <= 'f':
			n += uint16(c - 'a' + 10)
		default:
			n += uint16(c - 'A' + 10)
		}
	}
	return n
}
