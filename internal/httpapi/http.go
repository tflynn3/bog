package httpapi

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"time"

	"bog/internal/store"
	"bog/internal/wal"
)

type appender interface {
	Append([]byte, []store.Row) error
}
type API struct {
	store        *store.Store
	wal          appender
	fatal        func()
	slots        chan struct{}
	querySlots   chan struct{}
	queryTimeout time.Duration
	limits       store.QueryLimits
}

type QueryConfig struct {
	Concurrency int
	Timeout     time.Duration
	Limits      store.QueryLimits
}

func DefaultQueryConfig() QueryConfig {
	return QueryConfig{Concurrency: 8, Timeout: 30 * time.Second, Limits: store.QueryLimits{Work: 256 << 20, Rows: 1000000, MatchBytes: 256 << 20, ResultBytes: 16 << 20}}
}
func New(s *store.Store, w appender) http.Handler {
	return NewWithQueryConfig(s, w, DefaultQueryConfig())
}
func NewWithQueryConfig(s *store.Store, w appender, config QueryConfig) http.Handler {
	if config.Concurrency < 1 {
		panic("query concurrency must be positive")
	}
	a := &API{store: s, wal: w, fatal: func() { os.Exit(1) }, slots: make(chan struct{}, 16), querySlots: make(chan struct{}, config.Concurrency), queryTimeout: config.Timeout, limits: config.Limits}
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", a.health)
	mux.HandleFunc("/ingest", a.ingest)
	mux.HandleFunc("/query", a.query)
	return mux
}
func (a *API) health(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	w.WriteHeader(http.StatusOK)
}
func (a *API) ingest(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
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

var cursorKey = func() []byte {
	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		panic(err)
	}
	return key
}()

type cursorData struct {
	Version  int               `json:"v"`
	Start    time.Time         `json:"start"`
	End      time.Time         `json:"end"`
	Contains [sha256.Size]byte `json:"contains"`
	Count    int               `json:"count"`
	After    store.Position    `json:"after"`
}

func containsDigest(value string) [sha256.Size]byte {
	mac := hmac.New(sha256.New, cursorKey)
	mac.Write([]byte("bog-filter-v1\x00"))
	mac.Write([]byte(value))
	var digest [sha256.Size]byte
	copy(digest[:], mac.Sum(nil))
	return digest
}

func encodeCursor(c cursorData) string {
	b, _ := json.Marshal(c)
	mac := hmac.New(sha256.New, cursorKey)
	mac.Write([]byte("bog-cursor-v1\x00"))
	mac.Write(b)
	return base64.RawURLEncoding.EncodeToString(append(b, mac.Sum(nil)...))
}
func decodeCursor(token string) (cursorData, error) {
	var c cursorData
	b, err := base64.RawURLEncoding.Strict().DecodeString(token)
	if err != nil || len(b) <= sha256.Size || len(b) > 2048 {
		return c, errors.New("invalid cursor")
	}
	payload, sig := b[:len(b)-sha256.Size], b[len(b)-sha256.Size:]
	mac := hmac.New(sha256.New, cursorKey)
	mac.Write([]byte("bog-cursor-v1\x00"))
	mac.Write(payload)
	if !hmac.Equal(sig, mac.Sum(nil)) || json.Unmarshal(payload, &c) != nil || c.Version != 1 {
		return c, errors.New("invalid cursor")
	}
	return c, nil
}

var decimal = regexp.MustCompile(`^[0-9]+$`)

func (a *API) query(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	select {
	case <-r.Context().Done():
		return
	default:
	}
	select {
	case a.querySlots <- struct{}{}:
		defer func() { <-a.querySlots }()
	default:
		http.Error(w, "query overloaded", http.StatusServiceUnavailable)
		return
	}
	ctx := r.Context()
	if a.queryTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, a.queryTimeout)
		defer cancel()
	}
	q, queryErr := url.ParseQuery(r.URL.RawQuery)
	if queryErr != nil {
		http.Error(w, "invalid query", 400)
		return
	}
	for k, v := range q {
		if (k != "start" && k != "end" && k != "contains" && k != "limit" && k != "page" && k != "cursor") || len(v) != 1 {
			http.Error(w, "invalid parameter", 400)
			return
		}
	}
	start, e1 := store.ParseTime(q.Get("start"))
	end, e2 := store.ParseTime(q.Get("end"))
	limit := 500
	var e3 error
	if v, ok := q["limit"]; ok {
		if !decimal.MatchString(v[0]) {
			e3 = errors.New("invalid limit")
		} else {
			limit, e3 = strconv.Atoi(v[0])
		}
	}
	if e1 != nil || e2 != nil || e3 != nil || end.Before(start) || limit < 1 || limit > 1000 {
		http.Error(w, "invalid query", 400)
		return
	}
	_, page := q["page"]
	cursor, continuation := q["cursor"]
	if (page && (q.Get("page") != "1" || continuation)) || (continuation && cursor[0] == "") {
		http.Error(w, "invalid pagination", 400)
		return
	}
	// Reserve space for a fixed-size cursor and ordinary HTTP headers within
	// the server's 1 MiB header limit. Preserve the request target as received:
	// re-encoding already valid query characters can inflate its size.
	if page {
		if len(r.RequestURI)+2048+4096 > 1<<20 {
			http.Error(w, "query too large for continuation", 400)
			return
		}
	}
	var rows []store.Row
	var nextCursor string
	if page || continuation {
		var count *int
		var after *store.Position
		if continuation {
			c, err := decodeCursor(cursor[0])
			if err != nil || !c.Start.Equal(start) || !c.End.Equal(end) || c.Contains != containsDigest(q.Get("contains")) {
				http.Error(w, "invalid cursor", 400)
				return
			}
			count, after = &c.Count, &c.After
		}
		var captured int
		var next *store.Position
		var ok bool
		var err error
		rows, captured, next, ok, err = a.store.QueryPageContext(ctx, start, end, q.Get("contains"), limit, count, after, a.limits)
		if err != nil {
			queryError(w, err)
			return
		}
		if !ok {
			http.Error(w, "invalid cursor", 400)
			return
		}
		if next != nil {
			nextCursor = encodeCursor(cursorData{1, start, end, containsDigest(q.Get("contains")), captured, *next})
		}
	} else {
		var err error
		rows, err = a.store.QueryContext(ctx, start, end, q.Get("contains"), limit, a.limits)
		if err != nil {
			queryError(w, err)
			return
		}
	}
	if err := ctx.Err(); err != nil {
		queryError(w, err)
		return
	}
	if nextCursor != "" {
		w.Header().Set("Bog-Next-Cursor", nextCursor)
	}
	// Bound each response by the server deadline even for slow readers.
	if deadline, ok := ctx.Deadline(); ok {
		_ = http.NewResponseController(w).SetWriteDeadline(deadline)
	}
	w.Header().Set("Content-Type", "application/x-ndjson")
	enc := json.NewEncoder(w)
	for _, row := range rows {
		if enc.Encode(row) != nil {
			return
		}
	}
}

func queryError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, store.ErrQueryBudget):
		http.Error(w, "query work budget exhausted", http.StatusUnprocessableEntity)
	case errors.Is(err, context.DeadlineExceeded):
		http.Error(w, "query deadline exceeded", http.StatusGatewayTimeout)
	case errors.Is(err, context.Canceled):
		http.Error(w, "query canceled", http.StatusRequestTimeout)
	default:
		http.Error(w, "query failed", http.StatusInternalServerError)
	}
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
