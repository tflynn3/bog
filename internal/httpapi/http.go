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
	store            *store.Store
	wal              appender
	fatal            func()
	slots            chan struct{}
	maxResponseBytes int
	queryWorkBudget  uint64
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
// commit returns 408 "request cancelled". These errors use plain text.
func NewWithQueryLimits(s *store.Store, w appender, maxBytes, concurrency int, workBudget uint64) http.Handler {
	if maxBytes <= 0 {
		panic("query response byte limit must be positive")
	}
	if workBudget == 0 {
		panic("query work budget must be positive")
	}
	a := &API{
		store: s, wal: w, fatal: func() { os.Exit(1) }, slots: make(chan struct{}, 16),
		maxResponseBytes: maxBytes, queryWorkBudget: workBudget,
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", a.health)
	mux.HandleFunc("/ingest", a.ingest)
	query := queryAdmission(concurrency, http.HandlerFunc(a.query))
	mux.HandleFunc("/query", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		query.ServeHTTP(w, r)
	})
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

func encodeCursor(c cursorData) (string, error) {
	b, err := json.Marshal(c)
	if err != nil {
		return "", err
	}
	mac := hmac.New(sha256.New, cursorKey)
	mac.Write([]byte("bog-cursor-v1\x00"))
	mac.Write(b)
	return base64.RawURLEncoding.EncodeToString(append(b, mac.Sum(nil)...)), nil
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

func writeQueryError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, store.ErrQueryWorkExhausted):
		http.Error(w, "query work budget exhausted", http.StatusServiceUnavailable)
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		http.Error(w, "request cancelled", http.StatusRequestTimeout)
	case errors.Is(err, store.ErrInvalidQueryCursor):
		http.Error(w, "invalid cursor", http.StatusBadRequest)
	default:
		http.Error(w, "query failed", http.StatusInternalServerError)
	}
}

func (a *API) query(w http.ResponseWriter, r *http.Request) {
	work := store.NewQueryWork(r.Context(), a.queryWorkBudget)
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
	var captured int
	var anchor, next *store.Position
	var digest [sha256.Size]byte
	var err error
	contains := q.Get("contains")
	if page || continuation {
		var count *int
		if continuation {
			// Charge one unit for token validation and a unit per input byte
			// for decoding and authenticating the token and its literal.
			if err := work.Charge(1, uint64(len(cursor[0])), uint64(len(contains))); err != nil {
				writeQueryError(w, err)
				return
			}
			c, err := decodeCursor(cursor[0])
			digest = containsDigest(contains)
			if err != nil || !c.Start.Equal(start) || !c.End.Equal(end) || c.Contains != digest {
				http.Error(w, "invalid cursor", 400)
				return
			}
			count, anchor = &c.Count, &c.After
		}
		if page {
			// The one-row call captures both the snapshot and a usable anchor.
			rows, captured, next, err = a.store.QueryPageWithWork(start, end, contains, 1, nil, nil, work)
			if err == nil && next != nil && limit > 1 {
				anchor = next
				var rest []store.Row
				rest, _, next, err = a.store.QueryPageWithWork(start, end, contains, limit-1, &captured, anchor, work)
				rows = append(rows, rest...)
			}
		} else {
			rows, captured, next, err = a.store.QueryPageWithWork(start, end, contains, limit, count, anchor, work)
		}
	} else {
		rows, err = a.store.QueryWithWork(start, end, contains, limit, work)
	}
	if err != nil {
		writeQueryError(w, err)
		return
	}
	response := queryResponse{maxBytes: a.maxResponseBytes}
	emitted := 0
	for _, row := range rows {
		fits, err := response.appendRow(row, work)
		if err != nil {
			writeQueryError(w, err)
			return
		}
		if !fits {
			if emitted == 0 {
				http.Error(w, "query record exceeds response byte limit", http.StatusUnprocessableEntity)
				return
			}
			break
		}
		emitted++
	}
	if (page || continuation) && emitted < len(rows) {
		// QueryPage only exposes a position when another matching row exists.
		// Since a fetched row was omitted, the shorter call has that lookahead.
		shortLimit := emitted
		if page {
			shortLimit--
		}
		if shortLimit == 0 {
			next = anchor
		} else {
			_, _, next, err = a.store.QueryPageWithWork(start, end, contains, shortLimit, &captured, anchor, work)
			if err != nil {
				writeQueryError(w, err)
				return
			}
		}
	}
	var token string
	if next != nil {
		if page {
			if err := work.Charge(uint64(len(contains))); err != nil {
				writeQueryError(w, err)
				return
			}
			digest = containsDigest(contains)
		}
		token, err = encodeCursor(cursorData{1, start, end, digest, captured, *next})
		if err != nil {
			writeQueryError(w, err)
			return
		}
	}
	// Reserve the entire encoded body before committing status or cursor.
	delivery := (uint64(len(response.body)) + queryDeliveryBytesPerUnit - 1) / queryDeliveryBytesPerUnit
	if err := work.Charge(delivery); err != nil {
		writeQueryError(w, err)
		return
	}
	if token != "" {
		w.Header().Set("Bog-Next-Cursor", token)
	}
	w.Header().Set("Content-Type", "application/x-ndjson")
	if _, err := w.Write(response.body); err != nil {
		// The response may already be committed. Return and release the slot.
		return
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
