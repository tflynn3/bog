package httpapi

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strconv"

	"bog/internal/store"
	"bog/internal/wal"
)

type appender interface {
	Append([]byte, []store.Row) error
}
type API struct {
	store *store.Store
	wal   appender
	fatal func()
	slots chan struct{}
}

func New(s *store.Store, w appender) http.Handler {
	a := &API{store: s, wal: w, fatal: func() { os.Exit(1) }, slots: make(chan struct{}, 16)}
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

var decimal = regexp.MustCompile(`^[0-9]+$`)

func (a *API) query(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	q, queryErr := url.ParseQuery(r.URL.RawQuery)
	if queryErr != nil {
		http.Error(w, "invalid query", 400)
		return
	}
	for k, v := range q {
		if (k != "start" && k != "end" && k != "contains" && k != "limit") || len(v) != 1 {
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
	rows := a.store.Query(start, end, q.Get("contains"), limit)
	w.Header().Set("Content-Type", "application/x-ndjson")
	enc := json.NewEncoder(w)
	for _, row := range rows {
		if enc.Encode(row) != nil {
			return
		}
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
