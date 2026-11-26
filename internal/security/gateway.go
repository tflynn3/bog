// Package security implements an opt-in, fail-closed query gateway.
package security

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"bog/internal/store"
)

type Principal struct {
	Token   string   `json:"token"`
	Streams []string `json:"streams"`
}
type Rule struct {
	Pattern     string `json:"pattern"`
	Replacement string `json:"replacement"`
	Hash        bool   `json:"hash"`
	re          *regexp.Regexp
}
type Config struct {
	Streams       map[string]string    `json:"streams"`
	DefaultStream string               `json:"default_stream"`
	Principals    map[string]Principal `json:"principals"`
	Rules         []Rule               `json:"rules"`
	PolicyVersion string               `json:"policy_version"`
	CursorKey     string               `json:"cursor_key"`
	HashKey       string               `json:"hash_key"`
	AuditPath     string               `json:"audit_path"`
}
type Gateway struct {
	cfg     Config
	key     []byte
	hashKey []byte
	audit   *os.File
	mu      sync.Mutex
	client  *http.Client
	slots   chan struct{}
}
type cursor struct {
	Actor, Stream, Start, End, Filter, Policy, Upstream string
	Expires                                             int64
}
type event struct {
	Actor     string    `json:"actor"`
	Stream    string    `json:"stream"`
	Action    string    `json:"action"`
	Decision  string    `json:"decision"`
	Outcome   string    `json:"outcome"`
	Time      time.Time `json:"timestamp"`
	RequestID string    `json:"request_id"`
}

func New(c Config) (*Gateway, error) {
	key, e := hex.DecodeString(c.CursorKey)
	if e != nil || len(key) != 32 {
		return nil, errors.New("cursor_key must be 32 bytes hex")
	}
	hk, e := hex.DecodeString(c.HashKey)
	if e != nil || len(hk) < 32 {
		return nil, errors.New("hash_key must be at least 32 bytes hex")
	}
	if c.PolicyVersion == "" || c.AuditPath == "" || len(c.Streams) == 0 || len(c.Principals) == 0 {
		return nil, errors.New("missing required configuration")
	}
	if _, ok := c.Streams[c.DefaultStream]; !ok {
		return nil, errors.New("default_stream must name a stream")
	}
	endpoints := map[string]bool{}
	for name, raw := range c.Streams {
		u, err := url.Parse(raw)
		if name == "" || err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Path != "" && u.Path != "/") || endpoints[raw] {
			return nil, errors.New("invalid or duplicate stream endpoint")
		}
		endpoints[raw] = true
	}
	tokens := map[string]bool{}
	for name, p := range c.Principals {
		if name == "" || name == "unknown" || len(p.Token) < 32 || tokens[p.Token] {
			return nil, errors.New("invalid principal")
		}
		tokens[p.Token] = true
		for _, s := range p.Streams {
			if _, ok := c.Streams[s]; !ok {
				return nil, errors.New("unknown stream grant")
			}
		}
	}
	for i := range c.Rules {
		r := &c.Rules[i]
		r.re, e = regexp.Compile(r.Pattern)
		if e != nil || r.Pattern == "" || r.re.MatchString("") {
			return nil, errors.New("invalid rule")
		}
	}
	f, e := os.OpenFile(c.AuditPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if e != nil {
		return nil, e
	}
	st, e := f.Stat()
	if e != nil || !st.Mode().IsRegular() || st.Mode().Perm()&0077 != 0 {
		f.Close()
		return nil, errors.New("audit file must be private and regular")
	}
	return &Gateway{cfg: c, key: key, hashKey: hk, audit: f, client: &http.Client{Timeout: 30 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}, slots: make(chan struct{}, 16)}, nil
}
func (g *Gateway) Close() error { return g.audit.Close() }
func (g *Gateway) record(e event) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	b, err := json.Marshal(e)
	if err != nil {
		return err
	}
	b = append(b, '\n')
	n, err := g.audit.Write(b)
	if err != nil {
		return err
	}
	if n != len(b) {
		return io.ErrShortWrite
	}
	return g.audit.Sync()
}
func (g *Gateway) seal(c cursor) (string, error) {
	b, e := json.Marshal(c)
	if e != nil {
		return "", e
	}
	block, e := aes.NewCipher(g.key)
	if e != nil {
		return "", e
	}
	a, e := cipher.NewGCM(block)
	if e != nil {
		return "", e
	}
	nonce := make([]byte, a.NonceSize())
	if _, e = rand.Read(nonce); e != nil {
		return "", e
	}
	return base64.RawURLEncoding.EncodeToString(a.Seal(nonce, nonce, b, nil)), nil
}
func (g *Gateway) open(s string) (cursor, error) {
	var c cursor
	if len(s) > 8192 {
		return c, errors.New("invalid cursor")
	}
	b, e := base64.RawURLEncoding.Strict().DecodeString(s)
	if e != nil {
		return c, e
	}
	block, _ := aes.NewCipher(g.key)
	a, _ := cipher.NewGCM(block)
	if len(b) < a.NonceSize()+a.Overhead() {
		return c, errors.New("invalid cursor")
	}
	b, e = a.Open(nil, b[:a.NonceSize()], b[a.NonceSize():], nil)
	if e != nil {
		return c, e
	}
	e = json.Unmarshal(b, &c)
	return c, e
}
func (g *Gateway) redact(s string) string {
	for _, r := range g.cfg.Rules {
		s = r.re.ReplaceAllStringFunc(s, func(v string) string {
			if !r.Hash {
				return r.Replacement
			}
			h := hmac.New(sha256.New, g.hashKey)
			h.Write([]byte(v))
			return hex.EncodeToString(h.Sum(nil))
		})
	}
	return s
}
func (g *Gateway) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	id := make([]byte, 16)
	if _, e := rand.Read(id); e != nil {
		http.Error(w, "unavailable", 503)
		return
	}
	ev := event{Actor: "unknown", Action: "query", Decision: "deny", Outcome: "denied", Time: time.Now().UTC(), RequestID: hex.EncodeToString(id)}
	w.Header().Set("X-Request-ID", ev.RequestID)
	status := http.StatusForbidden
	var output []byte
	var next string
	defer func() {
		if err := g.record(ev); err != nil {
			http.Error(w, "audit unavailable", 503)
			return
		}
		if status != 200 {
			http.Error(w, http.StatusText(status), status)
			return
		}
		w.Header().Set("Content-Type", "application/x-ndjson")
		if next != "" {
			w.Header().Set("Bog-Next-Cursor", next)
		}
		w.WriteHeader(status)
		_, _ = w.Write(output)
	}()
	if r.TLS == nil {
		status = 400
		ev.Outcome = "transport_required"
		return
	}
	if r.Method != "GET" || r.URL.Path != "/query" {
		status = 405
		ev.Outcome = "unsupported"
		return
	}
	auth := r.Header.Get("Authorization")
	if !strings.HasPrefix(auth, "Bearer ") {
		status = 401
		ev.Outcome = "unauthenticated"
		return
	}
	token := strings.TrimPrefix(auth, "Bearer ")
	var p Principal
	for name, candidate := range g.cfg.Principals {
		if subtle.ConstantTimeCompare([]byte(token), []byte(candidate.Token)) == 1 {
			ev.Actor = name
			p = candidate
		}
	}
	if ev.Actor == "unknown" {
		status = 401
		ev.Outcome = "unauthenticated"
		return
	}
	q, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		status = 400
		ev.Outcome = "invalid_query"
		return
	}
	for k, v := range q {
		if len(v) != 1 || (k != "stream" && k != "start" && k != "end" && k != "contains" && k != "limit" && k != "page" && k != "cursor") {
			status = 400
			ev.Outcome = "invalid_query"
			return
		}
	}
	stream := q.Get("stream")
	if stream == "" {
		stream = g.cfg.DefaultStream
	}
	if _, ok := g.cfg.Streams[stream]; ok {
		ev.Stream = stream
	} else {
		ev.Stream = "unknown"
	}
	allowed := false
	for _, s := range p.Streams {
		if s == stream {
			allowed = true
		}
	}
	endpoint, exists := g.cfg.Streams[stream]
	if !allowed || !exists {
		return
	}
	ev.Decision = "allow"
	select {
	case g.slots <- struct{}{}:
		defer func() { <-g.slots }()
	default:
		status = 503
		ev.Outcome = "busy"
		return
	}
	start, e1 := store.ParseTime(q.Get("start"))
	end, e2 := store.ParseTime(q.Get("end"))
	limit := 500
	if v, ok := q["limit"]; ok {
		limit, err = strconv.Atoi(v[0])
	}
	if e1 != nil || e2 != nil || err != nil || end.Before(start) || limit < 1 || limit > 1000 || len(q.Get("contains")) > 4096 || len(r.URL.RawQuery) > 16384 {
		status = 400
		ev.Outcome = "invalid_query"
		return
	}
	_, page := q["page"]
	_, hasCursor := q["cursor"]
	if (page && (q.Get("page") != "1" || hasCursor)) || (hasCursor && q.Get("cursor") == "") {
		status = 400
		ev.Outcome = "invalid_query"
		return
	}
	c := cursor{Actor: ev.Actor, Stream: stream, Start: start.Format(time.RFC3339Nano), End: end.Format(time.RFC3339Nano), Filter: q.Get("contains"), Policy: g.cfg.PolicyVersion, Expires: time.Now().Add(15 * time.Minute).Unix()}
	if hasCursor {
		c2, e := g.open(q.Get("cursor"))
		if e != nil || c2.Actor != c.Actor || c2.Stream != c.Stream || c2.Start != c.Start || c2.End != c.End || c2.Filter != c.Filter || c2.Policy != c.Policy || c2.Expires <= time.Now().Unix() {
			status = 400
			ev.Outcome = "invalid_cursor"
			return
		}
		c = c2
	}
	// Fetch one upstream row at a time to preserve its snapshot and exact append position.
	var buf bytes.Buffer
	remaining := 256
	count := 0
	more := true
	for remaining > 0 && count < limit && more {
		remaining--
		vals := url.Values{"start": {c.Start}, "end": {c.End}, "limit": {"1"}}
		if c.Upstream == "" {
			vals.Set("page", "1")
		} else {
			vals.Set("cursor", c.Upstream)
		}
		req, e := http.NewRequestWithContext(r.Context(), "GET", strings.TrimRight(endpoint, "/")+"/query?"+vals.Encode(), nil)
		if e != nil {
			status = 502
			ev.Outcome = "upstream_error"
			return
		}
		resp, e := g.client.Do(req)
		if e != nil {
			status = 502
			ev.Outcome = "upstream_error"
			return
		}
		b, e := io.ReadAll(io.LimitReader(resp.Body, 4<<20+1))
		resp.Body.Close()
		if e != nil || resp.StatusCode != 200 || len(b) > 4<<20 {
			status = 502
			ev.Outcome = "upstream_error"
			return
		}
		upstream := resp.Header.Get("Bog-Next-Cursor")
		more = upstream != ""
		c.Upstream = upstream
		if len(b) > 0 {
			var row store.Row
			if json.Unmarshal(bytes.TrimSpace(b), &row) != nil {
				status = 502
				ev.Outcome = "upstream_error"
				return
			}
			row.Line = g.redact(row.Line)
			if strings.Contains(row.Line, c.Filter) {
				encoded, e := json.Marshal(row)
				if e != nil || buf.Len()+len(encoded)+1 > 4<<20 {
					status = 502
					ev.Outcome = "response_too_large"
					return
				}
				buf.Write(encoded)
				buf.WriteByte('\n')
				count++
			}
		}
	}
	if more {
		if !page && !hasCursor {
			status = 503
			ev.Outcome = "pagination_required"
			return
		}
		next, err = g.seal(c)
		if err != nil {
			status = 503
			ev.Outcome = "unavailable"
			return
		}
	}
	output = buf.Bytes()
	status = 200
	ev.Outcome = "success"
}
