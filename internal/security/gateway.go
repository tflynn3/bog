// Package security implements the opt-in, read-only stream gateway.
package security

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
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
	ID          string   `json:"id"`
	TokenSHA256 string   `json:"token_sha256"`
	Read        []string `json:"read"`
}
type Stream struct {
	Endpoint string `json:"endpoint"`
}
type Rule struct {
	Pattern     string `json:"pattern"`
	Replacement string `json:"replacement"`
	re          *regexp.Regexp
}
type Config struct {
	Streams       map[string]Stream `json:"streams"`
	DefaultStream string            `json:"default_stream"`
	Principals    []Principal       `json:"principals"`
	Rules         []Rule            `json:"rules"`
	PolicyVersion string            `json:"policy_version"`
	CursorKey     string            `json:"cursor_key"`
	AuditPath     string            `json:"audit_path"`
}
type Gateway struct {
	c      Config
	aead   cipher.AEAD
	audit  *os.File
	mu     sync.Mutex
	client *http.Client
	slots  chan struct{}
}
type cursor struct {
	Principal, Stream, Start, End, Filter, Policy, Backend string
	Expires                                                int64
}

func New(c Config) (*Gateway, error) {
	if c.DefaultStream == "" || c.Streams[c.DefaultStream].Endpoint == "" || c.PolicyVersion == "" || c.AuditPath == "" {
		return nil, errors.New("missing required configuration")
	}
	endpoints := map[string]bool{}
	for name, s := range c.Streams {
		u, e := url.Parse(s.Endpoint)
		if name == "" || e != nil || u.Scheme != "https" || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Path != "" && u.Path != "/") {
			return nil, errors.New("stream endpoints must be HTTPS origins")
		}
		endpoint := strings.TrimSuffix(s.Endpoint, "/")
		if endpoints[endpoint] {
			return nil, errors.New("duplicate stream endpoint")
		}
		endpoints[endpoint] = true
	}
	seen := map[string]bool{}
	for _, p := range c.Principals {
		b, e := hex.DecodeString(p.TokenSHA256)
		if p.ID == "" || e != nil || len(b) != 32 || seen[p.ID] {
			return nil, errors.New("invalid principal")
		}
		seen[p.ID] = true
		for _, s := range p.Read {
			if _, ok := c.Streams[s]; !ok {
				return nil, errors.New("unknown authorized stream")
			}
		}
	}
	for i := range c.Rules {
		r, e := regexp.Compile(c.Rules[i].Pattern)
		if e != nil || c.Rules[i].Pattern == "" {
			return nil, errors.New("invalid redaction rule")
		}
		c.Rules[i].re = r
	}
	key, e := hex.DecodeString(c.CursorKey)
	if e != nil || len(key) != 32 {
		return nil, errors.New("cursor key must be 32-byte hex")
	}
	block, e := aes.NewCipher(key)
	if e != nil {
		return nil, e
	}
	a, e := cipher.NewGCM(block)
	if e != nil {
		return nil, e
	}
	f, e := os.OpenFile(c.AuditPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0600)
	if e != nil {
		return nil, e
	}
	info, e := f.Stat()
	if e != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 {
		f.Close()
		return nil, errors.New("audit file must be private regular file")
	}
	return &Gateway{c: c, aead: a, audit: f, client: &http.Client{Timeout: 15 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}, slots: make(chan struct{}, 16)}, nil
}
func (g *Gateway) Close() error { return g.audit.Close() }
func (g *Gateway) event(actor, stream, decision, outcome, id string) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	b, _ := json.Marshal(map[string]string{"actor": actor, "stream": stream, "action": "query", "decision": decision, "outcome": outcome, "timestamp": time.Now().UTC().Format(time.RFC3339Nano), "request_id": id})
	b = append(b, '\n')
	if _, e := g.audit.Write(b); e != nil {
		return e
	}
	return g.audit.Sync()
}
func (g *Gateway) seal(c cursor) (string, error) {
	b, e := json.Marshal(c)
	if e != nil {
		return "", e
	}
	n := make([]byte, g.aead.NonceSize())
	if _, e = rand.Read(n); e != nil {
		return "", e
	}
	return base64.RawURLEncoding.EncodeToString(g.aead.Seal(n, n, b, nil)), nil
}
func (g *Gateway) open(s string) (cursor, error) {
	var c cursor
	if len(s) > 8192 {
		return c, errors.New("cursor too large")
	}
	b, e := base64.RawURLEncoding.Strict().DecodeString(s)
	if e != nil || len(b) < g.aead.NonceSize() {
		return c, errors.New("invalid cursor")
	}
	b, e = g.aead.Open(nil, b[:g.aead.NonceSize()], b[g.aead.NonceSize():], nil)
	if e != nil {
		return c, e
	}
	e = json.Unmarshal(b, &c)
	if e != nil || c.Expires < time.Now().Unix() {
		return c, errors.New("expired cursor")
	}
	return c, nil
}
func (g *Gateway) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	idBytes := make([]byte, 16)
	if _, e := rand.Read(idBytes); e != nil {
		http.Error(w, "unavailable", 503)
		return
	}
	id := hex.EncodeToString(idBytes)
	w.Header().Set("X-Request-ID", id)
	stream := r.Header.Get("Bog-Stream")
	if stream == "" {
		stream = g.c.DefaultStream
	}
	auditStream := stream
	if _, ok := g.c.Streams[stream]; !ok {
		auditStream = "unknown"
	}
	actor := "anonymous"
	decision := "deny"
	outcome := "denied"
	status := http.StatusForbidden
	var body []byte
	var next string
	// Audit is synchronous and must succeed before any response is released.
	defer func() {
		if e := g.event(actor, auditStream, decision, outcome, id); e != nil {
			http.Error(w, "audit unavailable", 503)
			return
		}
		if next != "" {
			w.Header().Set("Bog-Next-Cursor", next)
		}
		if status != 200 {
			http.Error(w, http.StatusText(status), status)
			return
		}
		w.Header().Set("Content-Type", "application/x-ndjson")
		w.WriteHeader(200)
		_, _ = w.Write(body)
	}()
	if r.TLS == nil {
		status = 400
		outcome = "transport_required"
		return
	}
	token := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
	if token == "" || r.Header.Get("Authorization") != "Bearer "+token {
		status = 401
		outcome = "unauthenticated"
		return
	}
	sum := sha256.Sum256([]byte(token))
	var p *Principal
	for i := range g.c.Principals {
		b, _ := hex.DecodeString(g.c.Principals[i].TokenSHA256)
		if hmac.Equal(sum[:], b) {
			p = &g.c.Principals[i]
		}
	}
	if p == nil {
		status = 401
		outcome = "unauthenticated"
		return
	}
	actor = p.ID
	authorized := false
	for _, s := range p.Read {
		if s == stream {
			authorized = true
		}
	}
	backend, exists := g.c.Streams[stream]
	if !authorized || !exists {
		return
	}
	decision = "allow"
	if r.URL.Path != "/query" || r.Method != http.MethodGet {
		status = 405
		outcome = "unsupported"
		return
	}
	select {
	case g.slots <- struct{}{}:
		defer func() { <-g.slots }()
	default:
		status = 503
		outcome = "busy"
		return
	}
	q, e := url.ParseQuery(r.URL.RawQuery)
	if e != nil || len(r.URL.RawQuery) > 16384 {
		status = 400
		outcome = "invalid_query"
		return
	}
	for k, v := range q {
		if len(v) != 1 || (k != "start" && k != "end" && k != "contains" && k != "limit" && k != "page" && k != "cursor") {
			status = 400
			outcome = "invalid_query"
			return
		}
	}
	start, e1 := store.ParseTime(q.Get("start"))
	end, e2 := store.ParseTime(q.Get("end"))
	limit := 500
	if q.Has("limit") {
		limit, e = strconv.Atoi(q.Get("limit"))
	}
	if e1 != nil || e2 != nil || e != nil || end.Before(start) || limit < 1 || limit > 1000 || (q.Has("page") && (q.Get("page") != "1" || q.Has("cursor"))) || (q.Has("cursor") && q.Get("cursor") == "") {
		status = 400
		outcome = "invalid_query"
		return
	}
	c := cursor{Principal: actor, Stream: stream, Start: start.Format(time.RFC3339Nano), End: end.Format(time.RFC3339Nano), Filter: q.Get("contains"), Policy: g.c.PolicyVersion, Expires: time.Now().Add(15 * time.Minute).Unix()}
	if q.Has("cursor") {
		old, err := g.open(q.Get("cursor"))
		if err != nil || old.Principal != c.Principal || old.Stream != c.Stream || old.Start != c.Start || old.End != c.End || old.Filter != c.Filter || old.Policy != c.Policy {
			status = 400
			outcome = "invalid_cursor"
			return
		}
		c = old
	}
	var out bytes.Buffer
	more := true
	// A bounded scan yields a continuation even when no visible rows matched.
	for work := 0; work < 1000 && out.Len() < 4<<20 && limit > 0 && more; work++ {
		params := url.Values{"start": {c.Start}, "end": {c.End}, "limit": {"1"}}
		if c.Backend == "" {
			params.Set("page", "1")
		} else {
			params.Set("cursor", c.Backend)
		}
		req, err := http.NewRequestWithContext(r.Context(), "GET", strings.TrimSuffix(backend.Endpoint, "/")+"/query?"+params.Encode(), nil)
		if err != nil {
			status = 502
			outcome = "backend_error"
			return
		}
		resp, err := g.client.Do(req)
		if err != nil {
			status = 502
			outcome = "backend_error"
			return
		}
		b, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20+1))
		resp.Body.Close()
		if err != nil || resp.StatusCode != 200 || len(b) > 4<<20 {
			status = 502
			outcome = "backend_error"
			return
		}
		c.Backend = resp.Header.Get("Bog-Next-Cursor")
		more = c.Backend != ""
		if len(b) > 0 {
			var row store.Row
			if json.Unmarshal(bytes.TrimSpace(b), &row) != nil {
				status = 502
				outcome = "backend_error"
				return
			}
			for _, rule := range g.c.Rules {
				row.Line = rule.re.ReplaceAllString(row.Line, rule.Replacement)
			}
			if strings.Contains(row.Line, c.Filter) {
				encoded, err := json.Marshal(row)
				if err != nil || out.Len()+len(encoded)+1 > 4<<20 {
					status = 502
					outcome = "response_too_large"
					return
				}
				out.Write(encoded)
				out.WriteByte('\n')
				limit--
			}
		}
	}
	if more {
		next, e = g.seal(c)
		if e != nil {
			status = 503
			outcome = "cursor_error"
			return
		}
	}
	body = out.Bytes()
	status = 200
	outcome = "success"
}
