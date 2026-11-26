// Package security implements an opt-in security boundary for private Bog servers.
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
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"bog/internal/store"
)

type Principal struct {
	ID    string   `json:"id"`
	Token string   `json:"token"`
	Read  []string `json:"read"`
	Write []string `json:"write"`
}
type Rule struct {
	Pattern string `json:"pattern"`
	Mode    string `json:"mode"`
	re      *regexp.Regexp
}
type Config struct {
	Streams       map[string]string `json:"streams"`
	DefaultStream string            `json:"default_stream"`
	Principals    []Principal       `json:"principals"`
	Rules         []Rule            `json:"rules"`
	PolicyVersion string            `json:"policy_version"`
	Key           string            `json:"key"`
	AuditPath     string            `json:"audit_path"`
}
type Gateway struct {
	cfg    Config
	key    []byte
	aead   cipher.AEAD
	client *http.Client
	audit  io.Writer
	mu     sync.Mutex
	slots  chan struct{}
}
type cursor struct {
	Principal, Stream, Start, End, Filter, Policy, Upstream string
	Expires                                                 int64
}

// New validates the entire configuration before accepting requests. audit must
// be an append-only durable writer whose Write returns only after sync.
func New(c Config, audit io.Writer) (*Gateway, error) {
	key, e := hex.DecodeString(c.Key)
	if e != nil || len(key) != 32 || audit == nil || c.PolicyVersion == "" || len(c.Streams) == 0 {
		return nil, errors.New("invalid security configuration")
	}
	if _, ok := c.Streams[c.DefaultStream]; !ok {
		return nil, errors.New("default stream missing")
	}
	seen := map[string]bool{}
	tokens := map[string]bool{}
	endpoints := map[string]bool{}
	for name, raw := range c.Streams {
		u, e := url.Parse(raw)
		if name == "" || e != nil || u.Scheme != "https" || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Path != "" && u.Path != "/") || endpoints[u.Host] {
			return nil, errors.New("invalid or duplicate stream endpoint")
		}
		endpoints[u.Host] = true
	}
	for _, p := range c.Principals {
		if p.ID == "" || len(p.Token) < 32 || seen[p.ID] || tokens[p.Token] {
			return nil, errors.New("invalid principal")
		}
		seen[p.ID] = true
		tokens[p.Token] = true
		for _, s := range append(append([]string{}, p.Read...), p.Write...) {
			if _, ok := c.Streams[s]; !ok {
				return nil, errors.New("unknown stream grant")
			}
		}
	}
	for i := range c.Rules {
		r := &c.Rules[i]
		r.re, e = regexp.Compile(r.Pattern)
		if e != nil || r.Pattern == "" || (e == nil && r.re.MatchString("")) || (r.Mode != "mask" && r.Mode != "hash") {
			return nil, errors.New("invalid redaction rule")
		}
	}
	block, _ := aes.NewCipher(key)
	aead, _ := cipher.NewGCM(block)
	return &Gateway{cfg: c, key: key, aead: aead, audit: audit, client: &http.Client{Timeout: 30 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}, slots: make(chan struct{}, 16)}, nil
}
func (g *Gateway) redact(s string) string {
	for _, r := range g.cfg.Rules {
		s = r.re.ReplaceAllStringFunc(s, func(v string) string {
			if r.Mode == "mask" {
				return "[REDACTED]"
			}
			h := hmac.New(sha256.New, g.key)
			h.Write([]byte("bog-pii-v1\x00"))
			h.Write([]byte(v))
			return "[HASH:" + hex.EncodeToString(h.Sum(nil)) + "]"
		})
	}
	return s
}
func (g *Gateway) token(c cursor) (string, error) {
	b, e := json.Marshal(c)
	if e != nil {
		return "", e
	}
	n := make([]byte, g.aead.NonceSize())
	if _, e = rand.Read(n); e != nil {
		return "", e
	}
	return base64.RawURLEncoding.EncodeToString(g.aead.Seal(n, n, b, []byte("bog-gateway-v1"))), nil
}
func (g *Gateway) untoken(s string) (cursor, error) {
	var c cursor
	if len(s) > 8192 {
		return c, errors.New("invalid cursor")
	}
	b, e := base64.RawURLEncoding.Strict().DecodeString(s)
	if e != nil || len(b) < g.aead.NonceSize()+g.aead.Overhead() {
		return c, errors.New("invalid cursor")
	}
	b, e = g.aead.Open(nil, b[:g.aead.NonceSize()], b[g.aead.NonceSize():], []byte("bog-gateway-v1"))
	if e != nil {
		return c, e
	}
	e = json.Unmarshal(b, &c)
	if e != nil || time.Now().Unix() >= c.Expires {
		return c, errors.New("invalid cursor")
	}
	return c, nil
}
func grant(a []string, s string) bool {
	for _, v := range a {
		if v == s {
			return true
		}
	}
	return false
}
func (g *Gateway) event(actor, stream, action, decision, id string, status int) error {
	b, _ := json.Marshal(struct {
		Time      time.Time `json:"timestamp"`
		Actor     string    `json:"actor"`
		Stream    string    `json:"stream"`
		Action    string    `json:"action"`
		Decision  string    `json:"decision"`
		RequestID string    `json:"request_id"`
		Status    int       `json:"status"`
	}{time.Now().UTC(), actor, stream, action, decision, id, status})
	g.mu.Lock()
	defer g.mu.Unlock()
	n, e := g.audit.Write(append(b, '\n'))
	if e == nil && n != len(b)+1 {
		e = io.ErrShortWrite
	}
	return e
}
func (g *Gateway) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	idbytes := make([]byte, 16)
	if _, e := rand.Read(idbytes); e != nil {
		http.Error(w, "unavailable", 503)
		return
	}
	id := hex.EncodeToString(idbytes)
	w.Header().Set("X-Request-ID", id)
	action := "unknown"
	if r.URL.Path == "/query" && r.Method == http.MethodGet {
		action = "read"
	}
	if r.URL.Path == "/ingest" && r.Method == http.MethodPost {
		action = "write"
	}
	stream := r.Header.Get("Bog-Stream")
	if stream == "" {
		stream = g.cfg.DefaultStream
	}
	if len(stream) > 128 {
		stream = "invalid"
	}
	actor := "anonymous"
	var principal *Principal
	if r.TLS != nil && strings.HasPrefix(r.Header.Get("Authorization"), "Bearer ") {
		token := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		for i := range g.cfg.Principals {
			p := &g.cfg.Principals[i]
			a := sha256.Sum256([]byte(token))
			b := sha256.Sum256([]byte(p.Token))
			if subtle.ConstantTimeCompare(a[:], b[:]) == 1 {
				principal = p
				actor = p.ID
			}
		}
	}
	allowed := principal != nil && ((action == "read" && grant(principal.Read, stream)) || (action == "write" && grant(principal.Write, stream)))
	if _, ok := g.cfg.Streams[stream]; !ok {
		allowed = false
	}
	if !allowed {
		if g.event(actor, stream, action, "deny", id, 403) != nil {
			http.Error(w, "unavailable", 503)
			return
		}
		http.Error(w, "forbidden", 403)
		return
	}
	if g.event(actor, stream, action, "allow", id, 0) != nil {
		http.Error(w, "unavailable", 503)
		return
	}
	select {
	case g.slots <- struct{}{}:
		defer func() { <-g.slots }()
	default:
		if g.event(actor, stream, action, "outcome", id, 503) != nil {
			http.Error(w, "unavailable", 503)
			return
		}
		http.Error(w, "busy", 503)
		return
	}
	var body []byte
	var status int
	var next string
	if action == "write" {
		status = g.ingest(r, stream)
	} else {
		body, next, status = g.query(r, stream, actor)
	}
	if g.event(actor, stream, action, "outcome", id, status) != nil {
		http.Error(w, "unavailable", 503)
		return
	}
	if status != 200 && status != 204 {
		http.Error(w, http.StatusText(status), status)
		return
	}
	if next != "" {
		w.Header().Set("Bog-Next-Cursor", next)
	}
	if status == 200 {
		w.Header().Set("Content-Type", "application/x-ndjson")
	}
	w.WriteHeader(status)
	_, _ = w.Write(body)
}
func (g *Gateway) ingest(r *http.Request, stream string) int {
	b, e := io.ReadAll(io.LimitReader(r.Body, store.MaxBatch+1))
	if e != nil {
		return 400
	}
	if len(b) > store.MaxBatch {
		return 413
	}
	rows, e := store.Decode(b)
	if e != nil {
		return 400
	}
	var out bytes.Buffer
	enc := json.NewEncoder(&out)
	for _, row := range rows {
		row.Line = g.redact(row.Line)
		if enc.Encode(row) != nil {
			return 400
		}
	}
	if out.Len() > store.MaxBatch {
		return 413
	}
	req, e := http.NewRequestWithContext(r.Context(), "POST", strings.TrimRight(g.cfg.Streams[stream], "/")+"/ingest", &out)
	if e != nil {
		return 503
	}
	resp, e := g.client.Do(req)
	if e != nil {
		return 503
	}
	defer resp.Body.Close()
	if resp.StatusCode != 204 {
		return 503
	}
	return 204
}
func (g *Gateway) query(r *http.Request, stream, actor string) ([]byte, string, int) {
	q, e := url.ParseQuery(r.URL.RawQuery)
	if e != nil || len(r.URL.RawQuery) > 16384 {
		return nil, "", 400
	}
	for k, v := range q {
		if (k != "start" && k != "end" && k != "contains" && k != "limit" && k != "page" && k != "cursor") || len(v) != 1 {
			return nil, "", 400
		}
	}
	start, e1 := store.ParseTime(q.Get("start"))
	end, e2 := store.ParseTime(q.Get("end"))
	limit := 500
	if v := q.Get("limit"); v != "" {
		limit, e = strconv.Atoi(v)
		if e != nil {
			return nil, "", 400
		}
	}
	if e1 != nil || e2 != nil || end.Before(start) || limit < 1 || limit > 1000 || len(q.Get("contains")) > 4096 {
		return nil, "", 400
	}
	_, page := q["page"]
	_, cont := q["cursor"]
	if (page && (q.Get("page") != "1" || cont)) || (cont && q.Get("cursor") == "") {
		return nil, "", 400
	}
	c := cursor{Principal: actor, Stream: stream, Start: start.Format(time.RFC3339Nano), End: end.Format(time.RFC3339Nano), Filter: q.Get("contains"), Policy: g.cfg.PolicyVersion, Expires: time.Now().Add(15 * time.Minute).Unix()}
	if cont {
		old, err := g.untoken(q.Get("cursor"))
		if err != nil || old.Principal != c.Principal || old.Stream != c.Stream || old.Start != c.Start || old.End != c.End || old.Filter != c.Filter || old.Policy != c.Policy {
			return nil, "", 400
		}
		c = old
	}
	var out bytes.Buffer
	more := true
	count := 0
	// A bounded walk of unfiltered upstream pages ensures filters see only the
	// redacted view; an empty page with a cursor signals work-budget exhaustion.
	for work := 0; work < 1000 && count < limit && more; work++ {
		p := url.Values{"start": {c.Start}, "end": {c.End}, "limit": {"1"}}
		if c.Upstream == "" {
			p.Set("page", "1")
		} else {
			p.Set("cursor", c.Upstream)
		}
		req, err := http.NewRequestWithContext(r.Context(), "GET", strings.TrimRight(g.cfg.Streams[stream], "/")+"/query?"+p.Encode(), nil)
		if err != nil {
			return nil, "", 503
		}
		resp, err := g.client.Do(req)
		if err != nil {
			return nil, "", 503
		}
		b, err := io.ReadAll(io.LimitReader(resp.Body, store.MaxBatch+1))
		resp.Body.Close()
		if err != nil || len(b) > store.MaxBatch || resp.StatusCode != 200 {
			return nil, "", 503
		}
		c.Upstream = resp.Header.Get("Bog-Next-Cursor")
		more = c.Upstream != ""
		if len(b) > 0 {
			var row store.Row
			if json.Unmarshal(bytes.TrimSpace(b), &row) != nil {
				return nil, "", 503
			}
			row.Line = g.redact(row.Line)
			if strings.Contains(row.Line, c.Filter) {
				encoded, err := json.Marshal(row)
				if err != nil || out.Len()+len(encoded)+1 > 8<<20 {
					return nil, "", 503
				}
				out.Write(encoded)
				out.WriteByte('\n')
				count++
			}
		}
	}
	if more {
		if !page && !cont {
			return nil, "", 503
		}
		t, err := g.token(c)
		if err != nil {
			return nil, "", 503
		}
		return out.Bytes(), t, 200
	}
	return out.Bytes(), "", 200
}
