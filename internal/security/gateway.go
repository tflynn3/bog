// Package security implements an opt-in, read-only security boundary for Bog.
package security

import (
	"bytes"
	"context"
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
	"fmt"
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
	TokenSHA256 string   `json:"token_sha256"`
	Streams     []string `json:"streams"`
}
type Stream struct {
	URL    string `json:"url"`
	Policy string `json:"policy"`
}
type Rule struct {
	Pattern string `json:"pattern"`
	Action  string `json:"action"`
}
type Policy struct {
	Version string `json:"version"`
	Rules   []Rule `json:"rules"`
}
type Config struct {
	DefaultStream string               `json:"default_stream"`
	Streams       map[string]Stream    `json:"streams"`
	Principals    map[string]Principal `json:"principals"`
	Policies      map[string]Policy    `json:"policies"`
	CursorKey     string               `json:"cursor_key"`
	HashKey       string               `json:"hash_key"`
	AuditFile     string               `json:"audit_file"`
}
type compiledRule struct {
	re     *regexp.Regexp
	action string
}
type Gateway struct {
	cfg     Config
	rules   map[string][]compiledRule
	aead    cipher.AEAD
	hashKey []byte
	audit   *os.File
	mu      sync.Mutex
	client  *http.Client
	slots   chan struct{}
}
type cursor struct {
	Principal, Stream, Start, End, Filter, Policy, Upstream string
	Expires                                                 int64
}
type auditEvent struct {
	Time      time.Time `json:"timestamp"`
	RequestID string    `json:"request_id"`
	Actor     string    `json:"actor"`
	Stream    string    `json:"stream"`
	Action    string    `json:"action"`
	Decision  string    `json:"decision"`
	Status    int       `json:"status"`
}

func New(c Config) (*Gateway, error) {
	key, err := hex.DecodeString(c.CursorKey)
	if err != nil || len(key) != 32 {
		return nil, errors.New("cursor_key must be 32 bytes in hex")
	}
	block, _ := aes.NewCipher(key)
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	hk, err := hex.DecodeString(c.HashKey)
	if err != nil || len(hk) < 32 {
		return nil, errors.New("hash_key must be at least 32 bytes in hex")
	}
	if c.DefaultStream == "" || c.Streams[c.DefaultStream].URL == "" || len(c.Principals) == 0 {
		return nil, errors.New("default stream and principals required")
	}
	g := &Gateway{cfg: c, aead: aead, hashKey: hk, rules: map[string][]compiledRule{}, client: &http.Client{Timeout: 15 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}, slots: make(chan struct{}, 16)}
	for name, p := range c.Policies {
		if p.Version == "" {
			return nil, fmt.Errorf("policy %s has no version", name)
		}
		for _, r := range p.Rules {
			re, e := regexp.Compile(r.Pattern)
			if e != nil || r.Pattern == "" || (r.Action != "mask" && r.Action != "hash") {
				return nil, fmt.Errorf("invalid rule in %s", name)
			}
			g.rules[name] = append(g.rules[name], compiledRule{re, r.Action})
		}
	}
	endpoints := map[string]bool{}
	for name, s := range c.Streams {
		u, e := url.Parse(s.URL)
		if e != nil || u.Scheme != "https" || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
			return nil, fmt.Errorf("stream %s requires an HTTPS endpoint", name)
		}
		endpoint := strings.TrimRight(u.String(), "/")
		if endpoints[endpoint] {
			return nil, errors.New("streams must use distinct endpoints")
		}
		endpoints[endpoint] = true
		if _, ok := c.Policies[s.Policy]; !ok {
			return nil, fmt.Errorf("unknown policy for %s", name)
		}
	}
	credentials := map[string]bool{}
	for _, p := range c.Principals {
		b, e := hex.DecodeString(p.TokenSHA256)
		if credentials[strings.ToLower(p.TokenSHA256)] {
			return nil, errors.New("duplicate credential digest")
		}
		credentials[strings.ToLower(p.TokenSHA256)] = true
		if e != nil || len(b) != 32 {
			return nil, errors.New("invalid credential digest")
		}
		for _, s := range p.Streams {
			if _, ok := c.Streams[s]; !ok {
				return nil, errors.New("unknown principal stream")
			}
		}
	}
	f, err := os.OpenFile(c.AuditFile, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0600)
	if err != nil {
		return nil, err
	}
	st, err := f.Stat()
	if err != nil || !st.Mode().IsRegular() || st.Mode().Perm()&0077 != 0 {
		f.Close()
		return nil, errors.New("audit file must be private regular file")
	}
	g.audit = f
	return g, nil
}
func (g *Gateway) Close() error { return g.audit.Close() }
func (g *Gateway) record(e auditEvent) error {
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
	nonce := make([]byte, g.aead.NonceSize())
	if _, e = rand.Read(nonce); e != nil {
		return "", e
	}
	return base64.RawURLEncoding.EncodeToString(g.aead.Seal(nonce, nonce, b, nil)), nil
}
func (g *Gateway) unseal(s string) (cursor, error) {
	var c cursor
	if len(s) > 8192 {
		return c, errors.New("cursor too large")
	}
	b, e := base64.RawURLEncoding.Strict().DecodeString(s)
	if e != nil || len(b) < g.aead.NonceSize()+g.aead.Overhead() {
		return c, errors.New("invalid cursor")
	}
	b, e = g.aead.Open(nil, b[:g.aead.NonceSize()], b[g.aead.NonceSize():], nil)
	if e != nil {
		return c, e
	}
	e = json.Unmarshal(b, &c)
	return c, e
}
func (g *Gateway) redact(policy, line string) string {
	for _, r := range g.rules[policy] {
		line = r.re.ReplaceAllStringFunc(line, func(s string) string {
			if r.action == "mask" {
				return "[REDACTED]"
			}
			m := hmac.New(sha256.New, g.hashKey)
			m.Write([]byte(s))
			return "[HMAC-SHA256:" + hex.EncodeToString(m.Sum(nil)) + "]"
		})
	}
	return line
}
func (g *Gateway) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	idBytes := make([]byte, 16)
	if _, e := rand.Read(idBytes); e != nil {
		http.Error(w, "unavailable", 503)
		return
	}
	id := hex.EncodeToString(idBytes)
	w.Header().Set("X-Request-ID", id)
	event := auditEvent{Time: time.Now().UTC(), RequestID: id, Actor: "unknown", Action: "query", Decision: "deny", Status: 401}
	status := 401
	var body []byte
	next := ""
	defer func() {
		event.Status = status
		if e := g.record(event); e != nil {
			http.Error(w, "audit unavailable", 503)
			return
		}
		if next != "" {
			w.Header().Set("Bog-Next-Cursor", next)
		}
		if status == 200 {
			w.Header().Set("Content-Type", "application/x-ndjson")
			w.WriteHeader(status)
			_, _ = w.Write(body)
		} else {
			http.Error(w, http.StatusText(status), status)
		}
	}()
	if r.TLS == nil {
		status = 403
		return
	}
	if r.URL.Path != "/query" || r.Method != http.MethodGet {
		status = 404
		return
	}
	auth := r.Header.Get("Authorization")
	if !strings.HasPrefix(auth, "Bearer ") || len(auth) > 4096 {
		return
	}
	sum := sha256.Sum256([]byte(strings.TrimPrefix(auth, "Bearer ")))
	actor := ""
	for n, p := range g.cfg.Principals {
		digest, _ := hex.DecodeString(p.TokenSHA256)
		if subtle.ConstantTimeCompare(sum[:], digest) == 1 {
			actor = n
		}
	}
	if actor == "" {
		return
	}
	event.Actor = actor
	// Stream selection is the only request parsing performed before authorization.
	q, e := url.ParseQuery(r.URL.RawQuery)
	if e != nil {
		status = 400
		return
	}
	stream := q.Get("stream")
	if stream == "" {
		stream = g.cfg.DefaultStream
	}
	event.Stream = stream
	s, exists := g.cfg.Streams[stream]
	allowed := false
	for _, v := range g.cfg.Principals[actor].Streams {
		if v == stream {
			allowed = true
		}
	}
	if !exists || !allowed {
		status = 403
		return
	}
	event.Decision = "allow"
	select {
	case g.slots <- struct{}{}:
		defer func() { <-g.slots }()
	default:
		status = 503
		return
	}
	if len(r.URL.RawQuery) > 16384 {
		status = 400
		return
	}
	for k, v := range q {
		if len(v) != 1 || (k != "stream" && k != "start" && k != "end" && k != "contains" && k != "limit" && k != "page" && k != "cursor") {
			status = 400
			return
		}
	}
	start, e1 := store.ParseTime(q.Get("start"))
	end, e2 := store.ParseTime(q.Get("end"))
	limit := 500
	var e3 error
	if v, ok := q["limit"]; ok {
		limit, e3 = strconv.Atoi(v[0])
		if v[0] == "" || strings.Trim(v[0], "0123456789") != "" {
			e3 = errors.New("limit")
		}
	}
	if e1 != nil || e2 != nil || e3 != nil || end.Before(start) || limit < 1 || limit > 1000 || len(q.Get("contains")) > 4096 {
		status = 400
		return
	}
	_, page := q["page"]
	_, cont := q["cursor"]
	if page && (q.Get("page") != "1" || cont) || cont && q.Get("cursor") == "" {
		status = 400
		return
	}
	c := cursor{Principal: actor, Stream: stream, Start: start.Format(time.RFC3339Nano), End: end.Format(time.RFC3339Nano), Filter: q.Get("contains"), Policy: s.Policy + "/" + g.cfg.Policies[s.Policy].Version, Expires: time.Now().Add(15 * time.Minute).Unix()}
	if cont {
		old, err := g.unseal(q.Get("cursor"))
		if err != nil || old.Expires <= time.Now().Unix() || old.Principal != c.Principal || old.Stream != c.Stream || old.Start != c.Start || old.End != c.End || old.Filter != c.Filter || old.Policy != c.Policy || old.Upstream == "" {
			status = 400
			return
		}
		c = old
	}
	ctx, cancel := context.WithTimeout(r.Context(), 20*time.Second)
	defer cancel()
	var out bytes.Buffer
	found := 0
	more := true
	for scanned := 0; scanned < 1000 && found < limit && more; scanned++ {
		u, _ := url.Parse(s.URL)
		u.Path = strings.TrimRight(u.Path, "/") + "/query"
		v := url.Values{"start": {c.Start}, "end": {c.End}, "limit": {"1"}}
		if c.Upstream == "" {
			v.Set("page", "1")
		} else {
			v.Set("cursor", c.Upstream)
		}
		u.RawQuery = v.Encode()
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
		if err != nil {
			status = 502
			return
		}
		resp, err := g.client.Do(req)
		if err != nil {
			status = 502
			return
		}
		data, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20+1))
		resp.Body.Close()
		if err != nil || resp.StatusCode != 200 || len(data) > 4<<20 {
			status = 502
			return
		}
		c.Upstream = resp.Header.Get("Bog-Next-Cursor")
		more = c.Upstream != ""
		if len(data) > 0 {
			var row store.Row
			if json.Unmarshal(bytes.TrimSpace(data), &row) != nil {
				status = 502
				return
			}
			row.Line = g.redact(s.Policy, row.Line)
			if strings.Contains(row.Line, c.Filter) {
				b, err := json.Marshal(row)
				if err != nil || out.Len()+len(b)+1 > 4<<20 {
					status = 503
					return
				}
				out.Write(b)
				out.WriteByte('\n')
				found++
			}
		}
	}
	if more {
		next, e = g.seal(c)
		if e != nil {
			status = 503
			return
		}
	}
	body = out.Bytes()
	status = 200
}
