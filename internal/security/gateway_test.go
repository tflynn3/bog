package security

import (
	"bytes"
	"crypto/tls"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
)

func TestAuthorizationAndRedactedFilter(t *testing.T) {
	calls := 0
	upstream := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if r.URL.Query().Get("contains") != "" {
			t.Error("raw filter sent upstream")
		}
		io.WriteString(w, `{"ts":"2025-01-01T00:00:00Z","line":"secret-123"}`+"\n")
	}))
	defer upstream.Close()
	var audit bytes.Buffer
	c := Config{Streams: map[string]string{"default": upstream.URL}, DefaultStream: "default", Principals: []Principal{{ID: "alice", Token: strings.Repeat("a", 32), Read: []string{"default"}}}, Rules: []Rule{{Pattern: `secret-[0-9]+`, Mode: "mask"}}, PolicyVersion: "1", Key: strings.Repeat("01", 32)}
	g, e := New(c, &audit)
	if e != nil {
		t.Fatal(e)
	}
	g.client = upstream.Client()
	q := url.Values{"start": {"2024-01-01T00:00:00Z"}, "end": {"2026-01-01T00:00:00Z"}, "contains": {"secret-123"}}
	req := httptest.NewRequest("GET", "https://gateway/query?"+q.Encode(), nil)
	req.TLS = &tls.ConnectionState{}
	req.Header.Set("Authorization", "Bearer "+strings.Repeat("a", 32))
	rec := httptest.NewRecorder()
	g.ServeHTTP(rec, req)
	if rec.Code != 200 || rec.Body.Len() != 0 || calls != 1 {
		t.Fatalf("status=%d body=%q calls=%d", rec.Code, rec.Body.String(), calls)
	}
	if strings.Contains(audit.String(), "secret-123") || strings.Contains(audit.String(), strings.Repeat("a", 32)) {
		t.Fatal("audit contains secret")
	}
	req = httptest.NewRequest("POST", "https://gateway/ingest", strings.NewReader("bad"))
	req.TLS = &tls.ConnectionState{}
	req.Header.Set("Authorization", "Bearer "+strings.Repeat("a", 32))
	rec = httptest.NewRecorder()
	g.ServeHTTP(rec, req)
	if rec.Code != 403 || calls != 1 {
		t.Fatal("write was not independently denied")
	}
}
func TestCursorIntegrity(t *testing.T) {
	c := Config{Streams: map[string]string{"s": "https://localhost:1234"}, DefaultStream: "s", PolicyVersion: "1", Key: strings.Repeat("01", 32)}
	g, e := New(c, io.Discard)
	if e != nil {
		t.Fatal(e)
	}
	token, e := g.token(cursor{Principal: "alice", Expires: 4102444800})
	if e != nil {
		t.Fatal(e)
	}
	b := []byte(token)
	b[len(b)/2] ^= 1
	if _, e = g.untoken(string(b)); e == nil {
		t.Fatal("tampered cursor accepted")
	}
}
