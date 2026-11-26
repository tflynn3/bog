package security

import (
	"crypto/tls"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"testing"
)

func TestRedactedFilterAndAuthorization(t *testing.T) {
	calls := 0
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		w.Header().Set("Content-Type", "application/x-ndjson")
		w.Write([]byte(`{"ts":"2025-01-01T00:00:00Z","line":"email alice@example.com"}` + "\n"))
	}))
	defer upstream.Close()
	path := t.TempDir() + "/audit"
	c := Config{Streams: map[string]string{"one": upstream.URL}, DefaultStream: "one", Principals: map[string]Principal{"alice": {Token: strings.Repeat("a", 32), Streams: []string{"one"}}}, Rules: []Rule{{Pattern: `[a-z]+@example\.com`, Replacement: "[masked]"}}, PolicyVersion: "1", CursorKey: strings.Repeat("01", 32), HashKey: strings.Repeat("02", 32), AuditPath: path}
	g, e := New(c)
	if e != nil {
		t.Fatal(e)
	}
	defer g.Close()
	query := url.Values{"start": {"2025-01-01T00:00:00Z"}, "end": {"2025-01-02T00:00:00Z"}, "contains": {"alice@example.com"}}
	req := httptest.NewRequest("GET", "https://gateway/query?"+query.Encode(), nil)
	req.TLS = &tls.ConnectionState{}
	req.Header.Set("Authorization", "Bearer "+strings.Repeat("a", 32))
	w := httptest.NewRecorder()
	g.ServeHTTP(w, req)
	if w.Code != 200 || w.Body.Len() != 0 {
		t.Fatalf("status %d body %s", w.Code, w.Body.String())
	}
	query.Set("contains", "[masked]")
	req = httptest.NewRequest("GET", "https://gateway/query?"+query.Encode(), nil)
	req.TLS = &tls.ConnectionState{}
	req.Header.Set("Authorization", "Bearer "+strings.Repeat("a", 32))
	w = httptest.NewRecorder()
	g.ServeHTTP(w, req)
	if w.Code != 200 || !strings.Contains(w.Body.String(), "[masked]") || strings.Contains(w.Body.String(), "alice@example.com") {
		t.Fatalf("status %d body %s", w.Code, w.Body.String())
	}
	req = httptest.NewRequest("GET", "https://gateway/query?stream=other", nil)
	req.TLS = &tls.ConnectionState{}
	req.Header.Set("Authorization", "Bearer "+strings.Repeat("a", 32))
	w = httptest.NewRecorder()
	g.ServeHTTP(w, req)
	if w.Code != 403 || calls != 2 {
		t.Fatalf("unauthorized access: %d calls %d", w.Code, calls)
	}
	audit, e := os.ReadFile(path)
	if e != nil {
		t.Fatal(e)
	}
	if strings.Contains(string(audit), "alice@example.com") || strings.Contains(string(audit), strings.Repeat("a", 32)) {
		t.Fatal("audit leaked secret")
	}
	for _, line := range strings.Split(strings.TrimSpace(string(audit)), "\n") {
		var v event
		if json.Unmarshal([]byte(line), &v) != nil || v.RequestID == "" {
			t.Fatal("invalid audit")
		}
	}
}
func TestCursorIsBoundAndTamperProtected(t *testing.T) {
	g := &Gateway{key: make([]byte, 32)}
	token, e := g.seal(cursor{Actor: "alice", Stream: "one"})
	if e != nil {
		t.Fatal(e)
	}
	c, e := g.open(token)
	if e != nil || c.Actor != "alice" || c.Stream != "one" {
		t.Fatal("bad cursor")
	}
	b := []byte(token)
	b[len(b)/2] ^= 1
	if _, e = g.open(string(b)); e == nil {
		t.Fatal("tampered cursor accepted")
	}
}
