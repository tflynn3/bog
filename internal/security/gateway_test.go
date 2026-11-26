package security

import (
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
)

func TestGatewayAuthorizationRedactionAndAudit(t *testing.T) {
	calls := 0
	upstream := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if r.URL.Query().Get("contains") != "" {
			t.Error("filter sent upstream")
		}
		w.Write([]byte(`{"ts":"2025-01-01T00:00:00Z","line":"email alice@example.com"}` + "\n"))
	}))
	defer upstream.Close()
	sum := sha256.Sum256([]byte("credential"))
	audit := t.TempDir() + "/audit"
	c := Config{DefaultStream: "legacy", Streams: map[string]Stream{"legacy": {URL: upstream.URL, Policy: "p"}}, Principals: map[string]Principal{"alice": {TokenSHA256: hex.EncodeToString(sum[:]), Streams: []string{"legacy"}}}, Policies: map[string]Policy{"p": {Version: "1", Rules: []Rule{{Pattern: `[a-z]+@example\.com`, Action: "mask"}}}}, CursorKey: strings.Repeat("01", 32), HashKey: strings.Repeat("02", 32), AuditFile: audit}
	g, err := New(c)
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()
	g.client = upstream.Client()
	query := "/query?start=2025-01-01T00:00:00Z&end=2025-01-02T00:00:00Z&contains=alice"
	request := func(token string) *httptest.ResponseRecorder {
		r := httptest.NewRequest("GET", query, nil)
		r.TLS = &tls.ConnectionState{}
		r.Header.Set("Authorization", "Bearer "+token)
		w := httptest.NewRecorder()
		g.ServeHTTP(w, r)
		return w
	}
	if w := request("bad"); w.Code != 401 || calls != 0 {
		t.Fatalf("unauthorized: %d calls %d", w.Code, calls)
	}
	if w := request("credential"); w.Code != 200 || w.Body.Len() != 0 || calls != 1 {
		t.Fatalf("hidden filter: %d %q calls %d", w.Code, w.Body.String(), calls)
	}
	query = strings.Replace(query, "contains=alice", "contains=REDACTED", 1)
	if w := request("credential"); w.Code != 200 || !strings.Contains(w.Body.String(), "[REDACTED]") || strings.Contains(w.Body.String(), "alice") {
		t.Fatalf("redaction: %d %q", w.Code, w.Body.String())
	}
	b, err := os.ReadFile(audit)
	if err != nil || strings.Contains(string(b), "credential") || strings.Contains(string(b), "alice@example") || strings.Contains(string(b), "REDACTED") {
		t.Fatalf("audit leaked: %s %v", b, err)
	}
}
