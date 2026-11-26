package security

import (
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
)

func TestAccessAndRedaction(t *testing.T) {
	backend := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, `{"ts":"2025-01-01T00:00:00Z","line":"secret=123"}`+"\n")
	}))
	defer backend.Close()
	hash := sha256.Sum256([]byte("credential"))
	path := t.TempDir() + "/audit"
	cfg := Config{Streams: map[string]Stream{"default": {Endpoint: backend.URL}}, DefaultStream: "default", Principals: []Principal{{ID: "alice", TokenSHA256: hex.EncodeToString(hash[:]), Read: []string{"default"}}}, Rules: []Rule{{Pattern: `123`, Replacement: "***"}}, PolicyVersion: "1", CursorKey: strings.Repeat("01", 32), AuditPath: path}
	g, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()
	g.client = backend.Client()
	query := "/query?start=2025-01-01T00:00:00Z&end=2025-01-02T00:00:00Z&contains=123"
	req := httptest.NewRequest("GET", "https://gateway"+query, nil)
	req.Header.Set("Authorization", "Bearer credential")
	rec := httptest.NewRecorder()
	g.ServeHTTP(rec, req)
	if rec.Code != 200 || rec.Body.Len() != 0 {
		t.Fatalf("%d %s", rec.Code, rec.Body.String())
	}
	req = httptest.NewRequest("GET", "https://gateway"+query, nil)
	req.Header.Set("Authorization", "Bearer wrong")
	rec = httptest.NewRecorder()
	g.ServeHTTP(rec, req)
	if rec.Code != 401 {
		t.Fatal(rec.Code)
	}
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(b), "123") || strings.Contains(string(b), "credential") {
		t.Fatal("audit contains secret")
	}
}
