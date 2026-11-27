package main

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestAdvanceChecksLivePublication(t *testing.T) {
	for _, tc := range []struct {
		name, statusBody string
		wantErr          bool
	}{
		{"exact", `{"state":"complete","generation":2,"covered_rows":12,"committed_rows":12}`, false},
		{"old generation", `{"state":"complete","generation":1,"covered_rows":12,"committed_rows":12}`, true},
		{"wrong cutoff", `{"state":"complete","generation":2,"covered_rows":11,"committed_rows":12}`, true},
		{"unexpected commit", `{"state":"complete","generation":2,"covered_rows":12,"committed_rows":13}`, true},
		{"missing committed count", `{"state":"complete","generation":2,"covered_rows":12}`, true},
		{"malformed", "{}{}", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				if req.URL.Path != controlPath || req.URL.Query().Get("cutoff") != "12" {
					t.Error("wrong control URL", req.URL)
					http.Error(w, "bad URL", http.StatusBadRequest)
					return
				}
				switch req.Method + " " + req.URL.Query().Get("action") {
				case "POST start":
					_, err := io.WriteString(w, `{"state":"running","generation":1,"covered_rows":8,"committed_rows":12}`)
					if err != nil {
						t.Error(err)
					}
				case "GET status":
					if _, err := io.WriteString(w, tc.statusBody); err != nil {
						t.Error(err)
					}
				default:
					t.Error("unexpected control operation", req.Method, req.URL)
					http.Error(w, "bad operation", http.StatusBadRequest)
				}
			}))
			defer srv.Close()
			s := liveService{client: srv.Client(), base: srv.URL}
			if err := s.advance(t.Context(), 12, 1); (err != nil) != tc.wantErr {
				t.Fatalf("advance: %v", err)
			}
		})
	}
}

func TestControlHTTPFailures(t *testing.T) {
	t.Parallel()
	for _, status := range []int{http.StatusNotFound, http.StatusServiceUnavailable, http.StatusOK} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(status)
				if status == http.StatusOK {
					if _, err := io.WriteString(w, strings.Repeat("x", (64<<10)+1)); err != nil {
						t.Error(err)
					}
				}
			}))
			defer srv.Close()
			client := srv.Client()
			client.Timeout = time.Second
			s := liveService{client: client, base: srv.URL}
			_, code, err := s.control(t.Context(), http.MethodGet, "status", 0)
			if code != status || (status == http.StatusOK) != (err != nil) {
				t.Fatalf("control: HTTP %d, %v", code, err)
			}
		})
	}
}
