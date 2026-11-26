package main

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"bog/internal/httpapi"
	"bog/internal/store"
)

type memoryAppender struct{ store *store.Store }

func (a memoryAppender) Append(_ []byte, rows []store.Row) error {
	a.store.Publish(rows)
	return nil
}

func TestProbeSaturationRealTCP(t *testing.T) {
	for _, admitted := range []bool{true, false} {
		t.Run(fmt.Sprintf("one_slot=%t", admitted), func(t *testing.T) {
			st := &store.Store{}
			api := httpapi.New(st, memoryAppender{st})
			slot := make(chan struct{}, 1)
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if admitted && r.URL.Path == "/query" {
					select {
					case slot <- struct{}{}:
						defer func() { <-slot }()
					default:
						http.Error(w, "busy", http.StatusServiceUnavailable)
						return
					}
				}
				api.ServeHTTP(w, r)
			}))
			defer srv.Close()
			ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
			defer cancel()
			seed := make([]row, seedRows)
			padding := strings.Repeat("x", payloadBytes)
			for i := range seed {
				seed[i] = row{fixtureStart, fmt.Sprintf("seed-%04d-%s", i, padding)}
			}
			cases := make(map[string]outcome)
			err := probeSaturation(ctx, srv.Client(), srv.URL, seed, cases)
			if admitted {
				if err != nil {
					t.Fatalf("failed real TCP fixture: %v (cases %+v)", err, cases)
				}
				for _, name := range admissionCases[:3] {
					if cases[name].Status != "pass" {
						t.Errorf("case %s: %+v", name, cases[name])
					}
				}
			} else if err == nil || cases["one_slot_saturation"].Status != "fail" {
				t.Fatalf("accepted server without query admission: %v", err)
			}
		})
	}
}
