package main

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"bog/internal/client"
	"bog/internal/httpapi"
	"bog/internal/store"
)

// fixtureSource gives the real HTTP paging handler an independently ordered
// in-memory view. Its opaque positions are one-based indexes into that view.
type fixtureSource []client.Row

func (s fixtureSource) Release(string) error { return nil }

func (s fixtureSource) Page(ctx context.Context, req httpapi.PageRequest, work *store.QueryWork) (httpapi.PageResult, error) {
	var offset int
	if req.After != "" {
		var err error
		offset, err = strconv.Atoi(string(req.After))
		if err != nil || offset < 1 || offset > len(s) {
			return httpapi.PageResult{}, fmt.Errorf("invalid fixture position %q", req.After)
		}
	}
	result := httpapi.PageResult{Capture: "fixture"}
	for i := offset; i < len(s); i++ {
		if err := work.Charge(1); err != nil {
			return httpapi.PageResult{}, err
		}
		row := s[i]
		f := req.Filter
		if row.TS.Before(f.Start) || !row.TS.Before(f.End) || !strings.Contains(row.Line, f.Contains) {
			continue
		}
		if len(result.Rows) == req.Limit {
			result.More = true
			break
		}
		result.Rows = append(result.Rows, httpapi.PageRow{Row: row, Position: httpapi.PagePosition(strconv.Itoa(i + 1))})
	}
	return result, ctx.Err()
}

func TestCLIWithRealBackupAndQueryServer(t *testing.T) {
	t.Parallel()
	dir, rows, _ := makeBundle(t)

	// Stable sorting retains append order, and the response cap forces smaller live
	// pages than the offline reader. Ties cross both page and storage boundaries.
	slices.SortStableFunc(rows, func(a, b client.Row) int { return a.TS.Compare(b.TS) })
	api := httpapi.NewWithPageSource(fixtureSource(rows), nil, 80, 1, httpapi.DefaultQueryWorkBudget)
	server := httptest.NewServer(http.StripPrefix("/bog", api))
	t.Cleanup(func() {
		server.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		check(t, api.Close(ctx))
	})
	for _, tc := range []struct {
		name  string
		extra []string
		want  string
		exit  int
	}{
		{"paged parity", nil, "rows=4 reference_pages=2 server_pages=4", 0},
		{"case-sensitive empty result", []string{"-contains", "KEEP-"}, "rows=0 reference_pages=1 server_pages=1", 0},
		{"equal bounds", []string{"-end", fixtureStart}, "rows=0 reference_pages=1 server_pages=1", 0},
		{"budget exhausted", []string{"-max-pages", "3"}, "server walk at row 4", 4},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			args := append(compareArgs(dir, server.URL+"/bog"), tc.extra...)
			status := run(t.Context(), args, &stdout, &stderr)
			if status != tc.exit || !strings.Contains(stdout.String()+stderr.String(), tc.want) {
				t.Fatalf("compare exit %d, stdout %q, stderr %q", status, &stdout, &stderr)
			}
			if status != 0 && stdout.Len() != 0 {
				t.Fatalf("failure emitted success output %q", &stdout)
			}
		})
	}
}
