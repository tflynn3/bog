package client

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"bog/internal/httpapi"
	"bog/internal/store"
)

type loopbackAppender struct{ store *store.Store }

func (a loopbackAppender) Append(_ []byte, rows []store.Row) error {
	a.store.Publish(rows)
	return nil
}

func loopbackBatch(t *testing.T, rows ...Row) []byte {
	t.Helper()
	var body []byte
	for _, row := range rows {
		record, err := json.Marshal(struct {
			TS   time.Time `json:"ts"`
			Line string    `json:"line"`
		}{row.TS, row.Line})
		if err != nil {
			t.Fatal(err)
		}
		body = append(body, record...)
		body = append(body, '\n')
	}
	return body
}

func TestLoopbackByteLimitedSnapshot(t *testing.T) {
	st := new(store.Store)
	stamp := time.Date(2025, time.January, 1, 0, 0, 0, 123456789, time.UTC)
	line := func(c string) string { return c + strings.Repeat("é", 2048) }
	early := Row{TS: stamp, Line: line("E")}
	firstTie := Row{TS: stamp.Add(time.Second), Line: line("A")}
	secondTie := Row{TS: firstTie.TS, Line: line("B")}
	late := Row{TS: stamp.Add(2 * time.Second), Line: line("L")}

	// Fit exactly two encoded records. The row limit would allow every row,
	// including the three tied records, to be selected at once.
	byteLimit := len(loopbackBatch(t, early, firstTie))
	server := httptest.NewServer(httpapi.NewWithQueryMaxResponseBytes(st, loopbackAppender{st}, byteLimit))
	defer server.Close()
	c, err := New(server.URL, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer c.http.CloseIdleConnections()
	c.MaxPageBytes = int64(byteLimit)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	q := Query{Start: stamp.Add(-time.Second), End: late.TS, Contains: "é", Limit: 10}
	if got, err := c.Ingest(ctx, loopbackBatch(t, firstTie, late, early, secondTie, firstTie)); err != nil || got != IngestCommitted {
		t.Fatalf("initial ingest outcome %v, error %v", got, err)
	}
	first, err := c.FetchPage(ctx, q, "")
	if err != nil || first.NextCursor == "" || !slices.Equal(first.Rows, []Row{early, firstTie}) {
		t.Fatalf("first page has %d rows, cursor %t, error %v", len(first.Rows), first.NextCursor != "", err)
	}

	// Insert both an earlier row and a tie after capturing the snapshot.
	backfill := Row{TS: early.TS, Line: line("X")}
	newTie := Row{TS: firstTie.TS, Line: line("Y")}
	if got, err := c.Ingest(ctx, loopbackBatch(t, backfill, newTie)); err != nil || got != IngestCommitted {
		t.Fatalf("late ingest outcome %v, error %v", got, err)
	}
	want := []Row{secondTie, firstTie, late}
	// Independent retries of the same cursor share the client and must each
	// observe the same ordered snapshot and terminal cursor.
	var walks sync.WaitGroup
	for i := 0; i < 4; i++ {
		walks.Go(func() {
			var rows []Row
			var pageSizes []int
			var cursors []string
			resume, err := c.WalkFrom(ctx, q, first.NextCursor, func(page Page) error {
				rows = append(rows, page.Rows...)
				pageSizes = append(pageSizes, len(page.Rows))
				cursors = append(cursors, page.NextCursor)
				return nil
			})
			if err != nil || resume != "" || !slices.Equal(pageSizes, []int{2, 1}) ||
				!slices.Equal(rows, want) || cursors[0] == "" || cursors[0] == first.NextCursor || cursors[1] != "" {
				t.Errorf("walk %d returned %v rows per page, final cursor %q, error %v", i, pageSizes, resume, err)
			}
		})
	}
	walks.Wait()

	t.Run("empty result is terminal", func(t *testing.T) {
		q.Contains = "absent"
		pages := 0
		cursor, err := c.Walk(ctx, q, func(p Page) error {
			pages++
			if len(p.Rows) != 0 || p.NextCursor != "" {
				return fmt.Errorf("empty query returned %d rows and cursor %q", len(p.Rows), p.NextCursor)
			}
			return nil
		})
		if cursor != "" || err != nil || pages != 1 {
			t.Fatalf("empty walk returned %q after %d pages, error %v", cursor, pages, err)
		}
	})
}
