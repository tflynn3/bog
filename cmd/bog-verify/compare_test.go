package main

import (
	"context"
	"io"
	"strings"
	"testing"
	"time"

	"bog/internal/client"
)

func TestCompareStreams(t *testing.T) {
	t.Parallel()
	a := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	b := a.Add(time.Nanosecond)
	c := b.Add(time.Nanosecond)
	first := client.Row{TS: a, Line: "b"}
	tie := client.Row{TS: a, Line: "a"}
	last := client.Row{TS: b, Line: "b"}
	page := func(terminal bool, rows ...client.Row) queryPage {
		return queryPage{rows, terminal}
	}
	for _, tc := range []struct {
		name            string
		reference, live []queryPage
		status          int
		matched         uint64
		diagnostic      string
	}{
		{"different boundaries preserve ties and duplicates",
			[]queryPage{page(false, first, tie), page(true, tie, last)},
			[]queryPage{page(false, first), page(false, tie, tie), page(true, last)}, exitOK, 4, ""},
		{"explicit empty terminal on both sides",
			[]queryPage{page(true)}, []queryPage{page(true)}, exitOK, 0, ""},
		{"empty terminal after matching rows",
			[]queryPage{page(false, first), page(true)}, []queryPage{page(true, first)}, exitOK, 1, ""},
		{"terminal evidence missing on server",
			[]queryPage{page(true, first)}, []queryPage{page(false, first)}, exitIncomplete, 1, "server walk at row 2: page 2: EOF"},
		{"terminal evidence missing on reference",
			[]queryPage{page(false, first)}, []queryPage{page(true, first)}, exitIncomplete, 1, "reference walk at row 2: page 2: EOF"},
		{"empty page with continuation",
			[]queryPage{page(true)}, []queryPage{page(false)}, exitIncomplete, 0, "empty page with continuation"},
		{"tied timestamps reordered",
			[]queryPage{page(true, first, tie)}, []queryPage{page(true, tie, first)}, exitMismatch, 0, "line mismatch at row 1"},
		{"ordering crosses a page boundary",
			[]queryPage{page(true, last, client.Row{TS: c, Line: "end"})},
			[]queryPage{page(false, last), page(true, first)}, exitMismatch, 1, "ordering mismatch at row 2: server"},
		{"wrong timestamp with same line",
			[]queryPage{page(true, first)}, []queryPage{page(true, last)}, exitMismatch, 0, "timestamp mismatch at row 1"},
		{"server missing row",
			[]queryPage{page(true, first, tie)}, []queryPage{page(true, first)}, exitMismatch, 1, "length mismatch at row 2: server exhausted"},
		{"server extra row",
			[]queryPage{page(true, first)}, []queryPage{page(false, first), page(true, tie)}, exitMismatch, 1, "length mismatch at row 2: reference exhausted"},
		{"first mismatch stops before continuation",
			[]queryPage{page(false, first)}, []queryPage{page(false, tie)}, exitMismatch, 0, "line mismatch at row 1"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			stream := func(pages []queryPage) pageStream {
				return func(context.Context) (queryPage, error) {
					if len(pages) == 0 {
						return queryPage{}, io.EOF
					}
					p := pages[0]
					pages = pages[1:]
					return p, nil
				}
			}
			got, status, err := compare(t.Context(), stream(tc.reference), stream(tc.live))
			if status != tc.status || got.matched != tc.matched {
				t.Fatalf("compare = (%+v, %d, %v), want %d matched, status %d", got, status, err, tc.matched, tc.status)
			}
			if tc.diagnostic == "" {
				if err != nil || got.referencePages != len(tc.reference) || got.livePages != len(tc.live) {
					t.Fatalf("walk did not complete both streams: %+v, %v", got, err)
				}
			} else if err == nil || !strings.Contains(err.Error(), tc.diagnostic) {
				t.Fatalf("got %v, want %q", err, tc.diagnostic)
			}
		})
	}
}

func TestCompareCancellationAfterPageRead(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	ref := func(context.Context) (queryPage, error) {
		cancel()
		return queryPage{terminal: true}, nil
	}
	live := func(context.Context) (queryPage, error) {
		t.Error("live stream called after cancellation")
		return queryPage{terminal: true}, nil
	}
	got, status, err := compare(ctx, ref, live)
	if got != (comparison{}) || status != exitIncomplete || err == nil || !strings.Contains(err.Error(), "context canceled") {
		t.Fatalf("canceled comparison = %+v, %d, %v", got, status, err)
	}
}

func TestLineMismatchShowsLateDifference(t *testing.T) {
	prefix := strings.Repeat("x", 1024)
	err := lineMismatch(12, client.Row{Line: prefix + "\nfirst"}, client.Row{Line: prefix + "\tsecond"})
	message := err.Error()
	if !strings.Contains(message, "row 12") || !strings.Contains(message, "byte 1025") ||
		!strings.Contains(message, `\nfirst"`) || !strings.Contains(message, `\tsecond"`) ||
		len(message) > 300 || strings.ContainsAny(message, "\n\t") {
		t.Fatalf("unexpected line mismatch diagnostic: %q", message)
	}
}
