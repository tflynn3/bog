package main

import (
	"context"
	"fmt"
	"time"

	"bog/internal/client"
)

const (
	exitOK           = 0
	exitUsage        = 2
	exitVerification = 3
	exitIncomplete   = 4
	exitMismatch     = 5
)

// queryPage represents one validated page. terminal is the source's explicit
// exhaustion signal and may accompany rows.
type queryPage struct {
	rows     []client.Row
	terminal bool
}

type pageStream func(context.Context) (queryPage, error)

type rowStream struct {
	next     pageStream
	rows     []client.Row
	terminal bool
	pages    int
}

// take pulls only when the current page is drained. An empty terminal page
// establishes exhaustion, including when the first page is empty.
func (s *rowStream) take(ctx context.Context) (client.Row, bool, error) {
	if err := ctx.Err(); err != nil {
		return client.Row{}, false, err
	}
	if len(s.rows) == 0 && !s.terminal {
		p, err := s.next(ctx)
		if err != nil {
			return client.Row{}, false, fmt.Errorf("page %d: %w", s.pages+1, err)
		}
		if err := ctx.Err(); err != nil {
			return client.Row{}, false, err
		}
		if len(p.rows) == 0 && !p.terminal {
			return client.Row{}, false, fmt.Errorf("page %d: empty page with continuation", s.pages+1)
		}
		s.rows, s.terminal = p.rows, p.terminal
		s.pages++
	}
	if len(s.rows) == 0 {
		return client.Row{}, false, nil
	}
	row := s.rows[0]
	s.rows = s.rows[1:]
	if len(s.rows) == 0 {
		s.rows = nil
	}
	return row, true, nil
}

type comparison struct {
	matched                   uint64
	referencePages, livePages int
}

// compare reads at most one page per side and compares rows in their emitted
// order. Equal timestamps are compared in place, preserving append order.
func compare(ctx context.Context, reference, live pageStream) (result comparison, status int, err error) {
	ref := rowStream{next: reference}
	server := rowStream{next: live}
	defer func() {
		result.referencePages = ref.pages
		result.livePages = server.pages
	}()
	var previous time.Time
	for {
		n := result.matched + 1
		want, haveWant, err := ref.take(ctx)
		if err != nil {
			return result, exitIncomplete, fmt.Errorf("reference walk at row %d: %w", n, err)
		}
		got, haveGot, err := server.take(ctx)
		if err != nil {
			return result, exitIncomplete, fmt.Errorf("server walk at row %d: %w", n, err)
		}
		if !haveWant && !haveGot {
			return result, exitOK, nil
		}
		if result.matched > 0 {
			if haveWant && want.TS.Before(previous) {
				return result, exitMismatch, orderMismatch(n, "reference", previous, want.TS)
			}
			if haveGot && got.TS.Before(previous) {
				return result, exitMismatch, orderMismatch(n, "server", previous, got.TS)
			}
		}
		switch {
		case !haveWant:
			return result, exitMismatch, fmt.Errorf("length mismatch at row %d: reference exhausted, server returned timestamp %s", n, timestamp(got.TS))
		case !haveGot:
			return result, exitMismatch, fmt.Errorf("length mismatch at row %d: server exhausted, reference has timestamp %s", n, timestamp(want.TS))
		case !want.TS.Equal(got.TS):
			return result, exitMismatch, fmt.Errorf("timestamp mismatch at row %d: reference %s, server %s", n, timestamp(want.TS), timestamp(got.TS))
		case want.Line != got.Line:
			return result, exitMismatch, lineMismatch(n, want, got)
		}
		previous = want.TS
		result.matched++
	}
}

func orderMismatch(n uint64, side string, previous, current time.Time) error {
	return fmt.Errorf("ordering mismatch at row %d: %s timestamp %s precedes previous timestamp %s",
		n, side, timestamp(current), timestamp(previous))
}

func timestamp(t time.Time) string {
	return t.UTC().Format(time.RFC3339Nano)
}

func lineMismatch(n uint64, want, got client.Row) error {
	var offset int
	for offset < min(len(want.Line), len(got.Line)) && want.Line[offset] == got.Line[offset] {
		offset++
	}
	start := max(0, offset-16)
	return fmt.Errorf("line mismatch at row %d (timestamp %s, byte %d): reference %s, server %s",
		n, timestamp(want.TS), offset+1, linePreview(want.Line, start), linePreview(got.Line, start))
}

func linePreview(line string, start int) string {
	const maxBytes = 80
	end := start + min(maxBytes, len(line)-start)
	quoted := fmt.Sprintf("%q", line[start:end])
	if start == 0 && end == len(line) {
		return quoted
	}
	return fmt.Sprintf("%s (showing bytes %d–%d of %d)", quoted, start+1, end, len(line))
}
