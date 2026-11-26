package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
)

var initial = []row{entry(10, "first"), entry(10, "second"), entry(20, "third")}

func seed(ctx context.Context, s *fixtureServer) (string, error) {
	if err := s.start(ctx); err != nil {
		return "", err
	}
	for _, r := range [][]row{initial[:2], initial[2:]} {
		if err := s.ingest(ctx, r...); err != nil {
			return "", err
		}
	}
	invalid, err := payload([]row{entry(10, "rejected-valid-prefix")})
	if err != nil {
		return "", err
	}
	if _, _, err := s.want(ctx, "/ingest", append(invalid, "{broken}\n"...), http.StatusBadRequest); err != nil {
		return "", err
	}
	if err := s.walk(ctx, "", initial); err != nil {
		return "", err
	}
	return s.cursor(ctx)
}

func checkSIGKILL(ctx context.Context, s *fixtureServer) error {
	cursor, err := seed(ctx, s)
	if err != nil {
		return err
	}
	batch := make([]row, 64)
	for i := range batch {
		batch[i] = entry(30, fmt.Sprintf("interrupted-%02d-%s", i, strings.Repeat("x", 8192)))
	}
	body, err := payload(batch)
	if err != nil {
		return err
	}
	status, err := s.interrupt(ctx, body)
	if err != nil {
		return err
	}
	if err := s.restart(ctx, cursor); err != nil {
		return err
	}
	full := append(slices.Clone(initial), batch...)
	switch status {
	case 0:
		return s.walk(ctx, "", initial, full)
	case http.StatusNoContent:
		return s.walk(ctx, "", full)
	case http.StatusBadRequest, http.StatusServiceUnavailable:
		return s.walk(ctx, "", initial)
	default:
		return fmt.Errorf("unexpected final ingest status %d", status)
	}
}

func (s *fixtureServer) cycle(ctx context.Context, token string) error {
	if err := s.signal(syscall.SIGTERM); err != nil {
		return err
	}
	return s.restart(ctx, token)
}

func checkStaleCursor(ctx context.Context, s *fixtureServer) error {
	token, err := seed(ctx, s)
	if err != nil {
		return err
	}
	if err := s.walk(ctx, token, initial[1:]); err != nil {
		return err
	}
	if err := s.cycle(ctx, token); err != nil {
		return err
	}
	return s.walk(ctx, "", initial)
}

func checkBackfill(ctx context.Context, s *fixtureServer) error {
	token, err := seed(ctx, s)
	if err != nil {
		return err
	}
	if err := s.cycle(ctx, token); err != nil {
		return err
	}
	snapshot, err := s.cursor(ctx)
	if err != nil {
		return err
	}
	tie, earlier := entry(10, "tied-backfill"), entry(5, "earlier-backfill")
	// Include an exact duplicate across batches as well as distinguishable ties.
	if err := s.ingest(ctx, tie, earlier, initial[0]); err != nil {
		return err
	}
	want := []row{earlier, initial[0], initial[1], tie, initial[0], initial[2]}
	if err := s.walk(ctx, snapshot, initial[1:]); err != nil {
		return err
	}
	if err := s.walk(ctx, "", want); err != nil {
		return err
	}
	token, err = s.cursor(ctx)
	if err != nil {
		return err
	}
	if err := s.cycle(ctx, token); err != nil {
		return err
	}
	return s.walk(ctx, "", want)
}

const walName = "bog.wal"
const markerName = "bog.commit"
const frameStart = len("BOGWAL01")
const payloadStart = frameStart + 16

func committedWAL(dir string) ([]byte, error) {
	data, err := os.ReadFile(filepath.Join(dir, walName))
	if err == nil && (len(data) <= payloadStart || string(data[:frameStart]) != "BOGWAL01") {
		err = errors.New("expected populated BOGWAL01 WAL")
	}
	return data, err
}

func checkTornSuffix(ctx context.Context, s *fixtureServer) error {
	token, err := seed(ctx, s)
	if err != nil {
		return err
	}
	for _, length := range []int{3, 17} {
		if err := s.signal(syscall.SIGTERM); err != nil {
			return err
		}
		wal, err := committedWAL(s.dir)
		if err != nil {
			return err
		}
		path := filepath.Join(s.dir, walName)
		markerPath := filepath.Join(s.dir, markerName)
		marker, err := os.ReadFile(markerPath)
		if err != nil {
			return err
		}
		if err := syncWrite(path, wal[frameStart:frameStart+length], os.O_APPEND); err != nil {
			return err
		}
		if err := s.restart(ctx, token); err != nil {
			return err
		}
		if err := errors.Join(unchanged(path, wal), unchanged(markerPath, marker)); err != nil {
			return err
		}
		if err := s.walk(ctx, "", initial); err != nil {
			return err
		}
		token, err = s.cursor(ctx)
		if err != nil {
			return err
		}
	}
	extra := entry(30, "post-tail-append")
	if err := s.ingest(ctx, extra); err != nil {
		return err
	}
	if err := s.cycle(ctx, token); err != nil {
		return err
	}
	return s.walk(ctx, "", append(slices.Clone(initial), extra))
}

func unchanged(path string, want []byte) error {
	got, err := os.ReadFile(path)
	if err == nil && !bytes.Equal(got, want) {
		err = fmt.Errorf("%s differs from expected bytes", path)
	}
	return err
}

func checkCorruption(ctx context.Context, s *fixtureServer) error {
	if _, err := seed(ctx, s); err != nil {
		return err
	}
	if err := s.signal(syscall.SIGTERM); err != nil {
		return err
	}
	wal, err := committedWAL(s.dir)
	if err != nil {
		return err
	}
	wal[payloadStart] ^= 1
	path := filepath.Join(s.dir, walName)
	markerPath := filepath.Join(s.dir, markerName)
	marker, err := os.ReadFile(markerPath)
	if err != nil {
		return err
	}
	if err := syncWrite(path, wal, os.O_TRUNC); err != nil {
		return err
	}
	startErr := s.start(ctx)
	p := s.process
	select {
	case <-p.done:
		if startErr != nil && p.err != nil && p.cmd.ProcessState.ExitCode() > 0 && strings.Contains(p.log.String(), "invalid WAL checksum") {
			return errors.Join(unchanged(path, wal), unchanged(markerPath, marker))
		}
	default:
	}
	return errors.Join(errors.New("expected committed checksum rejection at startup"), startErr,
		s.stop(), unchanged(path, wal), unchanged(markerPath, marker))
}
