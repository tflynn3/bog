package main

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"time"
)

func seed(ctx context.Context, s *server) (ledger, error) {
	var l ledger
	for i := range 3 {
		// Names descend so sorting by line can't accidentally satisfy tie order.
		rows := fixtureRows(fmt.Sprintf("ordered-%d", 2-i), 3, 1+i/2, "")
		if err := s.submit(ctx, rows, "", http.StatusNoContent); err != nil {
			return nil, err
		}
		l = append(l, batch{rows, acknowledged})
	}
	rows := fixtureRows("invalid", 2, 1, "")
	if err := s.submit(ctx, rows, "{\n", http.StatusBadRequest); err != nil {
		return nil, err
	}
	return append(l, batch{rows, rejected}), nil
}

func sigkillCase(ctx context.Context, s *server) (any, error) {
	l, err := seed(ctx, s)
	if err != nil {
		return nil, err
	}
	cursor, err := s.capture(ctx)
	if err != nil {
		return nil, err
	}
	rows := fixtureRows("interrupted", 24, 1, strings.Repeat("x", 16<<10))
	if err := s.interrupt(ctx, rows); err != nil {
		return nil, err
	}
	l = append(l, batch{rows, unknown})
	if err := s.resume(ctx, cursor); err != nil {
		return nil, err
	}
	proof, err := s.reconcile(ctx, l, "", nil)
	return struct {
		reconciliation
		StaleCursorsRejected int `json:"stale_cursors_rejected"`
	}{proof, 1}, err
}

func (s *server) interrupt(ctx context.Context, rows []row) (err error) {
	payload, err := encodeRows(rows)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, processTimeout)
	defer cancel()
	conn, err := (&net.Dialer{}).DialContext(ctx, "tcp4", s.addr)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, conn.Close()) }()
	deadline, _ := ctx.Deadline()
	if err := conn.SetDeadline(deadline); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(conn, "POST /ingest HTTP/1.1\r\nHost: %s\r\nContent-Length: %d\r\nExpect: 100-continue\r\nConnection: close\r\n\r\n", s.addr, len(payload)); err != nil {
		return err
	}
	resp, err := http.ReadResponse(bufio.NewReader(conn), nil)
	if err != nil {
		return err
	}
	if err := resp.Body.Close(); err != nil {
		return err
	}
	if resp.StatusCode != http.StatusContinue {
		return fmt.Errorf("interrupted request expected HTTP 100, got %d", resp.StatusCode)
	}
	if _, err := conn.Write(payload); err != nil {
		return err
	}
	// The request reached the server, but we deliberately never read a final
	// status. Recovery must contain either the whole batch or none of it.
	return s.process.stop(syscall.SIGKILL)
}

type recoveryEvidence struct {
	reconciliation
	TornSuffixDiscarded  bool `json:"torn_suffix_discarded"`
	SnapshotPreserved    bool `json:"snapshot_preserved_after_backfill"`
	CorruptionRefused    bool `json:"committed_corruption_refused"`
	CommittedFilesIntact bool `json:"committed_files_intact"`
	StaleCursorsRejected int  `json:"stale_cursors_rejected"`
}

func recoveryCase(ctx context.Context, s *server) (any, error) {
	var proof recoveryEvidence
	l, err := seed(ctx, s)
	if err != nil {
		return proof, err
	}
	cursor, err := s.capture(ctx)
	if err != nil {
		return proof, err
	}
	if err := s.process.stop(syscall.SIGTERM); err != nil {
		return proof, err
	}
	walPath := filepath.Join(s.dir, "bog.wal")
	committed, err := os.ReadFile(walPath)
	if err != nil {
		return proof, err
	}
	// Use a verified legacy WAL header and part of a real frame to mimic a
	// crash after writing the frame header and before completing the payload.
	const payloadOffset = len("BOGWAL01") + 16
	if len(committed) <= payloadOffset+1 || !bytes.HasPrefix(committed, []byte("BOGWAL01")) {
		return proof, errors.New("unrecognized WAL layout for torn-suffix injection")
	}
	markerPath := filepath.Join(s.dir, "bog.commit")
	marker, err := os.ReadFile(markerPath)
	if err != nil {
		return proof, err
	}
	f, err := os.OpenFile(walPath, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		return proof, err
	}
	_, err = f.Write(committed[len("BOGWAL01") : payloadOffset+1])
	if err := errors.Join(err, f.Sync(), f.Close()); err != nil {
		return proof, err
	}
	if err := s.resume(ctx, cursor); err != nil {
		return proof, err
	}
	proof.StaleCursorsRejected++
	recovered, err := os.ReadFile(walPath)
	if err != nil {
		return proof, err
	}
	if !bytes.Equal(recovered, committed) {
		return proof, errors.New("torn suffix recovery changed the committed WAL or retained the suffix")
	}
	gotMarker, err := os.ReadFile(markerPath)
	if err != nil {
		return proof, err
	}
	if !bytes.Equal(gotMarker, marker) {
		return proof, errors.New("torn suffix recovery changed the commit marker")
	}
	proof.TornSuffixDiscarded = true
	if _, err := s.reconcile(ctx, l, "", nil); err != nil {
		return proof, fmt.Errorf("torn suffix replay: %w", err)
	}

	first, cursor, err := s.page(ctx, "", 1)
	if err != nil {
		return proof, err
	}
	if len(first) != 1 || cursor == "" {
		return proof, errors.New("missing pre-backfill snapshot")
	}
	rows := fixtureRows("backfill", 4, 1, "")
	if err := s.submit(ctx, rows, "", http.StatusNoContent); err != nil {
		return proof, err
	}
	if _, err := s.reconcile(ctx, l, cursor, first); err != nil {
		return proof, fmt.Errorf("pre-backfill snapshot: %w", err)
	}
	proof.SnapshotPreserved = true
	l = append(l, batch{rows, acknowledged})
	if _, err := s.reconcile(ctx, l, "", nil); err != nil {
		return proof, err
	}
	if err := s.process.stop(syscall.SIGTERM); err != nil {
		return proof, err
	}
	if err := s.resume(ctx, cursor); err != nil {
		return proof, err
	}
	proof.StaleCursorsRejected++
	proof.reconciliation, err = s.reconcile(ctx, l, "", nil)
	if err != nil {
		return proof, err
	}
	if err := s.process.stop(syscall.SIGTERM); err != nil {
		return proof, err
	}
	committed, err = os.ReadFile(walPath)
	if err != nil {
		return proof, err
	}
	if len(committed) <= payloadOffset || !bytes.Equal(committed[:payloadOffset], recovered[:payloadOffset]) {
		return proof, errors.New("committed frame unavailable for corruption injection")
	}
	committed[payloadOffset] ^= 1
	f, err = os.OpenFile(walPath, os.O_WRONLY, 0)
	if err != nil {
		return proof, err
	}
	_, err = f.WriteAt(committed[payloadOffset:payloadOffset+1], int64(payloadOffset))
	if err := errors.Join(err, f.Sync(), f.Close()); err != nil {
		return proof, err
	}
	marker, err = os.ReadFile(markerPath)
	if err != nil {
		return proof, err
	}
	if err := s.refuseCorruption(ctx); err != nil {
		return proof, err
	}
	proof.CorruptionRefused = true
	for path, want := range map[string][]byte{walPath: committed, markerPath: marker} {
		got, err := os.ReadFile(path)
		if err != nil {
			return proof, err
		}
		if !bytes.Equal(got, want) {
			return proof, fmt.Errorf("refused startup changed %s", filepath.Base(path))
		}
	}
	proof.CommittedFilesIntact = true
	return proof, nil
}

func (s *server) refuseCorruption(ctx context.Context) (err error) {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := s.launchFor("corruption_refusal"); err != nil {
		return err
	}
	defer func() {
		s.recordStartupError(err)
		err = errors.Join(err, s.close())
	}()
	ctx, cancel := context.WithTimeout(ctx, processTimeout)
	defer cancel()
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-s.process.done:
			var exit *exec.ExitError
			if errors.As(s.process.result, &exit) && exit.ExitCode() > 0 &&
				strings.Contains(s.process.logs.text(), "invalid WAL checksum") {
				return nil
			}
			return fmt.Errorf("expected startup to refuse committed checksum corruption, got %v: %s", s.process.result, s.process.logs.text())
		case <-ctx.Done():
			return fmt.Errorf("corrupt candidate did not exit: %w", ctx.Err())
		case <-ticker.C:
			conn, err := (&net.Dialer{Timeout: 100 * time.Millisecond}).DialContext(ctx, "tcp4", s.addr)
			if err == nil {
				return errors.Join(errors.New("candidate listened with committed corruption"), conn.Close())
			}
			if !errors.Is(err, syscall.ECONNREFUSED) {
				return fmt.Errorf("probe corrupt candidate: %w", err)
			}
		}
	}
}
