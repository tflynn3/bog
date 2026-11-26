package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"bog/internal/backup"
	"bog/internal/checkpoint"
	"bog/internal/store"
	"bog/internal/wal"
)

const restoredParityCase = "restored_parity"

// The release gate supplies the already-built candidate. Ordinary unit runs
// exercise the fixture independently without building or launching a server.
func TestReleaseRestoredParity(t *testing.T) {
	path := os.Getenv("BOG_RELEASE_PARITY_REPORT")
	if path == "" {
		t.Skip("release parity report path required")
	}
	binary := os.Getenv("BOG_RELEASE_BINARY")
	id, err := readIdentity(binary, os.Getenv("BOG_RELEASE_REVISION"), os.Getenv("BOG_RELEASE_FIXTURE"))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	results := report{id, make(map[string]outcome)}
	proof, probeErr := restoredParity(ctx, binary, t.TempDir())
	probeErr = record(results.Cases, restoredParityCase, proof, probeErr)
	data, err := json.MarshalIndent(results, "", "  ")
	if err == nil {
		err = os.WriteFile(path, append(data, '\n'), 0600)
	}
	if err := errors.Join(probeErr, err); err != nil {
		t.Fatal(err)
	}
}

type parityProof struct {
	Point          backup.Point `json:"verified_point"`
	CapturedRows   int          `json:"captured_rows"`
	PostRestore204 bool         `json:"post_restore_204"`
	RestartRows    int          `json:"restart_rows"`
}

func parityRows() []row {
	const early = "2020-01-01T00:00:00Z"
	const tie = "2020-01-01T00:00:01Z"
	const late = "2020-01-01T00:00:02Z"
	return []row{
		{late, "checkpoint " + strings.Repeat("x", 110)},
		{tie, "duplicate [match]"},
		{tie, "duplicate [match]"},
		{tie, "tail " + strings.Repeat("y", 110)},
		{early, "backfill [match]"},
		{tie, "duplicate [match]"},
	}
}

func restoredParity(ctx context.Context, binary, root string) (proof parityProof, err error) {
	rows := parityRows()
	dir, point, err := makeRestoredFixture(ctx, root, rows)
	if err != nil {
		return proof, fmt.Errorf("prepare verified restore: %w", err)
	}
	proof.Point = point
	client := &http.Client{
		Timeout:       5 * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
	defer client.CloseIdleConnections()
	flags := []string{"-query-max-response-bytes=256"}
	added := row{rows[1].TS, "after restore [match]"}
	err = serveDirectory(ctx, binary, dir, client, flags, func(base string) error {
		if err := checkRestoredWalks(ctx, client, base, rows); err != nil {
			return err
		}
		proof.CapturedRows = len(rows)
		status, err := ingest(ctx, client, base, []row{added})
		proof.PostRestore204 = status == http.StatusNoContent && err == nil
		return err
	})
	if err != nil {
		return proof, fmt.Errorf("serve restored image: %w", err)
	}
	err = serveDirectory(ctx, binary, dir, client, flags, func(base string) error {
		rows = append(rows, added)
		if err := checkRestoredWalks(ctx, client, base, rows); err != nil {
			return err
		}
		proof.RestartRows = len(rows)
		return nil
	})
	if err != nil {
		err = fmt.Errorf("restart with acknowledged post-restore write: %w", err)
	}
	return proof, err
}

func checkRestoredWalks(ctx context.Context, client *http.Client, base string, appendOrder []row) error {
	for _, filter := range []string{"", "[match]", "absent"} {
		var want []row
		for _, r := range appendOrder {
			if strings.Contains(r.Line, filter) {
				want = append(want, r)
			}
		}
		slices.SortStableFunc(want, func(a, b row) int { return strings.Compare(a.TS, b.TS) })
		for _, limit := range []int{1, 3, 10} {
			if err := checkRestoredWalk(ctx, client, base, filter, limit, want); err != nil {
				return fmt.Errorf("filter %q, page size %d: %w", filter, limit, err)
			}
		}
	}
	return nil
}

func checkRestoredWalk(ctx context.Context, client *http.Client, base, filter string, limit int, want []row) error {
	seen := make(map[string]bool)
	cursor := ""
	for {
		res, err := request(ctx, client, http.MethodGet, base+queryPath(cursor, limit, filter), nil)
		if err != nil {
			return err
		}
		if len(res.body) > 256 || len(res.body) > 0 && res.body[len(res.body)-1] != '\n' {
			return errors.New("invalid NDJSON framing or response byte count")
		}
		n, err := checkRows(res, want)
		if err != nil {
			return err
		}
		if n > limit {
			return errors.New("page exceeded row limit")
		}
		want = want[n:]
		if !res.cursorHeader {
			if len(want) != 0 {
				return fmt.Errorf("walk ended with %d rows missing", len(want))
			}
			return nil
		}
		if res.cursor == "" || seen[res.cursor] || n == 0 || len(want) == 0 {
			return errors.New("invalid or repeated continuation")
		}
		cursor = res.cursor
		seen[cursor] = true
	}
}

// The fixture is private and quiescent throughout export, so the files
// supply one immutable checkpoint and committed WAL view.
type paritySnapshot struct {
	dir   string
	point backup.Point
}

func (s paritySnapshot) Point() backup.Point { return s.point }
func (s paritySnapshot) OpenManifest() (io.ReadCloser, error) {
	return os.Open(filepath.Join(s.dir, "bog.checkpoint"))
}
func (s paritySnapshot) OpenSegment(name string) (io.ReadCloser, error) {
	return os.Open(filepath.Join(s.dir, name))
}
func (s paritySnapshot) OpenWAL() (backup.WALReader, error) {
	return os.Open(filepath.Join(s.dir, "bog.wal"))
}

func appendParityBatch(dir string, rows []row) (err error) {
	var payload bytes.Buffer
	enc := json.NewEncoder(&payload)
	batch := make([]store.Row, 0, len(rows))
	for _, r := range rows {
		if err := enc.Encode(r); err != nil {
			return err
		}
		ts, err := time.Parse(time.RFC3339, r.TS)
		if err != nil {
			return err
		}
		batch = append(batch, store.Row{TS: ts, Line: r.Line})
	}
	log, err := wal.Open(dir, &store.Store{})
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, log.Close()) }()
	return log.Append(payload.Bytes(), batch)
}

func makeRestoredFixture(ctx context.Context, root string, rows []row) (string, backup.Point, error) {
	var point backup.Point
	source := filepath.Join(root, "source")
	if err := os.Mkdir(source, 0700); err != nil {
		return "", point, err
	}
	if err := appendParityBatch(source, rows[:3]); err != nil {
		return "", point, err
	}
	legacy, err := checkpoint.OpenLegacy(source)
	if err != nil {
		return "", point, err
	}
	manifest, err := legacy.Publish()
	if err := errors.Join(err, legacy.Close()); err != nil {
		return "", point, err
	}
	if err := appendParityBatch(source, rows[3:]); err != nil {
		return "", point, err
	}
	legacy, err = checkpoint.OpenLegacy(source)
	if err != nil {
		return "", point, err
	}
	end := legacy.CommittedOffset()
	if err := legacy.Close(); err != nil {
		return "", point, err
	}
	raw, err := os.ReadFile(filepath.Join(source, "bog.checkpoint"))
	if err != nil {
		return "", point, err
	}
	var envelope struct {
		Payload json.RawMessage `json:"payload"`
	}
	if err := json.Unmarshal(raw, &envelope); err != nil {
		return "", point, err
	}
	snapshot := paritySnapshot{source, backup.Point{
		Generation:    sha256.Sum256(envelope.Payload),
		CoveredOffset: manifest.CoveredOffset,
		NextOrdinal:   manifest.NextOrdinal,
		CommittedEnd:  end,
	}}
	bundle, target := filepath.Join(root, "bundle"), filepath.Join(root, "restored")
	if err := backup.Export(ctx, snapshot, bundle); err != nil {
		return "", point, err
	}
	point, err = backup.Verify(ctx, bundle)
	if err != nil {
		return "", point, err
	}
	if point != snapshot.point {
		return "", point, errors.New("verified backup point differs from captured point")
	}
	return target, point, backup.Restore(ctx, bundle, target)
}

func TestReleaseRestoredFixture(t *testing.T) {
	dir, point, err := makeRestoredFixture(t.Context(), t.TempDir(), parityRows())
	if err != nil {
		t.Fatal(err)
	}
	if point.NextOrdinal != 3 || point.CoveredOffset >= point.CommittedEnd {
		t.Fatalf("fixture requires checkpoint and WAL tail rows: %+v", point)
	}
	got, err := backup.Verify(t.Context(), dir)
	if err != nil || got != point {
		t.Fatalf("restored recovery point = %+v, %v; want %+v", got, err, point)
	}
}
