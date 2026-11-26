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

const (
	restoredPageCap = 256
)

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
	proof, probeErr := runRestoredParity(ctx, binary)
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
	Point backup.Point `json:"verified_point"`
	parityProgress
	FirstFailure *parityWalkFailure `json:"first_failure,omitempty"`
}

func runRestoredParity(ctx context.Context, binary string) (proof parityProof, err error) {
	root, err := os.MkdirTemp("", "bog-release-parity-")
	if err != nil {
		return proof, err
	}
	defer func() {
		cleanupErr := os.RemoveAll(root)
		proof.FixtureRemoved = cleanupErr == nil
		if cleanupErr != nil {
			err = errors.Join(err, fmt.Errorf("remove restore fixture: %w", cleanupErr))
		}
	}()
	proof, err = restoredParity(ctx, binary, root)
	errors.As(err, &proof.FirstFailure)
	return proof, err
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
	flags := []string{fmt.Sprintf("-query-max-response-bytes=%d", restoredPageCap)}
	model, err := parityModel(rows)
	if err != nil {
		return proof, err
	}
	captured := model.HighWater()
	added := row{rows[1].TS, "after restore [match]"}
	var staleCursor string
	env := slices.DeleteFunc(os.Environ(), func(kv string) bool {
		return strings.HasPrefix(kv, "BOG_CAPACITY_CONTROL=")
	})
	enabled := append(slices.Clone(env), "BOG_CAPACITY_CONTROL=1")
	err = serveDirectoryWithEnv(ctx, binary, dir, client, flags, enabled, func(base string) error {
		pager, err := newReleasePager(base, client, restoredPageCap)
		if err != nil {
			return err
		}
		if err := checkParityWalks(ctx, pager, model, captured, "restore"); err != nil {
			return err
		}
		proof.CapturedRows = captured
		want, err := parityMatches(pager, model, captured, "")
		if err != nil {
			return err
		}
		held := parityWalk{stage: "held_across_compaction", want: want}
		if err := held.page(ctx, pager, 1); err != nil {
			return err
		}

		status, err := ingest(ctx, client, base, []row{added})
		if err != nil {
			return fmt.Errorf("append to restored image: %w", err)
		}
		proof.PostRestore204 = status == http.StatusNoContent
		if err := appendParityModel(model, []row{added}); err != nil {
			return err
		}
		augmented := model.HighWater()
		if err := checkParityWalks(ctx, pager, model, augmented, "after_append"); err != nil {
			return err
		}
		proof.PostAppendRows = augmented
		proof.Compaction, err = replaceParityGeneration(ctx, client, base, augmented)
		if err != nil {
			return err
		}
		for held.cursor != "" {
			if err := held.page(ctx, pager, 1); err != nil {
				return err
			}
		}
		proof.HeldRows = held.offset
		if err := checkParityWalks(ctx, pager, model, augmented, "after_compaction"); err != nil {
			return err
		}
		proof.PostCompactionRows = augmented
		want, err = parityMatches(pager, model, augmented, "")
		if err != nil {
			return err
		}
		beforeRestart := parityWalk{stage: "before_restart", want: want}
		if err := beforeRestart.page(ctx, pager, 1); err != nil {
			return err
		}
		staleCursor = beforeRestart.cursor
		return nil
	})
	if err != nil {
		return proof, fmt.Errorf("serve restored image: %w", err)
	}
	err = serveDirectoryWithEnv(ctx, binary, dir, client, flags, env, func(base string) error {
		if err := requireStaleCursorRejection(ctx, client, base, staleCursor); err != nil {
			return fmt.Errorf("resume cursor from before restart: %w", err)
		}
		proof.StaleCursorReject = true
		pager, err := newReleasePager(base, client, restoredPageCap)
		if err != nil {
			return err
		}
		if err := checkParityWalks(ctx, pager, model, model.HighWater(), "restart"); err != nil {
			return err
		}
		proof.RestartRows = model.HighWater()
		return nil
	})
	if err != nil {
		err = fmt.Errorf("restart with acknowledged post-restore write: %w", err)
	}
	return proof, err
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

func TestReleaseParityCleansFixtureAfterStartupFailure(t *testing.T) {
	tmp := t.TempDir()
	t.Setenv("TMPDIR", tmp)
	binary := filepath.Join(tmp, "invalid-candidate")
	writeFixture(t, binary, []byte("invalid executable"))
	proof, err := runRestoredParity(t.Context(), binary)
	checkProblem(t, err, "serve restored image")
	if !proof.FixtureRemoved || proof.CapturedRows != 0 {
		t.Fatalf("unexpected cleanup evidence: %+v", proof)
	}
	entries, err := os.ReadDir(tmp)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Name() != filepath.Base(binary) {
		t.Fatalf("restore fixture leaked files: %v", entries)
	}
}
