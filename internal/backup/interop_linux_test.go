package backup

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"bog/internal/checkpoint"
	"bog/internal/store"
	"bog/internal/wal"
)

type stoppedSnapshot struct {
	directory string
	point     Point
}

func (s stoppedSnapshot) Point() Point { return s.point }
func (s stoppedSnapshot) OpenManifest() (io.ReadCloser, error) {
	return os.Open(filepath.Join(s.directory, "bog.checkpoint"))
}
func (s stoppedSnapshot) OpenSegment(name string) (io.ReadCloser, error) {
	return os.Open(filepath.Join(s.directory, name))
}
func (s stoppedSnapshot) OpenWAL() (WALReader, error) {
	return os.Open(filepath.Join(s.directory, "bog.wal"))
}

func TestProductionWALAndCheckpointCompatibility(t *testing.T) {
	ctx := context.Background()
	rows := newFixture(t).rows
	source := t.TempDir()
	appendBatch := func(rows []store.Row) {
		t.Helper()
		var live store.Store
		log, err := wal.Open(source, &live)
		must(t, err)
		defer func() { must(t, log.Close()) }()
		var payload bytes.Buffer
		enc := json.NewEncoder(&payload)
		for _, row := range rows {
			must(t, enc.Encode(row))
		}
		batch, err := store.Decode(payload.Bytes())
		must(t, err)
		must(t, log.Append(payload.Bytes(), batch))
	}
	appendBatch(rows[:6])
	legacy, err := checkpoint.OpenLegacy(source)
	must(t, err)
	m, err := legacy.Publish()
	must(t, errors.Join(err, legacy.Close()))
	appendBatch(rows[6:8])
	appendBatch(rows[8:])

	data, err := os.ReadFile(filepath.Join(source, "bog.checkpoint"))
	must(t, err)
	var env struct {
		Payload json.RawMessage `json:"payload"`
	}
	must(t, json.Unmarshal(data, &env))
	legacy, err = checkpoint.OpenLegacy(source)
	must(t, err)
	end := legacy.CommittedOffset()
	must(t, legacy.Close())
	p := Point{sha256.Sum256(env.Payload), m.CoveredOffset, m.NextOrdinal, end}
	parent := t.TempDir()
	bundle, target := filepath.Join(parent, "bundle"), filepath.Join(parent, "restored")
	must(t, Export(ctx, stoppedSnapshot{source, p}, bundle))
	must(t, Restore(ctx, bundle, target))

	var live store.Store
	log, err := wal.Open(source, &live)
	must(t, err)
	t.Cleanup(func() { must(t, log.Close()) })
	r, err := OpenRestored(ctx, target)
	must(t, err)
	t.Cleanup(func() { must(t, r.Close()) })
	checkWalk(t, r, rows)
	for _, literal := range []string{"", "[x]", "Case", "case"} {
		want := live.Query(minTime, maxTime, literal, len(rows)+1)
		q := Query{Start: minTime, End: maxTime, Contains: literal}
		var got []store.Row
		for range len(rows) + 1 {
			page, next, err := r.QueryPage(ctx, q, 2)
			must(t, err)
			for _, row := range page {
				got = append(got, row.Row)
			}
			if next == nil {
				break
			}
			q.After = next
		}
		if !slices.Equal(got, want) {
			t.Fatalf("literal %q: restore returned %d rows, WAL returned %d", literal, len(got), len(want))
		}
	}
}
