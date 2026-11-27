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
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"bog/internal/backup"
	"bog/internal/checkpoint"
	"bog/internal/store"
	"bog/internal/wal"
	querymodel "bog/verify/query-model"
)

const (
	restoredParityCase = "restored_parity"
	restoredPageCap    = 256
	restoredMaxPages   = 32
	restoredWalkTime   = 8 * time.Second
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
	var proof parityProof
	probeErr := withFixtureRoot(func(root string) error {
		var err error
		proof, err = restoredParity(ctx, binary, root)
		return err
	})
	probeErr = record(results.Cases, restoredParityCase, proof, probeErr)
	if err := errors.Join(probeErr, writeReport(path, results)); err != nil {
		t.Fatal(err)
	}
}

type parityProof struct {
	Point          backup.Point `json:"verified_point"`
	CapturedRows   int          `json:"captured_rows"`
	HeldPages      int          `json:"held_capture_pages"`
	HeldRows       int          `json:"held_capture_rows"`
	ByteTrimmed    int          `json:"held_capture_byte_trimmed_pages"`
	PostRestore204 bool         `json:"post_restore_204"`
	PostAppendRows int          `json:"post_append_rows"`
	RestartRows    int          `json:"restart_rows"`
}

func parityRows() []querymodel.Row {
	early := time.Date(2020, time.January, 1, 0, 0, 0, 0, time.UTC)
	tie, late := early.Add(time.Second), early.Add(2*time.Second)
	return []querymodel.Row{
		{Time: late, Line: "checkpoint " + strings.Repeat("x", 110)},
		{Time: tie, Line: "duplicate [match]"},
		{Time: tie, Line: "duplicate [match]"},
		{Time: tie, Line: "tail " + strings.Repeat("y", 110)},
		{Time: early, Line: "backfill [match]"},
		{Time: tie, Line: "duplicate [match]"},
	}
}

func parityWireRow(r querymodel.Row) row {
	return row{r.Time.UTC().Format(time.RFC3339Nano), r.Line}
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
	var log querymodel.Log
	log.Append(rows)
	// Backfill at the tied timestamp. Its append ordinal puts it after
	// the first page, where a resume against a fresh snapshot would see it.
	added := querymodel.Row{Time: rows[1].Time, Line: "after restore [match]"}
	var staleCursor string
	err = serveDirectory(ctx, binary, dir, client, flags, func(ctx context.Context, base string) error {
		pager, err := newReleasePager(base, client, restoredPageCap)
		if err != nil {
			return err
		}
		if err := checkRestoredWalks(ctx, pager, &log); err != nil {
			return err
		}
		proof.CapturedRows = log.HighWater()

		// Start above the first page's byte capacity and resume at a
		// smaller limit. Ingest only after the oracle accepts the cursor.
		walk := restoredWalk(pager, "", []int{10, 1, 3})
		// The early row and two duplicates fit in 256 bytes. The
		// next tied row, with its longer line, forces the trim.
		walk.FirstPageRows = 3
		held, err := querymodel.VerifyWireWalk(ctx, &log, walk, parityFetch(pager), func(ctx context.Context, cursor string) error {
			status, err := ingest(ctx, client, base, []row{parityWireRow(added)})
			if err != nil {
				return fmt.Errorf("append to restored image: %w", err)
			}
			proof.PostRestore204 = status == http.StatusNoContent
			log.Append([]querymodel.Row{added})
			staleCursor = cursor
			return nil
		})
		if err != nil {
			return fmt.Errorf("held restored capture across acknowledged backfill: %w", err)
		}
		proof.HeldPages, proof.HeldRows, proof.ByteTrimmed = held.Pages, held.Rows, held.ByteTrimmedPages

		if err := checkRestoredWalks(ctx, pager, &log); err != nil {
			return fmt.Errorf("walk after acknowledged post-restore write: %w", err)
		}
		proof.PostAppendRows = log.HighWater()
		return nil
	})
	if err != nil {
		return proof, fmt.Errorf("serve restored image: %w", err)
	}
	err = serveDirectory(ctx, binary, dir, client, flags, func(ctx context.Context, base string) error {
		if err := requireStaleCursorRejection(ctx, client, base, staleCursor); err != nil {
			return fmt.Errorf("resume cursor from before restart: %w", err)
		}
		pager, err := newReleasePager(base, client, restoredPageCap)
		if err != nil {
			return err
		}
		if err := checkRestoredWalks(ctx, pager, &log); err != nil {
			return err
		}
		proof.RestartRows = log.HighWater()
		return nil
	})
	if err != nil {
		err = fmt.Errorf("restart with acknowledged post-restore write: %w", err)
	}
	return proof, err
}

func restoredWalk(pager *releasePager, filter string, limits []int) querymodel.WireWalk {
	return querymodel.WireWalk{
		Filter:    querymodel.Filter{Start: pager.query.Start, End: pager.query.End, Contains: filter},
		RowLimits: limits,
		Limits: querymodel.WireLimits{
			MaxPages: restoredMaxPages, MaxPageBytes: restoredPageCap, Timeout: restoredWalkTime,
		},
	}
}

func checkRestoredWalks(ctx context.Context, pager *releasePager, log *querymodel.Log) error {
	fetch := parityFetch(pager)
	for _, filter := range []string{"", "[match]", "absent"} {
		for _, limits := range [][]int{{1}, {3}, {10, 1, 3}} {
			walk := restoredWalk(pager, filter, limits)
			if _, err := querymodel.VerifyWireWalk(ctx, log, walk, fetch, nil); err != nil {
				return fmt.Errorf("restored parity with row limits %v: %w", limits, err)
			}
		}
	}
	return nil
}

// parityFetch adapts production-client results at the verifier boundary. The
// pager bounds bytes while reading and verifies media type and cursor framing.
// ADR 0006 also requires the shipped server to declare the exact body length.
func parityFetch(pager *releasePager) querymodel.FetchWirePage {
	return func(ctx context.Context, req querymodel.WireRequest) (querymodel.WirePage, error) {
		if !req.Filter.Start.Equal(pager.query.Start) || !req.Filter.End.Equal(pager.query.End) ||
			req.MaxPageBytes != pager.client.MaxPageBytes {
			return querymodel.WirePage{}, errors.New("oracle bounds differ from the configured release pager")
		}
		page, res, err := pager.fetch(ctx, req.Filter.Contains, req.Limit, req.Cursor)
		if err != nil {
			return querymodel.WirePage{}, err
		}
		if res.ContentLength < 0 || res.Header.Get("Content-Length") == "" {
			return querymodel.WirePage{}, errors.New("missing exact Content-Length on query response")
		}
		rows := make([]querymodel.Row, len(page.Rows))
		for i, r := range page.Rows {
			rows[i] = querymodel.Row{Time: r.TS, Line: r.Line}
		}
		return querymodel.WirePage{Rows: rows, NextCursor: page.NextCursor, BodyBytes: res.ContentLength}, nil
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

func appendParityBatch(dir string, rows []querymodel.Row) (err error) {
	var payload bytes.Buffer
	enc := json.NewEncoder(&payload)
	batch := make([]store.Row, 0, len(rows))
	for _, r := range rows {
		if err := enc.Encode(parityWireRow(r)); err != nil {
			return err
		}
		batch = append(batch, store.Row{TS: r.Time, Line: r.Line})
	}
	log, err := wal.Open(dir, &store.Store{})
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, log.Close()) }()
	return log.Append(payload.Bytes(), batch)
}

func makeRestoredFixture(ctx context.Context, root string, rows []querymodel.Row) (string, backup.Point, error) {
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

func TestRestoredParityPagerRequiresShippedFraming(t *testing.T) {
	t.Parallel()
	const record = "{\"ts\":\"2020-01-01T00:00:00Z\",\"line\":\"row\"}\n"
	for _, tc := range []struct {
		name, body, contentType string
		chunked                 bool
		want                    string
	}{
		{name: "terminal row", body: record, contentType: "application/x-ndjson"},
		{name: "empty terminal page", contentType: "application/x-ndjson"},
		{name: "chunked page", body: record, contentType: "application/x-ndjson", chunked: true, want: "missing exact Content-Length"},
		{name: "wrong media type", body: record, contentType: "text/plain", want: "expected NDJSON content type"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				q := req.URL.Query()
				if req.URL.Path != "/query" || q.Get("page") != "1" ||
					q.Get("limit") != "1" || q.Get("start") != fixtureStart || q.Get("end") != fixtureEnd {
					t.Errorf("unexpected parity request %q", req.URL)
					http.Error(w, "unexpected request", http.StatusBadRequest)
					return
				}
				w.Header().Set("Content-Type", tc.contentType)
				if tc.chunked {
					if err := http.NewResponseController(w).Flush(); err != nil {
						t.Error(err)
						return
					}
				} else {
					w.Header().Set("Content-Length", strconv.Itoa(len(tc.body)))
				}
				if _, err := io.WriteString(w, tc.body); err != nil {
					t.Error(err)
				}
			}))
			defer srv.Close()
			pager, err := newReleasePager(srv.URL, srv.Client(), restoredPageCap)
			if err != nil {
				t.Fatal(err)
			}
			walk := restoredWalk(pager, "", []int{1})
			var log querymodel.Log
			rows := 0
			if tc.body != "" {
				rows = log.Append([]querymodel.Row{{Time: walk.Filter.Start, Line: "row"}})
			}
			stats, err := querymodel.VerifyWireWalk(t.Context(), &log, walk, parityFetch(pager), nil)
			if tc.want != "" {
				if err == nil || !strings.Contains(err.Error(), tc.want) || stats != (querymodel.WireStats{}) {
					t.Fatalf("walk = %+v, %v, want %q", stats, err, tc.want)
				}
				return
			}
			want := querymodel.WireStats{Pages: 1, Rows: rows, BodyBytes: int64(len(tc.body))}
			if err != nil || stats != want {
				t.Fatalf("walk = %+v, %v, want %+v", stats, err, want)
			}
		})
	}
}
