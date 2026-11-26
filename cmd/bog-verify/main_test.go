package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"bog/internal/backup"
	"bog/internal/checkpoint"
	"bog/internal/client"
	"bog/internal/httpapi"
	"bog/internal/store"
)

const fixtureStart = "2026-01-01T00:00:00Z"
const fixtureEnd = "2026-01-01T00:00:01Z"

type snapshot struct {
	point    backup.Point
	manifest []byte
	segment  []byte
	wal      []byte
}

func (s snapshot) Point() backup.Point { return s.point }
func (s snapshot) OpenManifest() (io.ReadCloser, error) {
	return io.NopCloser(bytes.NewReader(s.manifest)), nil
}
func (s snapshot) OpenSegment(string) (io.ReadCloser, error) {
	return io.NopCloser(bytes.NewReader(s.segment)), nil
}
func (s snapshot) OpenWAL() (backup.WALReader, error) {
	return fixtureWAL{bytes.NewReader(s.wal)}, nil
}

type fixtureWAL struct{ *bytes.Reader }

func (fixtureWAL) Close() error { return nil }

func check(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func makeBundle(t *testing.T) (string, []client.Row, backup.Point) {
	t.Helper()
	start, err := store.ParseTime(fixtureStart)
	check(t, err)
	rows := []client.Row{
		{TS: start, Line: "keep-z"},
		{TS: start.Add(3 * time.Nanosecond), Line: "outside filter"},
		{TS: start, Line: "keep-a"},
		{TS: start, Line: "keep-a"},
		{TS: start.Add(time.Nanosecond), Line: "keep-later"},
		{TS: start.Add(time.Second), Line: "keep-exclusive-end"},
	}
	dir, point := makeBundleFromRows(t, rows, len(rows)-2)
	return dir, rows, point
}

func makeBundleFromRows(t *testing.T, rows []client.Row, tailBatchSize int) (string, backup.Point) {
	t.Helper()
	checkpointRows := []checkpoint.BlockRow{{Row: rows[0], Ordinal: 0}, {Row: rows[1], Ordinal: 1}}
	block, err := checkpoint.EncodeBlock(checkpointRows)
	check(t, err)
	prefix := append([]byte("BOGWAL01"), encodeFrame(t, rows[:2])...)
	wal := bytes.Clone(prefix)
	for batch := range slices.Chunk(rows[2:], tailBatchSize) {
		wal = append(wal, encodeFrame(t, batch)...)
	}
	segment := checkpoint.Segment{
		Name: "bog-segment-00000000000000000000000000000001",
		Size: int64(len(block)), SHA256: fmt.Sprintf("%x", sha256.Sum256(block)),
	}
	payload, err := json.Marshal(checkpoint.Manifest{
		Version: 1, SourceSHA256: fmt.Sprintf("%x", sha256.Sum256(prefix)),
		CoveredOffset: int64(len(prefix)), NextOrdinal: 2, Segments: []checkpoint.Segment{segment},
	})
	check(t, err)
	generation := sha256.Sum256(payload)
	envelope, err := json.Marshal(struct {
		Payload json.RawMessage `json:"payload"`
		SHA256  string          `json:"sha256"`
	}{payload, fmt.Sprintf("%x", generation)})
	check(t, err)
	point := backup.Point{
		Generation: generation, CoveredOffset: int64(len(prefix)),
		NextOrdinal: 2, CommittedEnd: int64(len(wal)),
	}
	dir := filepath.Join(t.TempDir(), "reference")
	check(t, backup.Export(t.Context(), snapshot{point, envelope, block, wal}, dir))
	return dir, point
}

func encodeFrame(t *testing.T, rows []client.Row) []byte {
	t.Helper()
	var data bytes.Buffer
	for _, row := range rows {
		check(t, json.NewEncoder(&data).Encode(row))
	}
	table := crc32.MakeTable(crc32.Castagnoli)
	frame := make([]byte, 16, 16+data.Len())
	size := uint32(data.Len())
	binary.LittleEndian.PutUint32(frame[0:4], size)
	binary.LittleEndian.PutUint32(frame[4:8], ^size)
	binary.LittleEndian.PutUint32(frame[8:12], crc32.Checksum(data.Bytes(), table))
	binary.LittleEndian.PutUint32(frame[12:16], crc32.Checksum(frame[:12], table))
	return append(frame, data.Bytes()...)
}

func compareArgs(dir, url string) []string {
	return []string{"compare", "-backup", dir, "-url", url, "-start", fixtureStart, "-end", fixtureEnd,
		"-contains", "keep-", "-limit", "2", "-timeout", "5s"}
}

func fixtureCursor(value byte) string {
	return base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{value}, 33))
}

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
	dir, rows, point := makeBundle(t)
	var stdout, stderr bytes.Buffer
	if status := run(t.Context(), []string{"verify", "-backup", dir}, &stdout, &stderr); status != 0 ||
		stderr.Len() != 0 || !strings.Contains(stdout.String(), "backup verified: "+formatPoint(point)) {
		t.Fatalf("verify exit %d, stdout %q, stderr %q", status, &stdout, &stderr)
	}

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

func TestCLIFailures(t *testing.T) {
	t.Parallel()
	dir, _, _ := makeBundle(t)
	token := fixtureCursor('a')
	body := func(line string) string {
		return fmt.Sprintf("{\"ts\":%q,\"line\":%q}\n", fixtureStart, line)
	}
	for _, tc := range []struct {
		name  string
		body  string
		token string
		code  int
		want  string
	}{
		{"malformed after valid row", body("keep-z") + "{oops}\n", "", 4, "decode query record"},
		{"nonadvancing cursor", body("keep-z"), token, 4, "does not advance pagination"},
		{"invalid cursor syntax", body("keep-z"), "invalid", 4, "invalid next cursor header"},
		{"wrong tie order", body("keep-a"), "", 5, "line mismatch at row 1"},
		{"premature terminal page", body("keep-z"), "", 5, "length mismatch at row 2"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Length", strconv.Itoa(len(tc.body)))
				if tc.token != "" {
					w.Header().Set("Bog-Next-Cursor", tc.token)
				}
				n, err := io.WriteString(w, tc.body)
				if err != nil || n != len(tc.body) {
					t.Errorf("write response: %d of %d bytes, %v", n, len(tc.body), err)
				}
			}))
			defer server.Close()
			var stdout, stderr bytes.Buffer
			if code := run(t.Context(), compareArgs(dir, server.URL), &stdout, &stderr); code != tc.code ||
				stdout.Len() != 0 || !strings.Contains(stderr.String(), tc.want) {
				t.Fatalf("compare exit %d, stdout %q, stderr %q", code, &stdout, &stderr)
			}
		})
	}
}

func TestCLICompareEmptyContinuations(t *testing.T) {
	t.Parallel()
	dir, _, _ := makeBundle(t)
	a, b, c := fixtureCursor('a'), fixtureCursor('b'), fixtureCursor('c')
	const firstRows = `{"ts":"` + fixtureStart + `","line":"keep-z"}` + "\n" +
		`{"ts":"` + fixtureStart + `","line":"keep-a"}` + "\n"
	const lastRows = `{"ts":"` + fixtureStart + `","line":"keep-a"}` + "\n" +
		`{"ts":"2026-01-01T00:00:00.000000001Z","line":"keep-later"}` + "\n"
	type response struct {
		body, next string
		wait       bool
	}
	first := response{body: firstRows, next: a}
	empty := response{next: b}
	for _, tc := range []struct {
		name      string
		responses []response
		extra     []string
		status    int
		want      string
	}{
		{
			name:      "exact budget includes empty continuation and terminal pages",
			responses: []response{first, empty, {body: lastRows, next: c}, {}},
			extra:     []string{"-max-pages", "4"},
			status:    exitOK,
			want:      "rows=4 reference_pages=2 server_pages=4",
		},
		{
			name:      "first mismatch after an empty continuation",
			responses: []response{first, empty, {body: strings.Replace(lastRows, "keep-a", "keep-wrong", 1), next: c}},
			status:    exitMismatch,
			want:      "line mismatch at row 3",
		},
		{
			name:      "cursor cycle across empty pages",
			responses: []response{first, empty, {next: a}},
			status:    exitIncomplete,
			want:      "next cursor does not advance pagination: cursor cycle",
		},
		{
			name:      "empty continuation consumes final page budget",
			responses: []response{first, empty},
			extra:     []string{"-max-pages", "2"},
			status:    exitIncomplete,
			want:      "server walk at row 3: page 3: after 2 pages: query page budget exhausted",
		},
		{
			name:      "all rows match but terminal evidence exceeds page budget",
			responses: []response{first, empty, {body: lastRows, next: c}},
			extra:     []string{"-max-pages", "3"},
			status:    exitIncomplete,
			want:      "server walk at row 5: page 4: after 3 pages: query page budget exhausted",
		},
		{
			name:      "invalid framing in later response",
			responses: []response{first, empty, {body: strings.TrimSuffix(lastRows, "\n")}},
			status:    exitIncomplete,
			want:      "invalid query record framing",
		},
		{
			name:      "deadline while waiting for the next response",
			responses: []response{first, empty, {wait: true}},
			extra:     []string{"-timeout", "2s"},
			status:    exitIncomplete,
			want:      "context deadline exceeded",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var requests atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				i := int(requests.Add(1)) - 1
				if i >= len(tc.responses) {
					t.Errorf("unexpected request %d: %s %s", i+1, r.Method, r.URL)
					http.Error(w, "unexpected request", http.StatusInternalServerError)
					return
				}
				page, cursor := "1", ""
				if i > 0 {
					page, cursor = "", tc.responses[i-1].next
				}
				q := r.URL.Query()
				if r.Method != http.MethodGet || r.URL.Path != "/query" || q.Get("page") != page || q.Get("cursor") != cursor ||
					q.Get("start") != fixtureStart || q.Get("end") != fixtureEnd || q.Get("contains") != "keep-" || q.Get("limit") != "2" {
					t.Errorf("unexpected request %d: %s %s", i+1, r.Method, r.URL)
					http.Error(w, "unexpected request", http.StatusBadRequest)
					return
				}
				p := tc.responses[i]
				if p.wait {
					select {
					case <-r.Context().Done():
					case <-time.After(5 * time.Second):
						t.Error("HTTP request outlived the comparison deadline")
					}
					return
				}
				w.Header().Set("Content-Length", strconv.Itoa(len(p.body)))
				if p.next != "" {
					w.Header().Set("Bog-Next-Cursor", p.next)
				}
				if n, err := io.WriteString(w, p.body); err != nil || n != len(p.body) {
					t.Errorf("write response %d: %d of %d bytes, %v", i+1, n, len(p.body), err)
				}
			}))
			defer server.Close()
			var stdout, stderr bytes.Buffer
			status := run(t.Context(), append(compareArgs(dir, server.URL), tc.extra...), &stdout, &stderr)
			if status != tc.status || !strings.Contains(stdout.String()+stderr.String(), tc.want) ||
				(status == exitOK && stderr.Len() != 0) || (status != exitOK && stdout.Len() != 0) {
				t.Fatalf("compare exit %d, stdout %q, stderr %q", status, &stdout, &stderr)
			}
			if got := int(requests.Load()); got != len(tc.responses) {
				t.Fatalf("made %d requests, want %d", got, len(tc.responses))
			}
		})
	}
}

func TestCLIRejectsCorruptBackupBeforeHTTP(t *testing.T) {
	t.Parallel()
	dir, _, _ := makeBundle(t)
	check(t, os.WriteFile(filepath.Join(dir, "bog.backup"), []byte("corrupt"), 0600))
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		http.Error(w, "unexpected request", http.StatusInternalServerError)
	}))
	defer server.Close()
	for _, args := range [][]string{{"verify", "-backup", dir}, compareArgs(dir, server.URL)} {
		var stdout, stderr bytes.Buffer
		if code := run(t.Context(), args, &stdout, &stderr); code != 3 ||
			stdout.Len() != 0 || !strings.Contains(stderr.String(), "backup verification failed") ||
			!strings.Contains(stderr.String(), "invalid bundle metadata") {
			t.Fatalf("%s exit %d, stdout %q, stderr %q", args[0], code, &stdout, &stderr)
		}
	}
	if n := requests.Load(); n != 0 {
		t.Fatalf("invalid backup resulted in %d HTTP requests", n)
	}
}

func TestCLICancelsInFlightRequest(t *testing.T) {
	scratch := t.TempDir()
	dir, _, _ := makeBundle(t)
	t.Setenv("TMPDIR", scratch)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cancel()
		select {
		case <-r.Context().Done():
		case <-time.After(5 * time.Second):
			t.Error("HTTP request context did not cancel")
		}
	}))
	defer server.Close()
	var stdout, stderr bytes.Buffer
	if code := run(ctx, compareArgs(dir, server.URL), &stdout, &stderr); code != 4 ||
		stdout.Len() != 0 || !strings.Contains(stderr.String(), "context canceled") {
		t.Fatalf("compare exit %d, stdout %q, stderr %q", code, &stdout, &stderr)
	}
	assertEmptyScratch(t, scratch)
}

func TestCLIUsage(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		args []string
		code int
		want string
	}{
		{nil, 2, "Usage:"},
		{[]string{"--help"}, 0, "Exit statuses:"},
		{[]string{"compare", "-h"}, 0, "Usage:"},
		{[]string{"typo"}, 2, "unknown command"},
		{[]string{"verify"}, 2, "-backup is required"},
		{[]string{"verify", "-backup", "unused", "extra"}, 2, "unexpected argument"},
		{[]string{"verify", "-backup", "unused", "-timeout", "-1s"}, 2, "-timeout must be positive"},
		{compareArgs("unused", "http://localhost/?x=1"), 2, "-url must be an HTTP or HTTPS base URL"},
		{append(compareArgs("unused", "http://localhost"), "-start", "bad"), 2, "invalid -start"},
		{append(compareArgs("unused", "http://localhost"), "-end", "bad"), 2, "invalid -end"},
		{append(compareArgs("unused", "http://localhost"), "-end", "2025-12-31T00:00:00Z"), 2, "-end precedes -start"},
		{append(compareArgs("unused", "http://localhost"), "-limit", "1001"), 2, "-limit must be between"},
		{append(compareArgs("unused", "http://localhost"), "-max-pages", "0"), 2, "-max-pages must be positive"},
		{append(compareArgs("unused", "http://localhost"), "-max-page-bytes", "0"), 2, "-max-page-bytes must be between"},
		{append(compareArgs("unused", "http://localhost"), "-max-page-bytes", "-1"), 2, "-max-page-bytes must be between"},
		{append(compareArgs("unused", "http://localhost"), "-max-page-bytes", "9223372036854775807"), 2, "-max-page-bytes must be between"},
		{append(compareArgs("unused", "http://localhost"), "-max-page-bytes", "9223372036854775808"), 2, "-max-page-bytes"},
		{[]string{"verify", "-backup", "unused", "-max-page-bytes", "1"}, 2, "flag provided but not defined"},
	} {
		var stdout, stderr bytes.Buffer
		if code := run(t.Context(), tc.args, &stdout, &stderr); code != tc.code ||
			!strings.Contains(stdout.String()+stderr.String(), tc.want) {
			t.Errorf("%v exit %d, stdout %q, stderr %q", tc.args, code, &stdout, &stderr)
		}
	}
}

type failedWriter struct{}

func (failedWriter) Write([]byte) (int, error) { return 0, errors.New("output closed") }

func TestCLIReportsWriteFailure(t *testing.T) {
	dir, _, _ := makeBundle(t)
	var stderr bytes.Buffer
	if code := run(t.Context(), []string{"verify", "-backup", dir}, failedWriter{}, &stderr); code != 4 ||
		!strings.Contains(stderr.String(), "write result: output closed") {
		t.Fatalf("verify exit %d, stderr %q", code, &stderr)
	}
}
