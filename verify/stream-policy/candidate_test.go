package streampolicy

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// The candidate exposes real HTTP responses while the verifier opens its
// committed file separately. The fault changes the durable write only.
type candidateFixture struct {
	mu       sync.Mutex
	target   *syntheticTarget
	addr     string
	path     string
	fault    string
	appends  int
	restarts int
}

func newCandidateFixture(t *testing.T, fault string) (*candidateFixture, Candidate) {
	t.Helper()
	f := &candidateFixture{
		target: newSynthetic(), path: filepath.Join(t.TempDir(), "committed"), fault: fault,
	}
	server := httptest.NewServer(http.HandlerFunc(f.serveHTTP))
	t.Cleanup(server.Close)
	f.addr = server.URL
	return f, Candidate{
		Request: f.request, Policy: f, Evidence: f,
		ExportID: func(r Response) (string, error) {
			values := r.Headers["Location"]
			if len(values) > 1 {
				return "", errors.New("multiple artifact references")
			}
			return http.Header(r.Headers).Get("Location"), nil
		},
		VerifyArtifact: verifyCandidateFixtureArtifact,
	}
}

func (f *candidateFixture) request(ctx context.Context, probe CandidateRequest) (*http.Request, error) {
	method := http.MethodGet
	if probe.Operation == CandidateIngest || probe.Operation == CandidateCreateExport {
		method = http.MethodPost
	}
	r, err := http.NewRequestWithContext(ctx, method, f.addr, nil)
	if err != nil {
		return nil, err
	}
	if probe.Target.Principal != "" {
		r.Header.Set("Authorization", "Bearer test-credential-for-"+probe.Target.Principal)
	}
	r.Header.Set("Stream-Selector", probe.Target.Selector)
	values := r.URL.Query()
	switch probe.Operation {
	case CandidateIngest:
		r.URL.Path = "/ingest"
	case CandidateQuery:
		r.URL.Path = "/query"
		values.Set("start", probe.Query.Start)
		values.Set("end", probe.Query.End)
		values.Set("contains", probe.Query.Contains)
		values.Set("limit", strconv.Itoa(probe.Query.Limit))
		values.Set("cursor", probe.Query.Cursor)
	case CandidateCreateExport:
		r.URL.Path = "/exports/create"
	case CandidateFetchExport:
		r.URL.Path = "/exports/fetch"
		values.Set("id", probe.ArtifactID)
	}
	r.URL.RawQuery = values.Encode()
	return r, nil
}

func (f *candidateFixture) observe(r *http.Request) (Response, error) {
	ctx := r.Context()
	credential := r.Header.Get("Authorization")
	principal, ok := strings.CutPrefix(credential, "Bearer test-credential-for-")
	if !ok || principal == "" {
		return response(http.StatusUnauthorized, "credentials required\n"), nil
	}
	target := Target{principal, r.Header.Get("Stream-Selector")}
	v := r.URL.Query()
	switch r.URL.Path {
	case "/ingest":
		// Net/http closes incoming request bodies after the handler returns.
		body, err := io.ReadAll(io.LimitReader(r.Body, MaxEvidenceBytes+1))
		if err != nil {
			return Response{}, err
		}
		before := len(f.target.frames)
		f.appends++
		request := IngestRequest{Target: target, Body: string(body), Key: strconv.Itoa(f.appends)}
		result, err := f.target.Ingest(ctx, request)
		if err != nil || len(f.target.frames) == before {
			return result, err
		}
		committed := f.target.committed
		if f.fault != "" {
			committed = bytes.Replace(committed, []byte(replacement), []byte(replacement+" "+f.fault), 1)
		}
		return result, os.WriteFile(f.path, committed, 0600)
	case "/query":
		limit, err := strconv.Atoi(v.Get("limit"))
		if err != nil {
			return Response{}, err
		}
		got, err := f.target.Query(ctx, QueryRequest{
			Target: target, Start: v.Get("start"), End: v.Get("end"),
			Contains: v.Get("contains"), Limit: limit, Cursor: v.Get("cursor"),
		})
		return got.Response, err
	case "/exports/create":
		got, err := f.target.CreateExport(ctx, target)
		if got.ArtifactID != "" {
			got.Headers = map[string][]string{"Location": {got.ArtifactID}}
		}
		return got.Response, err
	case "/exports/fetch":
		got, err := f.target.FetchExport(ctx, target, v.Get("id"))
		if err == nil && got.Verified {
			got.Body = got.Bytes
		}
		return got.Response, err
	default:
		return response(http.StatusNotImplemented, ""), nil
	}
}

func (f *candidateFixture) serveHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	result, err := f.observe(r)
	if err != nil {
		result = response(http.StatusInternalServerError, "fixture operation failed\n")
	}
	for name, values := range result.Headers {
		w.Header()[name] = values
	}
	w.WriteHeader(result.Status)
	if len(result.Body) != 0 {
		if _, err := w.Write(result.Body); err != nil {
			panic(http.ErrAbortHandler)
		}
	}
}

func (f *candidateFixture) ApplyPolicy(ctx context.Context, revision PolicyRevision) (PolicyAck, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.target.ApplyPolicy(ctx, revision)
}

func (f *candidateFixture) OpenCommitted(ctx context.Context) (io.ReadCloser, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return os.Open(f.path)
}

func (f *candidateFixture) DecodeCommitted(ctx context.Context, raw []byte) ([]Record, error) {
	// Independently decode the fixture format without using the target's
	// recovery path or consulting its live rows.
	var records []Record
	for len(raw) != 0 {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		line, rest, ended := bytes.Cut(raw, []byte("\n"))
		var batch struct {
			Scope Scope
			Rows  []Row
		}
		if !ended || json.Unmarshal(line, &batch) != nil || batch.Scope == (Scope{}) || len(batch.Rows) == 0 {
			return nil, errors.New("invalid fixture frame")
		}
		for _, row := range batch.Rows {
			records = append(records, Record{batch.Scope, row})
		}
		raw = rest
	}
	return records, nil
}

func (f *candidateFixture) Restart(ctx context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	raw, err := os.ReadFile(f.path)
	if err != nil {
		return err
	}
	f.target.committed = raw
	if _, err := f.target.RestartAndReplay(ctx); err != nil {
		return err
	}
	f.restarts++
	return nil
}

func verifyCandidateFixtureArtifact(_ context.Context, raw []byte) (Manifest, []Record, error) {
	if len(raw) <= sha256.Size {
		return Manifest{}, nil, errors.New("truncated artifact")
	}
	payload, tag := raw[:len(raw)-sha256.Size], raw[len(raw)-sha256.Size:]
	want, err := fixtureTag(payload)
	if err != nil {
		return Manifest{}, nil, err
	}
	if !hmac.Equal(want, tag) {
		return Manifest{}, nil, errors.New("invalid artifact authentication")
	}
	var image syntheticImage
	err = json.Unmarshal(payload, &image)
	return image.Manifest, image.Records, err
}

func runCandidateFixture(t *testing.T, candidate Candidate) error {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	return RunCandidate(ctx, candidate)
}

func TestRunCandidate(t *testing.T) {
	t.Parallel()
	for range 3 {
		t.Run("independent target", func(t *testing.T) {
			t.Parallel()
			fixture, candidate := newCandidateFixture(t, "")
			if err := runCandidateFixture(t, candidate); err != nil {
				t.Fatal(err)
			}
			fixture.mu.Lock()
			defer fixture.mu.Unlock()
			if fixture.restarts != 1 || len(fixture.target.frames) != 4 {
				t.Fatalf("got %d restarts and %d commits", fixture.restarts, len(fixture.target.frames))
			}
		})
	}
}

func TestCandidateOutputFilteringCannotHideDurableRedactionFailure(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, raw string
	}{
		{"plain", plain},
		{"escaped", `fixture-secret-\u0022\u005c-\u96ea\ud83e\udd89`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			fixture, candidate := newCandidateFixture(t, tc.raw)
			err := runCandidateFixture(t, candidate)
			if !errors.Is(err, ErrViolation) || !strings.Contains(err.Error(), "committed bytes") ||
				!strings.Contains(err.Error(), "durable bytes contain unexpected fixture marker") {
				t.Fatalf("expected a durable redaction violation, got %v", err)
			}
			raw, err := os.ReadFile(fixture.path)
			if err != nil {
				t.Fatal(err)
			}
			records, err := fixture.DecodeCommitted(context.Background(), raw)
			if err != nil || len(records) == 0 {
				t.Fatalf("independent decoder did not recover fixture: %v", err)
			}
			if !strings.Contains(records[0].Row.Line, replacement) ||
				!strings.Contains(records[0].Row.Line, string(jsonUnescaped([]byte(tc.raw)))) {
				t.Fatal("committed record must contain both the retained replacement and the forbidden value")
			}
		})
	}
}

type candidateEvidenceOverride struct {
	CandidateEvidence
	open func(context.Context) (io.ReadCloser, error)
}

func (e candidateEvidenceOverride) OpenCommitted(ctx context.Context) (io.ReadCloser, error) {
	return e.open(ctx)
}

type failingEvidenceReader struct {
	io.Reader
	closeErr error
	closed   bool
}

func (r *failingEvidenceReader) Close() error {
	r.closed = true
	return r.closeErr
}

func TestCandidateIncompleteEvidence(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		change func(*Candidate)
		phase  string
	}{
		{"no policy authority", func(c *Candidate) { c.Policy = nil }, "requires"},
		{"no committed reader", func(c *Candidate) { c.Evidence = nil }, "requires"},
		{"no artifact verifier", func(c *Candidate) { c.VerifyArtifact = nil }, "requires"},
		{"no export ID binding", func(c *Candidate) { c.ExportID = nil }, "requires"},
		{"no HTTP routes", func(c *Candidate) { c.Request = nil }, "requires"},
		{"no committed bytes", func(c *Candidate) {
			c.Evidence = candidateEvidenceOverride{c.Evidence, func(context.Context) (io.ReadCloser, error) {
				return io.NopCloser(strings.NewReader("")), nil
			}}
		}, "committed bytes"},
		{"unavailable committed reader", func(c *Candidate) {
			c.Evidence = candidateEvidenceOverride{c.Evidence, func(context.Context) (io.ReadCloser, error) {
				return nil, errors.New(plain)
			}}
		}, "committed bytes"},
		{"export unavailable", func(c *Candidate) {
			request := c.Request
			c.Request = func(ctx context.Context, req CandidateRequest) (*http.Request, error) {
				if req.Operation == CandidateCreateExport {
					req.Operation = ""
				}
				return request(ctx, req)
			}
		}, "scoped exports"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, candidate := newCandidateFixture(t, "")
			tc.change(&candidate)
			err := runCandidateFixture(t, candidate)
			if !errors.Is(err, ErrIncomplete) || errors.Is(err, ErrViolation) ||
				!strings.Contains(err.Error(), tc.phase) || strings.Contains(err.Error(), plain) {
				t.Fatalf("expected opaque incomplete evidence for %q, got %v", tc.phase, err)
			}
		})
	}
}

type candidateReadError struct{ err error }

func (r candidateReadError) Read([]byte) (int, error) { return 0, r.err }

func TestReadCandidateEvidence(t *testing.T) {
	t.Parallel()
	failure := errors.New("fixture I/O failed")
	for _, tc := range []struct {
		name     string
		reader   io.Reader
		closeErr error
		want     error
	}{
		{"bounded read", io.LimitReader(strings.NewReader(strings.Repeat("x", MaxEvidenceBytes)), MaxEvidenceBytes), nil, nil},
		{"oversized read", strings.NewReader(strings.Repeat("x", MaxEvidenceBytes+1)), nil, ErrIncomplete},
		{"read failure", candidateReadError{failure}, nil, failure},
		{"close failure", strings.NewReader("valid bytes"), failure, failure},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := &failingEvidenceReader{Reader: tc.reader, closeErr: tc.closeErr}
			raw, err := readEvidence(r)
			if !r.closed || !errors.Is(err, tc.want) {
				t.Fatalf("closed=%t, got %v, want %v", r.closed, err, tc.want)
			}
			if err != nil && raw != nil {
				t.Fatal("read returned partial bytes as evidence")
			}
			if err == nil && len(raw) != MaxEvidenceBytes {
				t.Fatalf("got %d bytes at the exact evidence limit", len(raw))
			}
		})
	}
}

func TestCandidateQueryWireEvidence(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		status  int
		body    string
		headers http.Header
		wantErr bool
		rows    int
	}{
		{"complete row", 200, ndjson("ts", "fixture"), nil, false, 1},
		{"partial row framing", 200, strings.TrimSuffix(ndjson("ts", "fixture"), "\n"), nil, true, 0},
		{"invalid row", 200, "{\"line\":\"fixture\"}\n", nil, true, 0},
		{"denial with row", 403, ndjson("ts", "fixture"), nil, false, 1},
		{"denial with cursor", 403, "", http.Header{"Bog-Next-Cursor": {"opaque"}}, false, 0},
		{"denial with cursor trailer", 403, "forbidden\n", http.Header{"Trailer:bog-next-cursor": {"opaque"}}, true, 0},
		{"short HTTP body", 200, "", http.Header{"Content-Length": {"50"}}, true, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				for key, values := range tc.headers {
					if strings.HasPrefix(key, "Trailer:") {
						w.Header()[http.TrailerPrefix+strings.TrimPrefix(key, "Trailer:")] = values
					} else {
						w.Header()[key] = values
					}
				}
				w.WriteHeader(tc.status)
				if _, err := io.WriteString(w, tc.body); err != nil {
					panic(http.ErrAbortHandler)
				}
			}))
			defer server.Close()
			target := newCandidateAdapter(Candidate{
				Request: func(ctx context.Context, _ CandidateRequest) (*http.Request, error) {
					return http.NewRequestWithContext(ctx, http.MethodGet, server.URL, nil)
				},
			})
			defer target.client.CloseIdleConnections()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			got, err := target.Query(ctx, QueryRequest{})
			if (err != nil) != tc.wantErr || len(got.Rows) != tc.rows {
				t.Fatalf("got %d rows and error %v", len(got.Rows), err)
			}
			if tc.name == "denial with cursor" && got.NextCursor != "opaque" {
				t.Fatal("denial cursor was lost in the adapter")
			}
		})
	}
}

func TestCandidateResponseReadCancellation(t *testing.T) {
	t.Parallel()
	started, stopped := make(chan struct{}), make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer close(stopped)
		w.WriteHeader(http.StatusOK)
		if err := http.NewResponseController(w).Flush(); err != nil {
			t.Error("could not flush fixture response")
			return
		}
		close(started)
		<-r.Context().Done()
	}))
	defer server.Close()
	target := newCandidateAdapter(Candidate{
		Request: func(ctx context.Context, _ CandidateRequest) (*http.Request, error) {
			return http.NewRequestWithContext(ctx, http.MethodGet, server.URL, nil)
		},
	})
	defer target.client.CloseIdleConnections()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	result := make(chan error, 1)
	go func() {
		_, err := target.Query(ctx, QueryRequest{})
		result <- err
	}()
	select {
	case <-started:
		cancel()
	case <-ctx.Done():
		t.Fatal("fixture response did not start")
	}
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("query did not stop on cancellation: %v", err)
	}
	select {
	case <-stopped:
	case <-time.After(5 * time.Second):
		t.Fatal("candidate never observed request cancellation")
	}
}
