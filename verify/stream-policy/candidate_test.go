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
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// The fixture exposes real HTTP responses while the verifier opens its
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

func TestCandidateMaskingPolicyEvidence(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, diagnostic string
		version          string
		want             error
		apply            func(context.Context, PolicyObserver, PolicyRevision) (PolicyAck, error)
	}{
		{"acknowledged without applying", "old masking revision must invalidate", "fixture-masking-v2", ErrViolation,
			func(_ context.Context, _ PolicyObserver, revision PolicyRevision) (PolicyAck, error) {
				return PolicyAck{Complete: true, Number: revision.Number}, nil
			}},
		{"missing acknowledgment", "policy update lacks acknowledgment", "fixture-masking-v2", ErrIncomplete,
			func(ctx context.Context, policy PolicyObserver, revision PolicyRevision) (PolicyAck, error) {
				ack, err := policy.ApplyPolicy(ctx, revision)
				ack.Complete = false
				return ack, err
			}},
		{"unaffected stream revision changed", "unaffected stream must resume", "fixture-masking-v2", ErrViolation,
			func(ctx context.Context, policy PolicyObserver, revision PolicyRevision) (PolicyAck, error) {
				revision.Streams[1].MaskingVersion = "fixture-masking-v2"
				return policy.ApplyPolicy(ctx, revision)
			}},
		{"masking version applied without rules", "fresh query must match and page over masked rows", "fixture-masking-v2", ErrViolation,
			func(ctx context.Context, policy PolicyObserver, revision PolicyRevision) (PolicyAck, error) {
				revision.Streams[0].Masks = nil
				return policy.ApplyPolicy(ctx, revision)
			}},
		{"original revision reused", "restored masking rules must retain a new revision", "fixture-masking-v3", ErrViolation,
			func(ctx context.Context, policy PolicyObserver, revision PolicyRevision) (PolicyAck, error) {
				revision.Streams[0].MaskingVersion = "fixture-masking-v1"
				return policy.ApplyPolicy(ctx, revision)
			}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			fixture, candidate := newCandidateFixture(t, "")
			candidate.Policy = policyFunc(func(ctx context.Context, revision PolicyRevision) (PolicyAck, error) {
				if revision.Streams[0].MaskingVersion == tc.version {
					return tc.apply(ctx, fixture, revision)
				}
				return fixture.ApplyPolicy(ctx, revision)
			})
			err := runCandidateFixture(t, candidate)
			if !errors.Is(err, tc.want) || !strings.Contains(err.Error(), "masking revisions between pages") ||
				!strings.Contains(err.Error(), tc.diagnostic) {
				t.Fatalf("expected %v matching %q in masking scenario, got %v", tc.want, tc.diagnostic, err)
			}
		})
	}
}

func TestCandidateMaskingFilterEvidence(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, filter string
		want         error
		alter        func(*CandidateRequest)
	}{
		{"hidden input matched", maskedInput, ErrViolation, func(r *CandidateRequest) { r.Query.Contains = "" }},
		{"masked replacement missed", maskedLine, ErrViolation, func(r *CandidateRequest) { r.Query.Contains = maskedInput }},
		{"filtered query unavailable", maskedLine, ErrIncomplete, func(r *CandidateRequest) { r.Operation = "" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, candidate := newCandidateFixture(t, "")
			request := candidate.Request
			candidate.Request = func(ctx context.Context, probe CandidateRequest) (*http.Request, error) {
				if probe.Operation == CandidateQuery && probe.Query.Contains == tc.filter {
					tc.alter(&probe)
				}
				return request(ctx, probe)
			}
			err := runCandidateFixture(t, candidate)
			if !errors.Is(err, tc.want) || !strings.Contains(err.Error(), "fresh query must match and page over masked rows") {
				t.Fatalf("expected %v on fresh masked query, got %v", tc.want, err)
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

type successfulQueryWireCase struct {
	name, body string
	rows       []Row
	wantErr    error
}

func successfulQueryWireCases() []successfulQueryWireCase {
	fixture := []Row{{Timestamp: "ts", Line: "fixture"}}
	valid := ndjson("ts", "fixture")
	return []successfulQueryWireCase{
		{"empty body", "", nil, nil},
		{"complete row", valid, fixture, nil},
		{"empty strings", "{\"line\":\"\",\"ts\":\"\"}\n", []Row{{}}, nil},
		{"escaped keys, Unicode, and CRLF", " \t" + `{"l\u0069ne":"雪\ud83e\udd89\nnext","\u0074s":"ts"}` + " \t\r\n",
			[]Row{{Timestamp: "ts", Line: "雪🦉\nnext"}}, nil},
		{"ordered rows and repeats", valid + ndjson("ts", "second") + valid,
			[]Row{fixture[0], {Timestamp: "ts", Line: "second"}, fixture[0]}, nil},
		{"duplicate timestamp", `{"ts":"ts","ts":"ts","line":"fixture"}` + "\n", fixture, ErrViolation},
		{"duplicate decoded line key", `{"ts":"ts","line":"earlier","l\u0069ne":"fixture"}` + "\n", fixture, ErrViolation},
		{"unknown third field", `{"ts":"ts","line":"fixture","extra":false}` + "\n", fixture, ErrViolation},
		{"case variant key", `{"TS":"ts","line":"fixture"}` + "\n", fixture, ErrViolation},
		{"missing timestamp", `{"line":"fixture"}` + "\n", []Row{{Line: "fixture"}}, ErrViolation},
		{"missing line", `{"ts":"ts"}` + "\n", []Row{{Timestamp: "ts"}}, ErrViolation},
		{"null timestamp", `{"ts":null,"line":"fixture"}` + "\n", []Row{{Line: "fixture"}}, ErrViolation},
		{"null line", `{"ts":"ts","line":null}` + "\n", []Row{{Timestamp: "ts"}}, ErrViolation},
		{"number value", `{"ts":"ts","line":42}` + "\n", fixture, ErrViolation},
		{"object value", `{"ts":{},"line":"fixture"}` + "\n", fixture, ErrViolation},
		{"null record", "null\n", []Row{{}}, ErrViolation},
		{"array record", "[]\n", nil, ErrViolation},
		{"malformed object", `{"ts":"ts","line":"fixture",}` + "\n", fixture, ErrViolation},
		{"invalid UTF-8", "{\"ts\":\"ts\",\"line\":\"\xff\"}\n", []Row{{Timestamp: "ts", Line: "�"}}, ErrViolation},
		{"missing final newline", strings.TrimSuffix(valid, "\n"), fixture, ErrViolation},
		{"blank record", "\n", nil, ErrViolation},
		{"trailing blank record", valid + "\n", fixture, ErrViolation},
		{"trailing JSON value on same line", strings.TrimSuffix(valid, "\n") + " {}\n", fixture, ErrViolation},
		{"trailing non-JSON whitespace", strings.TrimSuffix(valid, "\n") + "\u00a0\n", fixture, ErrViolation},
		{"trailing bytes after complete row", valid + "x", fixture, ErrViolation},
	}
}

func TestCandidateSuccessfulQueryWireEvidence(t *testing.T) {
	t.Parallel()
	for _, tc := range successfulQueryWireCases() {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := queryCandidateWirePage(t, http.StatusOK, tc.body, nil)
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("got error %v, want %v", err, tc.wantErr)
			}
			if tc.wantErr != nil {
				if _, ok := err.(candidateViolation); !ok || len(got.Rows) != 0 || got.Complete {
					t.Fatal("malformed response did not produce a candidate violation with no partial evidence")
				}
				return
			}
			if !got.Complete || !slices.Equal(got.Rows, tc.rows) || string(got.Body) != tc.body {
				t.Fatalf("got page %+v, want complete evidence for %q with rows %v", got, tc.body, tc.rows)
			}
		})
	}
}

func TestCandidateQueryWireEvidence(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, body string
		status     int
		headers    http.Header
		wantErr    error
		rows       []Row
		cursor     string
	}{
		{"denial with row", ndjson("ts", "fixture"), http.StatusForbidden, nil, nil, []Row{{"ts", "fixture"}}, ""},
		{"denial with nonstandard row", "forbidden\n" + `{"ts":"ts","line":"earlier","line":"fixture","extra":true}`,
			http.StatusForbidden, nil, nil, []Row{{"ts", "fixture"}}, ""},
		{"denial with cursor", "", http.StatusForbidden, http.Header{"Bog-Next-Cursor": {"opaque"}}, nil, nil, "opaque"},
		{"denial with cursor trailer", "forbidden\n", http.StatusForbidden, http.Header{"Trailer:bog-next-cursor": {"opaque"}}, ErrViolation, nil, ""},
		{"short HTTP body", "", http.StatusOK, http.Header{"Content-Length": {"50"}}, io.ErrUnexpectedEOF, nil, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := queryCandidateWirePage(t, tc.status, tc.body, tc.headers)
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("got error %v, want %v", err, tc.wantErr)
			}
			if !slices.Equal(got.Rows, tc.rows) || got.NextCursor != tc.cursor {
				t.Fatalf("got rows %v and cursor %q, want rows %v and cursor %q", got.Rows, got.NextCursor, tc.rows, tc.cursor)
			}
		})
	}
}

func queryCandidateWirePage(t *testing.T, status int, body string, headers http.Header) (Page, error) {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		for key, values := range headers {
			if name, ok := strings.CutPrefix(key, "Trailer:"); ok {
				w.Header()[http.TrailerPrefix+name] = values
			} else {
				w.Header()[key] = values
			}
		}
		w.WriteHeader(status)
		if _, err := io.WriteString(w, body); err != nil {
			t.Errorf("write fixture response: %v", err)
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
	return target.Query(ctx, QueryRequest{})
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
