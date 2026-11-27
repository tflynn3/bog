package streampolicy

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"slices"
	"strings"
	"testing"
	"time"
)

func runFixture(t *testing.T, adapters Adapters) error {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return Run(ctx, adapters)
}

func TestReferenceScenario(t *testing.T) {
	t.Parallel()
	// Independent runs exercise fixture ownership and global state under -race.
	for range 8 {
		t.Run("fresh target", func(t *testing.T) {
			t.Parallel()
			target := newSynthetic()
			if err := runFixture(t, target.adapters()); err != nil {
				t.Fatal(err)
			}
			if got := len(target.frames); got != 4 {
				t.Fatalf("committed %d frames, want 4", got)
			}
		})
	}
}

// observedTarget modifies one category of real synthetic observations while
// continuing to drive the same target through the other adapter interfaces.
type observedTarget struct {
	*syntheticTarget
	query  func(QueryRequest, *Page) error
	ingest func(IngestRequest, *Response)
	replay func(*Committed)
	create func(Target, *ExportStart)
	fetch  func(Target, string, *Artifact)
}

func (o *observedTarget) Query(ctx context.Context, request QueryRequest) (Page, error) {
	got, err := o.syntheticTarget.Query(ctx, request)
	if err == nil && o.query != nil {
		err = o.query(request, &got)
	}
	return got, err
}

func (o *observedTarget) Ingest(ctx context.Context, request IngestRequest) (Response, error) {
	got, err := o.syntheticTarget.Ingest(ctx, request)
	if err == nil && o.ingest != nil {
		o.ingest(request, &got)
	}
	return got, err
}

func (o *observedTarget) RestartAndReplay(ctx context.Context) (Committed, error) {
	got, err := o.syntheticTarget.RestartAndReplay(ctx)
	if err == nil && o.replay != nil {
		o.replay(&got)
	}
	return got, err
}

func (o *observedTarget) CreateExport(ctx context.Context, target Target) (ExportStart, error) {
	got, err := o.syntheticTarget.CreateExport(ctx, target)
	if err == nil && o.create != nil {
		o.create(target, &got)
	}
	return got, err
}

func (o *observedTarget) FetchExport(ctx context.Context, target Target, id string) (Artifact, error) {
	got, err := o.syntheticTarget.FetchExport(ctx, target, id)
	if err == nil && o.fetch != nil {
		o.fetch(target, id, &got)
	}
	return got, err
}

func TestRejectionAndPositiveControls(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name  string
		phase string
		query func(QueryRequest, *Page) error
		want  error
	}{
		{
			name: "blank authorized query", phase: "authorization and cursor scope", want: ErrViolation,
			query: func(_ QueryRequest, p *Page) error {
				p.Rows = nil
				return nil
			},
		},
		{
			name: "positive rows without response bytes", phase: "authorization and cursor scope", want: ErrIncomplete,
			query: func(_ QueryRequest, p *Page) error {
				p.Body = nil
				return nil
			},
		},
		{
			name: "missing issuing cursor", phase: "opening page", want: ErrIncomplete,
			query: func(_ QueryRequest, p *Page) error {
				p.NextCursor = ""
				delete(p.Headers, "Bog-Next-Cursor")
				return nil
			},
		},
		{
			name: "cursor checked before authorization", phase: "query authorization before validation", want: ErrViolation,
			query: func(q QueryRequest, p *Page) error {
				if p.Status == http.StatusForbidden && q.Cursor != "" {
					p.Status = http.StatusBadRequest
				}
				return nil
			},
		},
		{
			name: "unknown selector has a different denial", phase: "authorization and cursor scope", want: ErrViolation,
			query: func(q QueryRequest, p *Page) error {
				if q.Selector == unknown {
					p.Headers = map[string][]string{"Bog-Reason": {"unknown"}}
				}
				return nil
			},
		},
		{
			name: "rejection returns decoded rows", phase: "query authorization before validation", want: ErrViolation,
			query: func(q QueryRequest, p *Page) error {
				if p.Status == http.StatusForbidden && q.Cursor != "" {
					p.Rows = []Row{{"ts", "unexpected"}}
				}
				return nil
			},
		},
		{
			name: "rejection returns a cursor", phase: "query authorization before validation", want: ErrViolation,
			query: func(q QueryRequest, p *Page) error {
				if p.Status == http.StatusForbidden && q.Cursor != "" {
					p.NextCursor = q.Cursor
				}
				return nil
			},
		},
		{
			name: "authorized scope accepts foreign cursor", phase: "authorized cursor scope mismatch", want: ErrViolation,
			query: func(q QueryRequest, p *Page) error {
				if p.Status == http.StatusBadRequest && q.Principal == bob {
					p.Status = http.StatusOK
				}
				return nil
			},
		},
		{
			name: "scope mismatch leaks which component failed", phase: "authorization and cursor scope", want: ErrViolation,
			query: func(q QueryRequest, p *Page) error {
				if p.Status == http.StatusBadRequest && q.Principal == bob {
					p.Body = []byte("different principal")
				}
				return nil
			},
		},
		{
			name: "adapter errors are incomplete and opaque", phase: "authorization and cursor scope", want: ErrIncomplete,
			query: func(QueryRequest, *Page) error {
				return errors.New(plain + " synthetic-cursor-0001")
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			target := &observedTarget{syntheticTarget: newSynthetic(), query: tc.query}
			assertFailure(t, target, tc.want, tc.phase)
		})
	}
}

func TestDurableEvidence(t *testing.T) {
	t.Parallel()
	const (
		replayPhase  = "committed bytes and replay"
		exportPhase  = "authorized export"
		encoded      = `fixture-secret-\u0022\u005c-\u96ea\ud83e\udd89`
		unrelated    = "retained-alice-b"
		responseLeak = "outside authorized scope"
	)
	for _, tc := range []struct {
		name, phase string
		replay      func(*Committed)
		fetch       func(*Artifact)
		want        error
	}{
		{name: "unattested replay", phase: replayPhase, want: ErrIncomplete, replay: func(c *Committed) { c.Complete = false }},
		{name: "missing bytes", phase: replayPhase, want: ErrIncomplete, replay: func(c *Committed) { c.Bytes = nil }},
		{name: "oversize bytes", phase: replayPhase, want: ErrIncomplete, replay: func(c *Committed) { c.Bytes = make([]byte, MaxEvidenceBytes+1) }},
		{name: "missing replay records", phase: replayPhase, want: ErrIncomplete, replay: func(c *Committed) { c.Records = nil }},
		{name: "unrelated replay bytes", phase: replayPhase, want: ErrIncomplete, replay: func(c *Committed) { c.Bytes = []byte("opaque") }},
		{name: "raw commit has plain secret", phase: replayPhase, want: ErrViolation, replay: func(c *Committed) { c.Bytes = append(c.Bytes, plain...) }},
		{name: "raw commit has escaped secret", phase: replayPhase, want: ErrViolation, replay: func(c *Committed) { c.Bytes = append(c.Bytes, encoded...) }},
		{name: "duplicate replay", phase: replayPhase, want: ErrViolation, replay: func(c *Committed) { c.Records = append(c.Records, c.Records[0]) }},
		{name: "missing retry namespace", phase: replayPhase, want: ErrViolation, replay: func(c *Committed) { c.Records = c.Records[:3] }},
		{name: "replay has secret despite clean bytes", phase: replayPhase, want: ErrViolation, replay: func(c *Committed) { c.Records[0].Row.Line += escaped }},
		{name: "replay loses permitted text", phase: replayPhase, want: ErrViolation, replay: func(c *Committed) { c.Records[0].Row.Line = replacement }},
		{name: "export integrity unavailable", phase: exportPhase, want: ErrIncomplete, fetch: func(a *Artifact) { a.Verified = false }},
		{name: "missing artifact response", phase: exportPhase, want: ErrIncomplete, fetch: func(a *Artifact) { a.Complete = false }},
		{name: "missing manifest version", phase: exportPhase, want: ErrIncomplete, fetch: func(a *Artifact) { a.Manifest.Version = "" }},
		{name: "missing policy versions", phase: exportPhase, want: ErrIncomplete, fetch: func(a *Artifact) { a.Manifest.PolicyVersions = nil }},
		{name: "missing decoded export", phase: exportPhase, want: ErrIncomplete, fetch: func(a *Artifact) { a.Records = nil }},
		{name: "missing export bytes", phase: exportPhase, want: ErrIncomplete, fetch: func(a *Artifact) { a.Bytes = nil }},
		{name: "export has raw secret", phase: exportPhase, want: ErrViolation, fetch: func(a *Artifact) { a.Bytes = append(a.Bytes, encoded...) }},
		{name: "export raw scope contamination", phase: exportPhase, want: ErrViolation, fetch: func(a *Artifact) { a.Bytes = append(a.Bytes, unrelated...) }},
		{name: "export raw metadata contamination", phase: exportPhase, want: ErrViolation, fetch: func(a *Artifact) { a.Bytes = append(a.Bytes, ReferenceFixture().Streams[2].Scope.Tenant...) }},
		{name: "export response leaks other stream", phase: exportPhase, want: ErrViolation, fetch: func(a *Artifact) { a.Body = []byte(responseLeak + unrelated) }},
		{name: "export has wrong scope", phase: exportPhase, want: ErrViolation, fetch: func(a *Artifact) { a.Manifest.Scope = ReferenceFixture().Streams[1].Scope }},
		{name: "export has wrong policy version", phase: exportPhase, want: ErrViolation, fetch: func(a *Artifact) { a.Manifest.PolicyVersions = []string{"wrong"} }},
		{name: "export contains uncommitted row", phase: exportPhase, want: ErrViolation, fetch: func(a *Artifact) { a.Bytes = append(a.Bytes, uncommitted...) }},
		{name: "export decoded records differ", phase: exportPhase, want: ErrViolation, fetch: func(a *Artifact) { a.Records[0].Row.Line += escaped }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			target := &observedTarget{syntheticTarget: newSynthetic(), replay: tc.replay}
			target.fetch = func(_ Target, _ string, a *Artifact) {
				if a.Status == http.StatusOK {
					tc.fetch(a)
				}
			}
			assertFailure(t, target, tc.want, tc.phase)
		})
	}
}

func TestIngestAndExportDenials(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		phase  string
		ingest func(IngestRequest, *Response)
		create func(Target, *ExportStart)
		fetch  func(Target, string, *Artifact)
	}{
		{
			name: "ingest looks up retry before authorization", phase: "ingest denied",
			ingest: func(r IngestRequest, resp *Response) {
				if r.Principal == exporter {
					resp.Status = http.StatusNoContent
				}
			},
		},
		{
			name: "namespace reports conflict on first append", phase: "retry namespace 2",
			ingest: func(r IngestRequest, resp *Response) {
				if r.Selector == "fixture-b" && resp.Status == http.StatusNoContent {
					resp.Status = http.StatusConflict
				}
			},
		},
		{
			name: "redactor failure reports success", phase: "redaction must fail the whole batch",
			ingest: func(r IngestRequest, resp *Response) {
				if strings.Contains(r.Body, failed) {
					resp.Status = http.StatusNoContent
				}
			},
		},
		{
			name: "export creation leaks existence", phase: "scoped export",
			create: func(r Target, resp *ExportStart) {
				if r.Selector == unknown {
					resp.Body = []byte("unknown stream")
				}
			},
		},
		{
			name: "denied create leaks artifact ID", phase: "rejected export disclosed",
			create: func(_ Target, resp *ExportStart) {
				if resp.Status == http.StatusForbidden {
					resp.ArtifactID = "private-artifact"
				}
			},
		},
		{
			name: "download leaks whether artifact exists", phase: "scoped export",
			fetch: func(_ Target, id string, resp *Artifact) {
				if resp.Status == http.StatusForbidden && strings.HasPrefix(id, "synthetic-artifact-") {
					resp.Body = []byte("known artifact")
				}
			},
		},
		{
			name: "denied download has manifest", phase: "rejected download disclosed",
			fetch: func(_ Target, _ string, resp *Artifact) {
				if resp.Status == http.StatusForbidden {
					resp.Manifest.Version = "private-version"
				}
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			target := &observedTarget{
				syntheticTarget: newSynthetic(), ingest: tc.ingest, create: tc.create, fetch: tc.fetch,
			}
			assertFailure(t, target, ErrViolation, tc.phase)
		})
	}
}

func TestReopenedRetryMustReturnRecordedOutcome(t *testing.T) {
	t.Parallel()
	reopened := false
	target := &observedTarget{
		syntheticTarget: newSynthetic(),
		replay:          func(*Committed) { reopened = true },
		ingest: func(_ IngestRequest, resp *Response) {
			if reopened && resp.Status == http.StatusNoContent {
				resp.Headers = map[string][]string{"Bog-Result": {"different outcome"}}
			}
		},
	}
	assertFailure(t, target, ErrViolation, "reopened retry namespace 1 returned a different outcome")
}

func TestAuthenticatedExportBytesAreChecked(t *testing.T) {
	t.Parallel()
	target := &observedTarget{syntheticTarget: newSynthetic()}
	target.create = func(_ Target, start *ExportStart) {
		if start.ArtifactID != "" {
			data := target.artifacts[start.ArtifactID]
			data[len(data)-1] ^= 1
		}
	}
	assertFailure(t, target, ErrIncomplete, "authorized export")
}

func TestArtifactDeniedThroughOtherAuthorizedStream(t *testing.T) {
	t.Parallel()
	target := &observedTarget{syntheticTarget: newSynthetic()}
	target.fetch = func(request Target, _ string, artifact *Artifact) {
		if request.Principal == bob && request.Selector == "fixture-b" && artifact.Status == http.StatusForbidden {
			artifact.Status = http.StatusOK
		}
	}
	assertFailure(t, target, ErrViolation, "artifact requested in another authorized stream")
}

func assertFailure(t *testing.T, target *observedTarget, want error, phase string) {
	t.Helper()
	err := runFixture(t, Adapters{target, target, target, target})
	if !errors.Is(err, want) || !strings.Contains(err.Error(), phase) {
		t.Fatalf("Run() = %v, want %v in %q", err, want, phase)
	}
	for _, sensitive := range []string{plain, escaped, failed, key, "synthetic-cursor-"} {
		if strings.Contains(err.Error(), sensitive) {
			t.Fatal("diagnostic disclosed a fixture value")
		}
	}
}

func TestAbsentAndCanceledObservers(t *testing.T) {
	t.Parallel()
	target := newSynthetic()
	for _, adapters := range []Adapters{
		{nil, target, target, target},
		{target, nil, target, target},
		{target, target, nil, target},
		{target, target, target, nil},
	} {
		if err := runFixture(t, adapters); !errors.Is(err, ErrIncomplete) {
			t.Fatalf("Run() = %v, want incomplete", err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := Run(ctx, target.adapters()); !errors.Is(err, ErrIncomplete) || !errors.Is(err, context.Canceled) {
		t.Fatalf("Run() = %v, want incomplete and canceled", err)
	}
	if len(target.committed) != 0 {
		t.Fatal("canceled run mutated target")
	}
}

func TestJSONEscapesInEvidence(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, input, want string
	}{
		{"plain", "no escapes", "no escapes"},
		{"JSON line with surrogate pair", ndjson("ts", escaped), "{\"ts\":\"ts\",\"line\":\"" + escaped + "\"}\n"},
		{"mixed escapes", `\u0061\u0062\u0063\/\n`, "abc/\n"},
		{"invalid escape followed by valid", `\uXYZZ\u0031`, `\uXYZZ1`},
		{"valid escape followed by invalid", `\u0031\uXYZZ`, `1\uXYZZ`},
		{"short trailing escape", `\u1`, `\u1`},
		{"literal escape remains literal", `\\u0031`, `\u0031`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			original := []byte(tc.input)
			clone := slices.Clone(original)
			if got := jsonUnescaped(original); string(got) != tc.want {
				t.Errorf("jsonUnescaped(%q) = %q, want %q", tc.input, got, tc.want)
			}
			if !bytes.Equal(original, clone) {
				t.Error("scanner mutated observed bytes")
			}
		})
	}
}
