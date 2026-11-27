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
			name: "authorized query header leaks another stream's retained line", phase: "authorized query in stream 1", want: ErrViolation,
			query: func(q QueryRequest, p *Page) error {
				if q.Selector == "fixture-a" && p.Status == http.StatusOK {
					p.Headers = map[string][]string{"Bog-Debug": {"retained-alice-b [redacted]"}}
				}
				return nil
			},
		},
		{
			name: "authorized cursor response body leaks another stream's policy", phase: "issuer resumes cursor", want: ErrViolation,
			query: func(q QueryRequest, p *Page) error {
				if q.Cursor != "" && p.Status == http.StatusOK {
					p.Body = append(p.Body, "fixture-policy-c-v1"...)
				}
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

func TestIngestAndExportResponses(t *testing.T) {
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
			name: "authorized append and retry headers leak another stream's retained line", phase: "retry namespace 4: first submission",
			ingest: func(r IngestRequest, resp *Response) {
				if r.Principal == bob && r.Selector == "fixture-a" && resp.Status == http.StatusNoContent {
					resp.Headers = map[string][]string{"Bog-Debug": {"retained-alice-c [redacted]"}}
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
			name: "authorized export creation header leaks another stream's retained line", phase: "authorized export in stream 1",
			create: func(r Target, resp *ExportStart) {
				if r.Selector == "fixture-a" && resp.Status == http.StatusOK {
					resp.Headers = map[string][]string{"Bog-Debug": {"retained-alice-b [redacted]"}}
				}
			},
		},
		{
			name: "authorized export creation body leaks another tenant", phase: "authorized export in stream 1",
			create: func(r Target, resp *ExportStart) {
				if r.Selector == "fixture-a" && resp.Status == http.StatusOK {
					resp.Body = []byte("fixture-tenant-two")
				}
			},
		},
		{
			name: "authorized export ID leaks another stream's selector", phase: "authorized export in stream 1",
			create: func(r Target, resp *ExportStart) {
				if r.Selector == "fixture-a" && resp.Status == http.StatusOK {
					resp.ArtifactID += "-fixture-b"
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

func TestMissingArtifactDeniedThroughAuthorizedStream(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		alter  func(*Artifact, string)
		detail string
	}{
		{"status", func(a *Artifact, _ string) { a.Status = http.StatusNotFound }, "want 403"},
		{"body", func(a *Artifact, _ string) { a.Body = []byte("artifact missing\n") }, "different response evidence"},
		{"header", func(a *Artifact, _ string) {
			a.Headers = map[string][]string{"Bog-Result": {"artifact missing"}}
		}, "different response evidence"},
		{"artifact ID", func(a *Artifact, id string) { a.Body = []byte(id) }, "unexpected fixture marker"},
		{"fixture secret", func(a *Artifact, _ string) { a.Body = []byte(plain) }, "unexpected fixture marker"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			target := &observedTarget{syntheticTarget: newSynthetic()}
			allowed := Target{bob, target.fixture.Streams[1].Selector}
			target.fetch = func(request Target, id string, artifact *Artifact) {
				if _, exists := target.artifacts[id]; !exists && request == allowed && artifact.Status == http.StatusForbidden {
					tc.alter(artifact, id)
				}
			}
			err := assertFailure(t, target, ErrViolation, "artifact requested in another authorized stream")
			if !strings.Contains(err.Error(), tc.detail) {
				t.Fatalf("diagnostic does not report %q", tc.detail)
			}
		})
	}
}

func TestExportDenialMatrix(t *testing.T) {
	t.Parallel()
	target := &observedTarget{syntheticTarget: newSynthetic()}
	a, b, c := target.fixture.Streams[0].Selector, target.fixture.Streams[1].Selector, target.fixture.Streams[2].Selector
	ids := make(map[string]string)
	target.create = func(request Target, start *ExportStart) {
		if start.ArtifactID != "" {
			ids[request.Selector] = start.ArtifactID
		}
	}
	denials := make(map[Target][]string)
	target.fetch = func(request Target, id string, artifact *Artifact) {
		if artifact.Status == http.StatusForbidden {
			denials[request] = append(denials[request], id)
		}
	}
	if err := runFixture(t, Adapters{target, target, target, target}); err != nil {
		t.Fatal(err)
	}
	for _, selector := range []string{a, b, c} {
		if ids[selector] == "" {
			t.Fatalf("no positive artifact for %s", selector)
		}
	}
	for _, tc := range []struct {
		request Target
		sources []string
	}{
		{Target{alice, a}, []string{b, c}},
		{Target{alice, b}, []string{a, c}},
		{Target{alice, c}, []string{a, b}},
		{Target{alice, unknown}, []string{a, b, c}},
		{Target{bob, a}, []string{a, b, c}},
		{Target{bob, b}, []string{a, c}},
		{Target{bob, c}, []string{a, b, c}},
		{Target{bob, unknown}, []string{a, b, c}},
		{Target{exporter, a}, []string{b, c}},
		{Target{exporter, b}, []string{a, b, c}},
		{Target{exporter, c}, []string{a, b, c}},
		{Target{exporter, unknown}, []string{a, b, c}},
	} {
		got := denials[tc.request]
		if len(got) != 2*len(tc.sources) {
			t.Errorf("%+v observed %d denials, want %d", tc.request, len(got), 2*len(tc.sources))
			continue
		}
		for i, source := range tc.sources {
			existing, missing := got[2*i], got[2*i+1]
			if _, exists := target.artifacts[missing]; existing != ids[source] || missing == "" || exists {
				t.Errorf("%+v has no existing/absent pair for %s", tc.request, source)
			}
		}
	}
}

func TestExportDenialRegression(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name      string
		principal string
		selector  string
		source    string // Empty for an absent-ID fetch.
		alter     func(*Artifact, string)
		detail    string
	}{
		{
			name:      "reverse same-tenant access through another authorized stream",
			principal: alice, selector: "fixture-a", source: "fixture-b",
			alter: func(a *Artifact, _ string) { a.Status = http.StatusOK }, detail: "want 403",
		},
		{
			name:      "reverse cross-tenant access through the same stream ID",
			principal: alice, selector: "fixture-a", source: "fixture-c",
			alter: func(a *Artifact, _ string) { a.Status = http.StatusOK }, detail: "want 403",
		},
		{
			name:      "same-scope request from a different principal without a grant",
			principal: exporter, selector: "fixture-b", source: "fixture-b",
			alter: func(a *Artifact, _ string) { a.Bytes = []byte("private data") }, detail: "disclosed artifact evidence",
		},
		{
			name:      "denied cross-tenant request returns records",
			principal: bob, selector: "fixture-b", source: "fixture-c",
			alter:  func(a *Artifact, _ string) { a.Records = []Record{{Row: Row{Line: "private record"}}} },
			detail: "disclosed artifact evidence",
		},
		{
			name:      "existing ID changes the header for a cross-tenant request without a grant",
			principal: bob, selector: "fixture-c", source: "fixture-b",
			alter: func(a *Artifact, _ string) {
				a.Headers = map[string][]string{"Bog-Result": {"artifact found"}}
			},
			detail: "different response evidence",
		},
		{
			name:      "absent ID changes the body through a grant for another scope",
			principal: alice, selector: "fixture-a",
			alter: func(a *Artifact, _ string) { a.Body = []byte("artifact missing\n") }, detail: "different response evidence",
		},
		{
			name:      "unknown selector echoes the third stream artifact ID",
			principal: exporter, selector: unknown, source: "fixture-c",
			alter: func(a *Artifact, id string) { a.Body = []byte(id) }, detail: "unexpected fixture marker",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			target := &observedTarget{syntheticTarget: newSynthetic()}
			var sourceID string
			target.create = func(request Target, start *ExportStart) {
				if request.Selector == tc.source && start.ArtifactID != "" {
					sourceID = start.ArtifactID
				}
			}
			target.fetch = func(request Target, id string, artifact *Artifact) {
				if request != (Target{tc.principal, tc.selector}) || artifact.Status != http.StatusForbidden {
					return
				}
				_, exists := target.artifacts[id]
				if (tc.source == "" && !exists) || (exists && id == sourceID) {
					tc.alter(artifact, id)
				}
			}
			assertFailure(t, target, ErrViolation, tc.detail)
		})
	}
}

func TestExportRequiresAnAbsentArtifactProbe(t *testing.T) {
	t.Parallel()
	target := &observedTarget{syntheticTarget: newSynthetic()}
	target.create = func(_ Target, start *ExportStart) {
		const reserved = "fixture-missing-artifact"
		if start.ArtifactID != "" {
			target.artifacts[reserved] = target.artifacts[start.ArtifactID]
			delete(target.artifacts, start.ArtifactID)
			start.ArtifactID = reserved
		}
	}
	assertFailure(t, target, ErrIncomplete, "missing-artifact probe does not name an absent artifact")
}

func assertFailure(t *testing.T, target *observedTarget, want error, phase string) error {
	t.Helper()
	err := runFixture(t, Adapters{target, target, target, target})
	if err == nil {
		t.Fatalf("Run() passed, want %v in %q", want, phase)
	}
	for _, sensitive := range []string{"fixture-", "retained-alice-", "retained-bob-", "synthetic-cursor-", "synthetic-artifact-"} {
		if strings.Contains(err.Error(), sensitive) {
			t.Fatal("diagnostic disclosed a fixture value")
		}
	}
	if !errors.Is(err, want) || !strings.Contains(err.Error(), phase) {
		t.Fatalf("Run() = %v, want %v in %q", err, want, phase)
	}
	return err
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
