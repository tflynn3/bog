package streampolicy

import (
	"context"
	"crypto/sha256"
	"errors"
	"net/http"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestShortArtifactIDs(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		body    string
		headers map[string][]string
		leak    bool
	}{
		{name: "ordinary denial prose", body: "Access denied. No export was authorized."},
		{name: "ordinary problem response", body: `{"title":"Export denied","detail":"This action requires authorization."}`,
			headers: map[string][]string{"Content-Type": {"application/problem+json"}, "X-Denial": {"export denied"}}},
		{name: "plain reference", body: "Access denied for artifact d.", leak: true},
		{name: "escaped JSON reference", body: `{"detail":"Export x is forbidden"}`, leak: true},
		{name: "percent encoded URL reference", body: "Access denied at /exports/%7A", leak: true},
		{name: "opaque header reference", headers: map[string][]string{"Bog-Debug": {"requested=d"}}, leak: true},
		{name: "link with opaque reference", headers: map[string][]string{"Link": {"</exports/x>; rel=download"}}, leak: true},
		{name: "unobserved ID in header", headers: map[string][]string{"Bog-Artifact-Id": {"unobserved"}}, leak: true},
		{name: "unobserved ID in body", body: `{"diagnostic":[{"artifact_id":"unobserved"}]}`, leak: true},
		{name: "manifest header", headers: map[string][]string{"X-Artifact-Manifest": {"private-version"}}, leak: true},
		{name: "offset after large JSON number", body: `[1e999,{"manifest":{"version":"private-version"}}]`, leak: true},
		{name: "artifact terms as JSON values", body: `{"title":"manifest","detail":"An export requires authorization"}`},
		{name: "embedded letters", body: "redzés denied at example.invalid"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			target := &observedTarget{syntheticTarget: newSynthetic()}
			shortIDs := []string{"d", "x", "z"}
			nextID := 0
			alter := func(resp *Response) {
				resp.Body = []byte(tc.body)
				resp.Headers = tc.headers
			}
			target.create = func(_ Target, start *ExportStart) {
				if start.Status == http.StatusForbidden {
					alter(&start.Response)
					return
				}
				id := shortIDs[nextID]
				nextID++
				target.artifacts[id] = target.artifacts[start.ArtifactID]
				delete(target.artifacts, start.ArtifactID)
				start.ArtifactID = id
			}
			target.fetch = func(_ Target, _ string, got *Artifact) {
				if got.Status == http.StatusForbidden {
					alter(&got.Response)
				}
			}
			err := runFixture(t, Adapters{target, target, target, target})
			if tc.leak {
				if !errors.Is(err, ErrViolation) || !strings.Contains(err.Error(), "scoped export") {
					t.Fatalf("Run() = %v, want export evidence violation", err)
				}
			} else if err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestMatrixObservesQueryAndRetryScopes(t *testing.T) {
	t.Parallel()
	target := &observedTarget{syntheticTarget: newSynthetic()}
	issued := make(map[Target]string)
	queries := make(map[Target]map[string][]int)
	retries := make(map[Target][]IngestRequest)
	target.query = func(req QueryRequest, got *Page) error {
		if got.NextCursor != "" {
			issued[req.Target] = got.NextCursor
		}
		if req.Limit == 1 {
			if queries[req.Target] == nil {
				queries[req.Target] = make(map[string][]int)
			}
			queries[req.Target][req.Cursor] = append(queries[req.Target][req.Cursor], got.Status)
		}
		return nil
	}
	target.ingest = func(req IngestRequest, got *Response) {
		if got.Status == http.StatusForbidden {
			retries[req.Target] = append(retries[req.Target], req)
		}
	}
	if err := runFixture(t, Adapters{target, target, target, target}); err != nil {
		t.Fatal(err)
	}

	// List expected grants independently of the production matrix builder.
	queryGrants := map[Target]bool{
		{alice, "fixture-a"}: true, {alice, "fixture-b"}: true, {alice, "fixture-c"}: true, {bob, "fixture-a"}: true,
	}
	if len(issued) != len(queryGrants) {
		t.Fatalf("issued %d cursors, want %d", len(issued), len(queryGrants))
	}
	for issuer := range queryGrants {
		if issued[issuer] == "" {
			t.Errorf("no issued token for %v", issuer)
		}
	}
	for _, principal := range []string{alice, bob, exporter} {
		for _, selector := range []string{"fixture-a", "fixture-b", "fixture-c", unknown} {
			cell := Target{principal, selector}
			authorized := queryGrants[cell]
			for issuer, token := range issued {
				want := http.StatusForbidden
				if authorized {
					want = http.StatusBadRequest
					if issuer == cell {
						want = http.StatusOK
					}
				}
				if got := queries[cell][token]; !slices.Equal(got, []int{want}) {
					t.Errorf("valid token from %v through %v: got %v, want %d", issuer, cell, got, want)
				}
			}
			if !authorized && !slices.Equal(queries[cell][""], []int{http.StatusForbidden}) {
				t.Errorf("missing initial query denial for %v", cell)
			}
			ingestAuthorized := authorized // Reference grants currently pair these two permissions.
			var want []IngestRequest
			if !ingestAuthorized {
				for _, entry := range fixtureAppends(target.fixture) {
					request := entry.request
					request.Target = cell
					conflict := request
					conflict.Body += ndjson("2026-01-01T00:00:07Z", uncommitted)
					fresh := conflict
					fresh.Key = "fixture-unseen-retry-key"
					want = append(want, request, conflict, fresh)
				}
			}
			if !slices.Equal(retries[cell], want) {
				t.Errorf("denied keyed inputs differ for %v", cell)
			}
		}
	}
}

func TestDenialsLeaveStateUnchanged(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		mutate func(*observedTarget)
		want   error
		detail string
	}{
		{
			name: "committed bytes change without row changes",
			mutate: func(target *observedTarget) {
				target.ingest = func(_ IngestRequest, resp *Response) {
					if resp.Status == http.StatusForbidden {
						target.committed = append(target.committed, '\n')
					}
				}
			},
			want: ErrViolation, detail: "committed state changed",
		},
		{
			name: "denied create leaves an unadvertised artifact",
			mutate: func(target *observedTarget) {
				target.create = func(_ Target, start *ExportStart) {
					if start.Status == http.StatusForbidden {
						target.artifacts["unadvertised"] = []byte("private artifact")
					}
				}
			},
			want: ErrViolation, detail: "export state changed",
		},
		{
			name: "denied download changes pending state",
			mutate: func(target *observedTarget) {
				pending := 0
				target.fetch = func(_ Target, _ string, artifact *Artifact) {
					if artifact.Status == http.StatusForbidden {
						pending++
					}
				}
				target.state = func(state *ExportState) { state.Pending = pending }
			},
			want: ErrViolation, detail: "export state changed",
		},
		{
			name: "artifact digest changes at same inventory size",
			mutate: func(target *observedTarget) {
				var inventories int
				target.state = func(state *ExportState) {
					inventories++
					if inventories == 3 {
						for id := range state.Artifacts {
							state.Artifacts[id] = sha256.Sum256([]byte("different persisted bytes"))
							break
						}
					}
				}
			},
			want: ErrViolation, detail: "export state changed",
		},
		{
			name: "unattested export inventory",
			mutate: func(target *observedTarget) {
				target.state = func(state *ExportState) { state.Complete = false }
			},
			want: ErrIncomplete, detail: "export inventory unavailable",
		},
		{
			name: "inventory observer omits known artifact",
			mutate: func(target *observedTarget) {
				target.state = func(state *ExportState) { clear(state.Artifacts) }
			},
			want: ErrViolation, detail: "export inventory differs",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			target := &observedTarget{syntheticTarget: newSynthetic()}
			tc.mutate(target)
			assertFailure(t, target, tc.want, tc.detail)
		})
	}
}

type overlappingRetries struct {
	*syntheticTarget
	mu              sync.Mutex
	exact           int
	allowed, denied chan struct{}
}

func (s *overlappingRetries) Ingest(ctx context.Context, req IngestRequest) (Response, error) {
	// The initial submission, its retry, and the reopened retry precede the
	// matrix. Rendezvous on the next exact retry and a foreign caller's probe.
	base := fixtureAppends(s.fixture)[0].request
	if req.Body == base.Body && req.Key == base.Key {
		s.mu.Lock()
		var peer <-chan struct{}
		if req.Target == base.Target {
			s.exact++
			if s.exact == 4 {
				close(s.allowed)
				peer = s.denied
			}
		}
		if req.Target == (Target{exporter, base.Selector}) {
			close(s.denied)
			peer = s.allowed
		}
		s.mu.Unlock()
		if peer != nil {
			select {
			case <-peer:
			case <-ctx.Done():
				return Response{}, ctx.Err()
			}
		}
	}
	return s.syntheticTarget.Ingest(ctx, req)
}

func TestSameTargetAuthorizedAndDeniedRetriesOverlap(t *testing.T) {
	t.Parallel()
	target := &overlappingRetries{syntheticTarget: newSynthetic(), allowed: make(chan struct{}), denied: make(chan struct{})}
	if err := runFixture(t, Adapters{target, target, target, target}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-target.allowed:
	default:
		t.Fatal("authorized retry did not overlap a denied retry")
	}
	select {
	case <-target.denied:
	default:
		t.Fatal("denied retry did not overlap an authorized retry")
	}
}

func TestRunRequiresExportInventoryObserver(t *testing.T) {
	t.Parallel()
	target := newSynthetic()
	// Expose only the two request methods.
	export := struct{ ExportObserver }{target}
	if err := runFixture(t, Adapters{target, target, target, export}); !errors.Is(err, ErrIncomplete) {
		t.Fatalf("Run() = %v, want missing inventory evidence", err)
	}
}

func TestQueryWireEvidenceMatchesDecodedRows(t *testing.T) {
	t.Parallel()
	row := Row{Timestamp: "2026-01-01T00:00:01Z", Line: "retained evidence"}
	for _, tc := range []struct {
		name string
		rows []Row
		body string
		want error
	}{
		{"valid page", []Row{row}, ndjson(row.Timestamp, row.Line), nil},
		{"valid empty page", nil, "", nil},
		{"missing wire evidence", []Row{row}, "", ErrIncomplete},
		{"different wire row", []Row{row}, ndjson(row.Timestamp, "undisclosed"), ErrViolation},
		{"unreported row", []Row{row}, ndjson(row.Timestamp, row.Line) + ndjson(row.Timestamp, "undisclosed"), ErrViolation},
		{"unreported data on an empty page", nil, ndjson(row.Timestamp, "undisclosed"), ErrViolation},
		{"invalid body", []Row{row}, "{", ErrViolation},
		{"unknown row fields", []Row{row}, `{"ts":"2026-01-01T00:00:01Z","line":"retained evidence","private":"payload"}`, ErrViolation},
	} {
		t.Run(tc.name, func(t *testing.T) {
			page := Page{Response: response(http.StatusOK, tc.body), Rows: tc.rows}
			if got := checkQueryBody(page, 1); !errors.Is(got, tc.want) {
				t.Errorf("checkQueryBody() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestDeniedQueryWireRows(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		body string
		want error
	}{
		{`{"message":"forbidden","rows":[]}`, nil},
		{`{"message":"forbidden","records":null}`, nil},
		{`{"message":"forbidden","rows":[{"line":"private"}]}`, ErrViolation},
		{`{"message":"forbidden","nested":{"records":[{"line":"private"}]}}`, ErrViolation},
	} {
		t.Run(tc.body, func(t *testing.T) {
			target := &observedTarget{syntheticTarget: newSynthetic()}
			target.query = func(_ QueryRequest, page *Page) error {
				if page.Status == http.StatusForbidden {
					page.Body = []byte(tc.body)
				}
				return nil
			}
			err := runFixture(t, Adapters{target, target, target, target})
			if !errors.Is(err, tc.want) {
				t.Fatalf("Run() = %v, want %v", err, tc.want)
			}
		})
	}
}

type cancelDuringMatrix struct {
	*syntheticTarget
	started chan struct{}
	drained chan struct{}
}

func (s *cancelDuringMatrix) Query(ctx context.Context, req QueryRequest) (Page, error) {
	if req.Target == (Target{exporter, unknown}) && req.Cursor == "" {
		close(s.started)
		<-ctx.Done()
		close(s.drained)
		return Page{}, ctx.Err()
	}
	return s.syntheticTarget.Query(ctx, req)
}

func TestCancellationDrainsActiveProbes(t *testing.T) {
	t.Parallel()
	target := &cancelDuringMatrix{syntheticTarget: newSynthetic(), started: make(chan struct{}), drained: make(chan struct{})}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	adapters := Adapters{target, target, target, target}
	result := make(chan error, 1)
	go func() { result <- Run(ctx, adapters) }()
	select {
	case <-target.started:
		cancel()
	case err := <-result:
		t.Fatalf("Run() returned before active probe: %v", err)
	case <-ctx.Done():
		t.Fatal("matrix did not reach the cancellation probe")
	}
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) || !errors.Is(err, ErrIncomplete) {
			t.Fatalf("Run() = %v, want cancellation and incomplete evidence", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run() did not return after cancellation")
	}
	select {
	case <-target.drained:
	default:
		t.Fatal("Run() returned with a probe still active")
	}
}
