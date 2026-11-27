package streampolicy

import (
	"net/http"
	"slices"
	"strings"
	"testing"
)

// Declare the expected grants independently of the runner's matrix builder.
type expectedGrants struct {
	target                Target
	query, ingest, export bool
}

var policyCases = []expectedGrants{
	{Target{alice, "fixture-a"}, true, true, true},
	{Target{alice, "fixture-b"}, true, true, true},
	{Target{alice, "fixture-c"}, true, true, true},
	{Target{alice, unknown}, false, false, false},
	{Target{bob, "fixture-a"}, true, true, false},
	{Target{bob, "fixture-b"}, false, false, true},
	{Target{bob, "fixture-c"}, false, false, false},
	{Target{bob, unknown}, false, false, false},
	{Target{exporter, "fixture-a"}, false, false, true},
	{Target{exporter, "fixture-b"}, false, false, false},
	{Target{exporter, "fixture-c"}, false, false, false},
	{Target{exporter, unknown}, false, false, false},
}

type queryObservation struct {
	request QueryRequest
	status  int
	next    string
}

type retryObservation struct {
	request IngestRequest
	status  int
}

type downloadObservation struct {
	id     string
	status int
}

func TestPolicyMatrix(t *testing.T) {
	t.Parallel()
	target := &observedTarget{syntheticTarget: newSynthetic()}
	var epoch int
	var queries [2][]queryObservation
	var retries [2][]retryObservation
	starts := make(map[Target]ExportStart)
	downloads := make(map[Target][]downloadObservation)
	target.replay = func(*Committed) { epoch++ }
	target.query = func(request QueryRequest, page *Page) error {
		if epoch >= len(queries) {
			t.Fatal("query after final replay")
		}
		queries[epoch] = append(queries[epoch], queryObservation{request, page.Status, page.NextCursor})
		return nil
	}
	target.ingest = func(request IngestRequest, response *Response) {
		if epoch >= len(retries) {
			t.Fatal("ingest after final replay")
		}
		retries[epoch] = append(retries[epoch], retryObservation{request, response.Status})
	}
	target.create = func(request Target, start *ExportStart) {
		if _, found := starts[request]; found {
			t.Fatalf("multiple create probes for %+v", request)
		}
		starts[request] = *start
	}
	target.fetch = func(request Target, id string, artifact *Artifact) {
		downloads[request] = append(downloads[request], downloadObservation{id, artifact.Status})
	}
	if err := runFixture(t, Adapters{target, target, target, target}); err != nil {
		t.Fatal(err)
	}
	if epoch != 2 {
		t.Fatalf("observed %d replays, want 2", epoch)
	}
	for i := range queries {
		t.Run([]string{"before replay", "after replay"}[i], func(t *testing.T) {
			checkQueryMatrix(t, queries[i])
			checkRetryMatrix(t, target.fixture, retries[i], i == 1)
		})
	}
	checkExportMatrix(t, starts, downloads)
}

func checkQueryMatrix(t *testing.T, observations []queryObservation) {
	t.Helper()
	issued := make(map[string]Target)
	for _, call := range observations {
		if call.next != "" {
			if _, exists := issued[call.next]; exists {
				t.Fatal("opening probes reused a cursor")
			}
			issued[call.next] = call.request.Target
		}
	}
	if len(issued) != 4 {
		t.Fatalf("issued %d cursors, want one per allowed query target", len(issued))
	}
	for _, expected := range policyCases {
		cursors := make(map[string][]int)
		openings := make(map[string]int)
		for _, call := range observations {
			if call.request.Target != expected.target {
				continue
			}
			if call.request.Cursor == "" {
				want := http.StatusForbidden
				if expected.query {
					want = http.StatusOK
				}
				if call.status != want {
					t.Errorf("%+v initial query got %d, want %d", expected.target, call.status, want)
				}
				openings[call.request.Contains]++
				continue
			}
			token := call.request.Cursor
			cursors[token] = append(cursors[token], call.status)
		}
		if expected.query {
			for _, filter := range []string{"", replacement, plain, escaped, failed, uncommitted} {
				if openings[filter] == 0 {
					t.Errorf("%+v skipped an authorized filter probe", expected.target)
				}
			}
		} else if openings[""] != 1 {
			t.Errorf("%+v initial denials = %d, want 1", expected.target, openings[""])
		}

		if len(cursors) != 2*len(issued) {
			t.Errorf("%+v received %d distinct continuation probes, want %d", expected.target, len(cursors), 2*len(issued))
		}
		for token := range issued {
			if len(cursors[token]) != 1 {
				t.Errorf("%+v did not observe one probe for an issued cursor", expected.target)
			}
		}
		for token, statuses := range cursors {
			want := http.StatusForbidden
			if expected.query {
				want = http.StatusBadRequest
				if issued[token] == expected.target {
					want = http.StatusOK
				}
			}
			if !slices.Equal(statuses, []int{want}) {
				t.Errorf("%+v continuation statuses %v, want [%d]", expected.target, statuses, want)
			}
		}
	}
}

func checkRetryMatrix(t *testing.T, fixture Fixture, observations []retryObservation, reopened bool) {
	t.Helper()
	calls := make(map[IngestRequest][]int)
	for _, call := range observations {
		calls[call.request] = append(calls[call.request], call.status)
	}
	for _, expected := range policyCases {
		for _, source := range fixtureAppends(fixture) {
			request := source.request
			request.Target = expected.target
			changed := request
			changed.Body += ndjson("2026-01-01T00:00:06Z", uncommitted)
			fresh := request
			fresh.Key = unusedKey
			if expected.ingest {
				if request.Target != source.request.Target {
					continue
				}
				want := []int{http.StatusNoContent}
				if !reopened {
					want = append(want, http.StatusNoContent)
				}
				if !slices.Equal(calls[request], want) || !slices.Equal(calls[changed], []int{http.StatusConflict}) {
					t.Errorf("%+v missing authorized retry or conflict", expected.target)
				}
				continue
			}
			for _, probe := range []IngestRequest{request, changed, fresh} {
				if !slices.Equal(calls[probe], []int{http.StatusForbidden}) {
					t.Errorf("%+v missing a forbidden retry probe", expected.target)
				}
			}
		}
	}
}

func checkExportMatrix(t *testing.T, starts map[Target]ExportStart, downloads map[Target][]downloadObservation) {
	t.Helper()
	for _, expected := range policyCases {
		start := starts[expected.target]
		want := http.StatusForbidden
		if expected.export {
			want = http.StatusOK
		}
		if start.Status != want || (start.ArtifactID != "") != expected.export {
			t.Errorf("%+v create status %d and ID present %t, want %d and %t",
				expected.target, start.Status, start.ArtifactID != "", want, expected.export)
		}
		var observed, absent int
		for _, source := range policyCases {
			if !source.export {
				continue
			}
			id := starts[source.target].ArtifactID
			want := http.StatusForbidden
			if expected.export && expected.target.Selector == source.target.Selector {
				want = http.StatusOK
			}
			var hits []int
			for _, call := range downloads[expected.target] {
				if call.id == id {
					hits = append(hits, call.status)
				}
			}
			if !slices.Equal(hits, []int{want}) {
				t.Errorf("source %+v, downloader %+v: got %v, want [%d]", source.target, expected.target, hits, want)
			}
			observed += len(hits)
		}
		for _, call := range downloads[expected.target] {
			if call.id == missingArtifact && call.status == http.StatusForbidden {
				absent++
			}
		}
		if observed != 5 || absent != 5 || len(downloads[expected.target]) != observed+absent {
			t.Errorf("%+v downloads included %d existing and %d absent controls, want 5 each", expected.target, observed, absent)
		}
	}
}

func TestPolicyMatrixRegressions(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, phase string
		want        error
		mutate      func(*observedTarget)
	}{
		{
			name:  "initial query distinguishes unknown selector from missing grant",
			phase: "query authorization before validation (initial", want: ErrViolation,
			mutate: func(target *observedTarget) {
				target.query = func(q QueryRequest, p *Page) error {
					if q.Target == (Target{alice, unknown}) && q.Cursor == "" {
						p.Body = []byte("selector unavailable")
					}
					return nil
				}
			},
		},
		{
			name:  "second tenant can resume within first tenant",
			phase: "authorized cursor scope mismatch", want: ErrViolation,
			mutate: func(target *observedTarget) {
				target.query = func(q QueryRequest, p *Page) error {
					issuer, issued := target.cursors[q.Cursor]
					if issued && issuer.request.Selector == "fixture-c" && q.Selector == "fixture-a" && p.Status == http.StatusBadRequest {
						p.Status = http.StatusOK
					}
					return nil
				}
			},
		},
		{
			name:  "other principal's valid continuation leaks its token",
			phase: "authorized cursor scope mismatch", want: ErrViolation,
			mutate: func(target *observedTarget) {
				target.query = func(q QueryRequest, p *Page) error {
					issuer, issued := target.cursors[q.Cursor]
					if issued && issuer.request.Principal == bob && q.Target == (Target{alice, "fixture-a"}) {
						p.Headers = map[string][]string{"Bog-Detail": {q.Cursor}}
					}
					return nil
				}
			},
		},
		{
			name:  "authorized continuation lacks the retained response witness",
			phase: "issuer resumes cursor", want: ErrIncomplete,
			mutate: func(target *observedTarget) {
				target.query = func(q QueryRequest, p *Page) error {
					if q.Selector == "fixture-c" && q.Cursor != "" && p.Status == http.StatusOK {
						p.Body = []byte("opaque")
					}
					return nil
				}
			},
		},
		{
			name:  "filtered query exposes a row only in raw bytes",
			phase: "outside the expected page", want: ErrViolation,
			mutate: func(target *observedTarget) {
				target.query = func(q QueryRequest, p *Page) error {
					if q.Principal == bob && q.Contains == escaped {
						p.Body = []byte(ndjson("2026-01-01T00:00:04Z", "retained-bob-a "+replacement))
					}
					return nil
				}
			},
		},
		{
			name:  "unused key distinguishes an unauthorized target after replay",
			phase: "reopened query and retry: ingest denied", want: ErrViolation,
			mutate: func(target *observedTarget) {
				reopened := false
				target.replay = func(*Committed) { reopened = true }
				target.ingest = func(r IngestRequest, response *Response) {
					if reopened && r.Target == (Target{bob, "fixture-c"}) && r.Key == unusedKey {
						response.Status = http.StatusConflict
					}
				}
			},
		},
		{
			name:  "changed retry echoes redaction input through an error header",
			phase: "ingest denied", want: ErrViolation,
			mutate: func(target *observedTarget) {
				target.ingest = func(r IngestRequest, response *Response) {
					if r.Target == (Target{alice, unknown}) && strings.Contains(r.Body, uncommitted) {
						response.Headers = map[string][]string{"Bog-Detail": {escaped}}
					}
				}
			},
		},
		{
			name:  "download is incorrectly bound to the artifact creator",
			phase: "authorized export download", want: ErrViolation,
			mutate: func(target *observedTarget) {
				creators := make(map[string]Target)
				target.create = func(q Target, start *ExportStart) {
					if start.ArtifactID != "" {
						creators[start.ArtifactID] = q
					}
				}
				target.fetch = func(q Target, id string, a *Artifact) {
					if q != creators[id] && a.Status == http.StatusOK {
						a.Response = forbiddenResponse()
					}
				}
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			target := &observedTarget{syntheticTarget: newSynthetic()}
			tc.mutate(target)
			assertFailure(t, target, tc.want, tc.phase)
		})
	}
}

func TestExportIDReuse(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name          string
		second        Target
		wantViolation bool
	}{
		{"same scope", Target{exporter, "fixture-a"}, false},
		{"different scope", Target{alice, "fixture-c"}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			target := &observedTarget{syntheticTarget: newSynthetic()}
			var firstID string
			target.create = func(q Target, start *ExportStart) {
				if start.ArtifactID == "" {
					return
				}
				switch q {
				case Target{alice, "fixture-a"}:
					firstID = start.ArtifactID
				case tc.second:
					delete(target.artifacts, start.ArtifactID)
					start.ArtifactID = firstID
				}
			}
			if tc.wantViolation {
				assertFailure(t, target, ErrViolation, "artifact ID reused across streams")
				return
			}
			if err := runFixture(t, Adapters{target, target, target, target}); err != nil {
				t.Fatal(err)
			}
		})
	}
}
