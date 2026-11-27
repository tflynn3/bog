package streampolicy

import (
	"net/http"
	"testing"
)

func TestShortArtifactReference(t *testing.T) {
	t.Parallel()
	const id = "d"
	for _, tc := range []struct {
		name          string
		create, fetch func(*Response)
		phase         string
	}{
		{name: "uniform ordinary denial prose"},
		{
			name:   "uniform escaped denial prose",
			create: func(r *Response) { r.Body = []byte(`{"error":"d\u0065nied by policy"}`) },
			fetch:  func(r *Response) { r.Body = []byte(`100% denied {"error":"d%65nied by policy"}`) },
		},
		{
			name: "create leaks exact body", phase: "denied export create",
			create: func(r *Response) { r.Body = []byte(id) },
		},
		{
			name: "create leaks JSON escaped ID", phase: "denied export create",
			create: func(r *Response) { r.Body = []byte(`{"artifact_id":"\u0064"}`) },
		},
		{
			name: "download leaks a header reference", phase: "unexpected fixture marker",
			fetch: func(r *Response) { r.Headers = map[string][]string{"Bog-Artifact-ID": {id}} },
		},
		{
			name: "download leaks a URL path", phase: "unexpected fixture marker",
			fetch: func(r *Response) { r.Body = []byte("see /artifacts/d.") },
		},
		{
			name: "download leaks URI escaped ID", phase: "unexpected fixture marker",
			fetch: func(r *Response) {
				r.Headers = map[string][]string{"Location": {"download 100% denied at /artifacts/%64"}}
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			target := &observedTarget{syntheticTarget: newSynthetic()}
			const prose = "download denied by policy\n"
			target.create = func(q Target, start *ExportStart) {
				if start.Status == http.StatusForbidden {
					start.Body = []byte(prose)
					if tc.create != nil {
						tc.create(&start.Response)
					}
					return
				}
				if q == (Target{alice, "fixture-a"}) {
					target.artifacts[id] = target.artifacts[start.ArtifactID]
					delete(target.artifacts, start.ArtifactID)
					start.ArtifactID = id
				}
			}
			target.fetch = func(_ Target, _ string, a *Artifact) {
				if a.Status == http.StatusForbidden {
					a.Body = []byte(prose)
					if tc.fetch != nil {
						tc.fetch(&a.Response)
					}
				}
			}
			if tc.phase == "" {
				if err := runFixture(t, Adapters{target, target, target, target}); err != nil {
					t.Fatal(err)
				}
				return
			}
			// All denials on an affected endpoint carry the same response.
			// This requires disclosure scanning even on its first baseline.
			assertFailure(t, target, ErrViolation, tc.phase)
		})
	}
}

func TestReferenceBoundaries(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, text, id string
		want           bool
	}{
		{"in prose", "download denied", "d", false},
		{"in longer hyphenated ID", "/artifacts/d-closed", "d", false},
		{"in longer underscore ID", "/artifacts/d_closed", "d", false},
		{"in longer dotted ID", "/artifacts/d.closed", "d", false},
		{"in Unicode word", "vendré", "d", false},
		{"in Unicode identifier", "/artifacts/雪d", "d", false},
		{"exact", "d", "d", true},
		{"quoted", `artifact "d" is private`, "d", true},
		{"punctuated", "artifact d.", "d", true},
		{"link query", "GET /download?id=d&format=raw", "d", true},
		{"first occurrence embedded", "denied artifact d", "d", true},
		{"entire dotted reference", "/artifacts/d.v1?format=raw", "d.v1", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := hasReference([]byte(tc.text), []byte(tc.id)); got != tc.want {
				t.Errorf("hasReference(%q, %q) = %t, want %t", tc.text, tc.id, got, tc.want)
			}
		})
	}
}
