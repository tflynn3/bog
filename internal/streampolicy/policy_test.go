package streampolicy_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"bog/internal/streampolicy"
	fixturepolicy "bog/verify/stream-policy"
)

var (
	scopeA = streampolicy.Scope{TenantID: "tenant-one", StreamID: "stream-one"}
	scopeB = streampolicy.Scope{TenantID: "tenant-one", StreamID: "stream-two"}
	scopeC = streampolicy.Scope{TenantID: "tenant-two", StreamID: "stream-one"}
)

func activate(t *testing.T, engine *streampolicy.Engine, revision streampolicy.Revision) {
	t.Helper()
	if err := engine.Activate(revision); err != nil {
		t.Fatal(err)
	}
}

func query(t *testing.T, engine *streampolicy.Engine, principal string, scope streampolicy.Scope) streampolicy.QueryAdmission {
	t.Helper()
	page, err := engine.AdmitQuery(principal, scope)
	if err != nil {
		t.Fatal(err)
	}
	return page
}

func cursor(binding streampolicy.CursorBinding) func() (streampolicy.CursorBinding, error) {
	return func() (streampolicy.CursorBinding, error) { return binding, nil }
}

func scopeFromFixture(scope fixturepolicy.Scope) streampolicy.Scope {
	return streampolicy.Scope{TenantID: scope.Tenant, StreamID: scope.Stream}
}

func fixtureRevision(t *testing.T) (fixturepolicy.Fixture, streampolicy.Revision) {
	t.Helper()
	fixture := fixturepolicy.ReferenceFixture()
	revision := streampolicy.Revision{Number: 1}
	scopes := make(map[string]streampolicy.Scope, len(fixture.Streams))
	rules := make([]streampolicy.Replacement, 0, len(fixture.Replacements))
	for _, rule := range fixture.Replacements {
		rules = append(rules, streampolicy.Replacement{Find: rule.Forbidden, Replace: rule.Replacement})
	}
	redactor, err := streampolicy.NewLiteralRedactor(rules)
	if err != nil {
		t.Fatal(err)
	}
	for _, stream := range fixture.Streams {
		scope := scopeFromFixture(stream.Scope)
		scopes[stream.Selector] = scope
		revision.Streams = append(revision.Streams, streampolicy.StreamPolicy{
			Scope: scope, RedactionVersion: stream.PolicyVersion, Redactor: redactor,
		})
	}
	for _, grant := range fixture.Grants {
		revision.Grants = append(revision.Grants, streampolicy.Grant{
			Principal: grant.Principal, Scope: scopes[grant.Selector],
			Ingest: grant.Ingest, Query: grant.Query, Export: grant.Export,
		})
	}
	return fixture, revision
}

func TestReferenceGrantMatrix(t *testing.T) {
	t.Parallel()
	f, rev := fixtureRevision(t)
	var engine streampolicy.Engine
	activate(t, &engine, rev)
	scopes := []streampolicy.Scope{{TenantID: "unknown", StreamID: "unknown"}, {}}
	for _, stream := range f.Streams {
		scopes = append(scopes, scopeFromFixture(stream.Scope))
	}
	principals := []string{"", "unknown"}
	seen := make(map[string]bool)
	for _, grant := range f.Grants {
		if !seen[grant.Principal] {
			principals = append(principals, grant.Principal)
			seen[grant.Principal] = true
		}
	}
	for _, principal := range principals {
		for _, scope := range scopes {
			t.Run(fmt.Sprintf("%s/%s/%s", principal, scope.TenantID, scope.StreamID), func(t *testing.T) {
				var want streampolicy.Grant
				for _, grant := range rev.Grants {
					if grant.Principal == principal && grant.Scope == scope {
						want = grant
					}
				}
				_, ingestErr := engine.AdmitIngest(principal, scope)
				_, queryErr := engine.AdmitQuery(principal, scope)
				_, exportErr := engine.AdmitExport(principal, scope)
				for _, check := range []struct {
					permission string
					allowed    bool
					err        error
				}{
					{"ingest", want.Ingest, ingestErr},
					{"query", want.Query, queryErr},
					{"export", want.Export, exportErr},
				} {
					var expected error
					if !check.allowed {
						expected = streampolicy.ErrDenied
					}
					if check.err != expected {
						t.Errorf("%s error = %v, want %v", check.permission, check.err, expected)
					}
				}
			})
		}
	}
}

func TestActivationValidationAndDefensiveCopies(t *testing.T) {
	t.Parallel()
	var engine streampolicy.Engine
	if engine.CurrentRevision() != 0 {
		t.Fatal("new engine has a revision")
	}
	if _, err := engine.AdmitQuery("alice", scopeA); err != streampolicy.ErrDenied {
		t.Fatalf("unconfigured query error = %v", err)
	}
	baseline := func(number uint64) streampolicy.Revision {
		return streampolicy.Revision{
			Number: number,
			Streams: []streampolicy.StreamPolicy{{
				Scope: scopeA, Masks: []streampolicy.Replacement{{Find: "secret", Replace: "[masked]"}},
			}},
			Grants: []streampolicy.Grant{{Scope: scopeA, Principal: "alice", Query: true}},
		}
	}
	rev := baseline(10)
	activate(t, &engine, rev)
	prior := query(t, &engine, "alice", scopeA)
	rev.Streams[0].Masks[0].Replace = "modified"
	rev.Streams[0] = streampolicy.StreamPolicy{}
	rev.Grants[0].Query = false
	rev.Number = 42
	if got := query(t, &engine, "alice", scopeA); got.Revision() != 10 || got.MaskLine("secret") != "[masked]" {
		t.Fatal("active snapshot aliases revision input")
	}
	for _, tc := range []struct {
		name   string
		change func(*streampolicy.Revision)
		want   error
	}{
		{"zero revision", func(r *streampolicy.Revision) { r.Number = 0 }, streampolicy.ErrInvalidRevision},
		{"older revision", func(r *streampolicy.Revision) { r.Number = 9 }, streampolicy.ErrStaleRevision},
		{"equal revision", func(r *streampolicy.Revision) { r.Number = 10 }, streampolicy.ErrStaleRevision},
		{"incomplete scope", func(r *streampolicy.Revision) { r.Streams[0].Scope.TenantID = "" }, streampolicy.ErrInvalidRevision},
		{"duplicate scope", func(r *streampolicy.Revision) { r.Streams = append(r.Streams, r.Streams[0]) }, streampolicy.ErrInvalidRevision},
		{"empty rule", func(r *streampolicy.Revision) { r.Streams[0].Masks[0].Find = "" }, streampolicy.ErrInvalidRevision},
		{"invalid UTF-8 rule", func(r *streampolicy.Revision) { r.Streams[0].Masks[0].Replace = "\xff" }, streampolicy.ErrInvalidRevision},
		{"no principal", func(r *streampolicy.Revision) { r.Grants[0].Principal = "" }, streampolicy.ErrInvalidRevision},
		{"unknown scope", func(r *streampolicy.Revision) { r.Grants[0].Scope = scopeC }, streampolicy.ErrInvalidRevision},
		{"duplicate grant", func(r *streampolicy.Revision) { r.Grants = append(r.Grants, r.Grants[0]) }, streampolicy.ErrInvalidRevision},
	} {
		t.Run(tc.name, func(t *testing.T) {
			next := baseline(11)
			tc.change(&next)
			if err := engine.Activate(next); !errors.Is(err, tc.want) {
				t.Fatalf("Activate = %v, want %v", err, tc.want)
			}
			if engine.CurrentRevision() != 10 {
				t.Fatal("failed activation replaced active revision")
			}
		})
	}
	activate(t, &engine, streampolicy.Revision{Number: 11})
	if _, err := engine.AdmitQuery("alice", scopeA); err != streampolicy.ErrDenied {
		t.Fatalf("removed scope query error = %v", err)
	}
	activate(t, &engine, baseline(12))
	if _, err := engine.ResumeQuery("alice", scopeA, cursor(prior.Binding())); err != streampolicy.ErrInvalidCursor {
		t.Fatalf("cursor from removed scope error = %v", err)
	}
	if got := prior.MaskLine("secret"); got != "[masked]" {
		t.Fatalf("already admitted page mask = %q", got)
	}
}

func TestConcurrentActivationAndAdmission(t *testing.T) {
	t.Parallel()
	const last = 300
	makeRevision := func(number uint64) streampolicy.Revision {
		marker := fmt.Sprint(number)
		principal := "even"
		if number%2 != 0 {
			principal = "odd"
		}
		return streampolicy.Revision{
			Number: number,
			Streams: []streampolicy.StreamPolicy{{
				Scope: scopeA, RedactionVersion: marker,
				Redactor: func(_ context.Context, _ string) (string, error) { return marker, nil },
				Masks:    []streampolicy.Replacement{{Find: "raw", Replace: marker}},
			}},
			Grants: []streampolicy.Grant{{Scope: scopeA, Principal: principal, Query: true, Ingest: true}},
		}
	}
	var engine streampolicy.Engine
	activate(t, &engine, makeRevision(1))
	start := make(chan struct{})
	var workers sync.WaitGroup
	for _, first := range []uint64{2, 3} {
		workers.Go(func() {
			<-start
			for number := first; number <= last; number += 2 {
				if err := engine.Activate(makeRevision(number)); err != nil && err != streampolicy.ErrStaleRevision {
					t.Errorf("unexpected activation error: %v", err)
					return
				}
			}
		})
	}
	for _, principal := range []string{"odd", "even", "odd", "even"} {
		workers.Go(func() {
			<-start
			wantOdd := principal == "odd"
			for i := 0; i < last; i++ {
				page, err := engine.AdmitQuery(principal, scopeA)
				switch err {
				case nil:
					rev := page.Revision()
					if (rev%2 != 0) != wantOdd || page.Binding().MaskingVersion != rev || page.MaskLine("raw") != fmt.Sprint(rev) {
						t.Errorf("incoherent page admission at revision %d", rev)
						return
					}
				case streampolicy.ErrDenied:
				default:
					t.Errorf("unexpected query error: %v", err)
					return
				}
				batch, err := engine.AdmitIngest(principal, scopeA)
				switch err {
				case nil:
					rev := batch.Revision()
					out, redactErr := batch.RedactBatch(t.Context(), []string{"raw"})
					if (rev%2 != 0) != wantOdd || batch.RedactionVersion() != fmt.Sprint(rev) ||
						redactErr != nil || len(out) != 1 || out[0] != batch.RedactionVersion() {
						t.Errorf("incoherent ingest admission at revision %d", rev)
						return
					}
				case streampolicy.ErrDenied:
				default:
					t.Errorf("unexpected ingest error: %v", err)
					return
				}
			}
		})
	}
	close(start)
	workers.Wait()
	if got := engine.CurrentRevision(); got != last {
		t.Fatalf("current revision = %d, want %d", got, last)
	}
	if _, err := engine.AdmitIngest("odd", scopeA); err != streampolicy.ErrDenied {
		t.Fatalf("revoked ingest after activation error = %v", err)
	}
}
