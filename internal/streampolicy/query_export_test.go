package streampolicy_test

import (
	"errors"
	"testing"

	"bog/internal/streampolicy"
)

func TestCursorScopeRevocationAndMaskTransitions(t *testing.T) {
	t.Parallel()
	var engine streampolicy.Engine
	rev := streampolicy.Revision{
		Number:  1,
		Streams: []streampolicy.StreamPolicy{{Scope: scopeA}, {Scope: scopeB}, {Scope: scopeC}},
		Grants: []streampolicy.Grant{
			{Scope: scopeA, Principal: "alice", Query: true, Ingest: true},
			{Scope: scopeA, Principal: "bob", Query: true},
			{Scope: scopeB, Principal: "alice", Query: true},
			{Scope: scopeC, Principal: "alice", Query: true},
		},
	}
	activate(t, &engine, rev)
	original := query(t, &engine, "alice", scopeA).Binding()
	other := query(t, &engine, "alice", scopeB).Binding()
	if original.MaskingVersion == 0 {
		t.Fatal("disabled masking requires an explicit version")
	}
	authorize := cursor(original)
	for _, tc := range []struct {
		name, principal string
		scope           streampolicy.Scope
		verify          func() (streampolicy.CursorBinding, error)
		want            error
	}{
		{"owner", "alice", scopeA, authorize, nil},
		{"same stream different principal", "bob", scopeA, authorize, streampolicy.ErrInvalidCursor},
		{"same tenant different stream", "alice", scopeB, authorize, streampolicy.ErrInvalidCursor},
		{"same stream ID different tenant", "alice", scopeC, authorize, streampolicy.ErrInvalidCursor},
		{"missing token", "alice", scopeA, nil, streampolicy.ErrInvalidCursor},
		{"unscoped token", "alice", scopeA, cursor(streampolicy.CursorBinding{}), streampolicy.ErrInvalidCursor},
		{"bad authentication", "alice", scopeA, func() (streampolicy.CursorBinding, error) {
			return original, errors.New("private token details")
		}, streampolicy.ErrInvalidCursor},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := engine.ResumeQuery(tc.principal, tc.scope, tc.verify)
			if err != tc.want {
				t.Fatalf("ResumeQuery = %v, want %v", err, tc.want)
			}
			if err != nil && got.Binding() != (streampolicy.CursorBinding{}) {
				t.Fatal("denied admission returned a binding")
			}
		})
	}
	rev.Number++
	rev.Grants[0].Query = false
	activate(t, &engine, rev)
	neverVerify := func() (streampolicy.CursorBinding, error) {
		t.Error("cursor authenticated before authorization")
		return original, nil
	}
	if _, err := engine.ResumeQuery("alice", scopeA, neverVerify); err != streampolicy.ErrDenied {
		t.Fatalf("revoked continuation error = %v", err)
	}
	if _, err := engine.AdmitIngest("alice", scopeA); err != nil {
		t.Fatalf("query revocation affected independent ingest: %v", err)
	}
	rev.Number++
	rev.Grants[0].Query = true
	activate(t, &engine, rev)
	if _, err := engine.ResumeQuery("alice", scopeA, authorize); err != nil {
		t.Fatalf("cursor failed after regrant: %v", err)
	}

	rev.Number++
	rules := []streampolicy.Replacement{{Find: "retained-alice-a", Replace: "part"}, {Find: "part", Replace: "[masked]"}}
	rev.Streams[0].Masks = rules
	activate(t, &engine, rev)
	masked := query(t, &engine, "alice", scopeA)
	for _, tc := range []struct{ input, want string }{
		{"retained-alice-a [redacted]", "[masked] [redacted]"},
		{"retained-alice-a/retained-alice-a", "[masked]/[masked]"},
		{"Retained-alice-a", "Retained-alice-a"},
	} {
		if got := masked.MaskLine(tc.input); got != tc.want {
			t.Errorf("MaskLine(%q) = %q, want %q", tc.input, got, tc.want)
		}
	}
	if _, err := engine.ResumeQuery("alice", scopeB, cursor(other)); err != nil {
		t.Fatalf("unchanged stream cursor failed: %v", err)
	}
	if _, err := engine.ResumeQuery("alice", scopeA, authorize); err != streampolicy.ErrInvalidCursor {
		t.Fatalf("old mask cursor error = %v", err)
	}

	rev.Number++
	rev.Streams[0].RedactionVersion = "ingest-only-change"
	activate(t, &engine, rev)
	if _, err := engine.ResumeQuery("alice", scopeA, cursor(masked.Binding())); err != nil {
		t.Fatalf("redaction change invalidated query cursor: %v", err)
	}

	rev.Number++
	rev.Streams[0].Masks = nil
	activate(t, &engine, rev)
	disabled := query(t, &engine, "alice", scopeA)
	if disabled.Binding().MaskingVersion == original.MaskingVersion || disabled.MaskLine("retained-alice-a") != "retained-alice-a" {
		t.Fatalf("disabling masking reused the original version or policy")
	}
	rev.Number++
	rev.Streams[0].Masks = rules
	activate(t, &engine, rev)
	if _, err := engine.ResumeQuery("alice", scopeA, cursor(masked.Binding())); err != streampolicy.ErrInvalidCursor {
		t.Fatalf("restored mask accepted stale cursor: %v", err)
	}
	if query(t, &engine, "alice", scopeA).Binding().MaskingVersion == disabled.Binding().MaskingVersion {
		t.Fatal("enabling masking reused the disabled version")
	}
}

func TestContinuationKeepsAdmissionWhilePolicyChanges(t *testing.T) {
	t.Parallel()
	var engine streampolicy.Engine
	rev := streampolicy.Revision{
		Number: 1,
		Streams: []streampolicy.StreamPolicy{{
			Scope: scopeA, Masks: []streampolicy.Replacement{{Find: "raw", Replace: "old"}},
		}},
		Grants: []streampolicy.Grant{{Scope: scopeA, Principal: "alice", Query: true}},
	}
	activate(t, &engine, rev)
	original := query(t, &engine, "alice", scopeA).Binding()
	page, err := engine.ResumeQuery("alice", scopeA, func() (streampolicy.CursorBinding, error) {
		rev.Number++
		rev.Grants[0].Query = false
		rev.Streams[0].Masks[0].Replace = "new"
		activate(t, &engine, rev)
		return original, nil
	})
	if err != nil || page.Revision() != 1 || page.Binding() != original || page.MaskLine("raw") != "old" {
		t.Fatalf("continuation did not retain admitted policy: %v", err)
	}
	if _, err := engine.ResumeQuery("alice", scopeA, cursor(original)); err != streampolicy.ErrDenied {
		t.Fatalf("new continuation after revocation error = %v", err)
	}
}

func TestExportPublicationAndDownload(t *testing.T) {
	t.Parallel()
	var engine, otherEngine streampolicy.Engine
	rev := streampolicy.Revision{
		Number:  1,
		Streams: []streampolicy.StreamPolicy{{Scope: scopeA}, {Scope: scopeB}},
		Grants: []streampolicy.Grant{
			{Scope: scopeA, Principal: "owner", Export: true},
			{Scope: scopeA, Principal: "peer", Export: true},
			{Scope: scopeB, Principal: "owner", Export: true},
			{Scope: scopeA, Principal: "query-only", Query: true},
		},
	}
	activate(t, &engine, rev)
	activate(t, &otherEngine, rev)
	pending, err := engine.AdmitExport("owner", scopeA)
	if err != nil || pending.Scope() != scopeA || pending.Principal() != "owner" || pending.Revision() != 1 {
		t.Fatalf("initial export admission = %v, %v", pending, err)
	}
	if err := engine.AuthorizeExportPublication(pending); err != nil {
		t.Fatalf("authorized publication error = %v", err)
	}
	if err := otherEngine.AuthorizeExportPublication(pending); err != streampolicy.ErrDenied {
		t.Fatalf("other engine publication error = %v", err)
	}
	if err := engine.AuthorizeExportPublication(streampolicy.ExportAdmission{}); err != streampolicy.ErrDenied {
		t.Fatalf("empty publication error = %v", err)
	}

	artifact := func() (streampolicy.Scope, error) { return scopeA, nil }
	for _, tc := range []struct {
		name, principal string
		scope           streampolicy.Scope
		lookup          func() (streampolicy.Scope, error)
		want            error
	}{
		{"same-scope peer", "peer", scopeA, artifact, nil},
		{"wrong scope", "owner", scopeB, artifact, streampolicy.ErrDenied},
		{"missing scope", "peer", scopeA, nil, streampolicy.ErrDenied},
		{"missing artifact", "peer", scopeA, func() (streampolicy.Scope, error) {
			return scopeA, errors.New("private artifact details")
		}, streampolicy.ErrDenied},
		{"missing grant", "query-only", scopeA, func() (streampolicy.Scope, error) {
			t.Error("artifact looked up before authorization")
			return scopeA, nil
		}, streampolicy.ErrDenied},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := engine.AuthorizeExportDownload(tc.principal, tc.scope, tc.lookup); got != tc.want {
				t.Fatalf("AuthorizeExportDownload = %v, want %v", got, tc.want)
			}
		})
	}

	rev.Number++
	rev.Grants[0].Export = false
	activate(t, &engine, rev)
	if err := engine.AuthorizeExportPublication(pending); err != streampolicy.ErrDenied {
		t.Fatalf("revoked publication error = %v", err)
	}
	for _, lookup := range []func() (streampolicy.Scope, error){artifact, nil} {
		if err := engine.AuthorizeExportDownload("owner", scopeA, lookup); err != streampolicy.ErrDenied {
			t.Fatalf("revoked download error = %v", err)
		}
	}
	if err := engine.AuthorizeExportDownload("peer", scopeA, artifact); err != nil {
		t.Fatalf("owner revocation affected peer download: %v", err)
	}
	if _, err := engine.AdmitExport("owner", scopeB); err != nil {
		t.Fatalf("revocation affected other scope: %v", err)
	}
}
