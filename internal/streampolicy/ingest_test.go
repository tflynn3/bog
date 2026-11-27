package streampolicy_test

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"testing"

	"bog/internal/streampolicy"
)

func TestBatchRedactionFixtureAndPolicyReplacement(t *testing.T) {
	t.Parallel()
	f, rev := fixtureRevision(t)
	var engine streampolicy.Engine
	activate(t, &engine, rev)
	grant := rev.Grants[0]
	old, err := engine.AdmitIngest(grant.Principal, grant.Scope)
	if err != nil {
		t.Fatal(err)
	}

	lines := []string{"retain " + f.Replacements[0].Forbidden, "keep " + f.Replacements[1].Forbidden, "duplicate", "duplicate"}
	input := slices.Clone(lines)
	want := []string{"retain " + f.Replacements[0].Replacement, "keep " + f.Replacements[1].Replacement, "duplicate", "duplicate"}
	raw, err := json.Marshal(lines)
	if err != nil {
		t.Fatal(err)
	}
	var decoded []string
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatal(err)
	}
	rev.Number++
	newRules := []streampolicy.Replacement{{Find: f.Replacements[0].Forbidden, Replace: "[new]"}}
	newRedactor, err := streampolicy.NewLiteralRedactor(newRules)
	if err != nil {
		t.Fatal(err)
	}
	newRules[0].Replace = "changed after compilation"
	rev.Streams[0].Redactor = newRedactor
	rev.Streams[0].RedactionVersion = "redaction-v2"
	activate(t, &engine, rev)
	fresh, err := engine.AdmitIngest(grant.Principal, grant.Scope)
	if err != nil {
		t.Fatal(err)
	}
	out, err := fresh.RedactBatch(t.Context(), input[:1])
	if err != nil || !slices.Equal(out, []string{"retain [new]"}) || fresh.RedactionVersion() != "redaction-v2" {
		t.Fatalf("fresh redaction = %q, %v", out, err)
	}

	rev.Number++
	rev.Grants[0].Ingest = false
	activate(t, &engine, rev)
	if _, err := engine.AdmitIngest(grant.Principal, grant.Scope); err != streampolicy.ErrDenied {
		t.Fatalf("revoked ingest or keyed retry error = %v", err)
	}
	if old.Principal() != grant.Principal || old.Scope() != grant.Scope ||
		old.RedactionVersion() != f.Streams[0].PolicyVersion || old.Revision() != 1 {
		t.Fatal("old batch lost admission metadata")
	}
	out, err = old.RedactBatch(t.Context(), decoded)
	if err != nil || !slices.Equal(out, want) || !slices.Equal(decoded, input) {
		t.Fatalf("original batch = %q, %v, input %q", out, err, decoded)
	}
	encoded, err := json.Marshal(out)
	if err != nil {
		t.Fatal(err)
	}
	for _, rule := range f.Replacements {
		quoted, err := json.Marshal(rule.Forbidden)
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(encoded), rule.Forbidden) || strings.Contains(string(encoded), string(quoted[1:len(quoted)-1])) {
			t.Fatal("serialized output included a forbidden value")
		}
	}
	out[0] = "changed"
	if !slices.Equal(decoded, input) {
		t.Fatal("output aliases the decoded input")
	}
}

func TestRedactionFailureIsAtomic(t *testing.T) {
	t.Parallel()
	const failure = "fixture-failed-redaction"
	for _, tc := range []struct {
		name string
		fail streampolicy.Redactor
	}{
		{"unavailable redactor", nil},
		{"error with partial result", func(_ context.Context, line string) (string, error) {
			return line, errors.New(line)
		}},
		{"invalid UTF-8", func(context.Context, string) (string, error) { return "\xff", nil }},
		{"panic", func(_ context.Context, line string) (string, error) { panic(line) }},
		{"nil panic", func(context.Context, string) (string, error) { panic(nil) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var engine streampolicy.Engine
			var redactor streampolicy.Redactor
			calls := 0
			if tc.fail != nil {
				redactor = func(ctx context.Context, line string) (string, error) {
					calls++
					if line == failure {
						return tc.fail(ctx, line)
					}
					return "[redacted]", nil
				}
			}
			activate(t, &engine, streampolicy.Revision{
				Number:  1,
				Streams: []streampolicy.StreamPolicy{{Scope: scopeA, RedactionVersion: "v1", Redactor: redactor}},
				Grants:  []streampolicy.Grant{{Principal: "alice", Scope: scopeA, Ingest: true}},
			})
			admission, err := engine.AdmitIngest("alice", scopeA)
			if err != nil {
				t.Fatal(err)
			}
			input := []string{"first", failure, "third"}
			out, err := admission.RedactBatch(t.Context(), input)
			if out != nil || err != streampolicy.ErrRedactionUnavailable {
				t.Fatalf("failed batch returned %q, %v", out, err)
			}
			if tc.fail != nil && calls != 2 {
				t.Fatalf("redactor called %d times, want 2", calls)
			}
			if !slices.Equal(input, []string{"first", failure, "third"}) {
				t.Fatal("failed batch mutated input")
			}
		})
	}
}

func TestRedactionCancellationAndExplicitIdentity(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	var engine streampolicy.Engine
	calls := 0
	revision := streampolicy.Revision{
		Number: 1,
		Streams: []streampolicy.StreamPolicy{{
			Scope: scopeA, RedactionVersion: "cancel-v1",
			Redactor: func(_ context.Context, line string) (string, error) {
				calls++
				cancel()
				return line, nil
			},
		}},
		Grants: []streampolicy.Grant{{Principal: "alice", Scope: scopeA, Ingest: true}},
	}
	activate(t, &engine, revision)
	batch, err := engine.AdmitIngest("alice", scopeA)
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if out, err := batch.RedactBatch(ctx, []string{"first", "second"}); out != nil || err != context.Canceled {
			t.Fatalf("canceled redaction = %q, %v", out, err)
		}
	}
	if calls != 1 {
		t.Fatalf("cancellation stopped redaction after %d calls, want 1", calls)
	}

	identity, err := streampolicy.NewLiteralRedactor(nil)
	if err != nil {
		t.Fatal(err)
	}
	revision.Number++
	revision.Streams[0].Redactor = identity
	revision.Streams[0].RedactionVersion = ""
	if err := engine.Activate(revision); !errors.Is(err, streampolicy.ErrInvalidRevision) {
		t.Fatalf("unversioned redactor activation error = %v", err)
	}
	if _, err := streampolicy.NewLiteralRedactor([]streampolicy.Replacement{{Replace: "x"}}); !errors.Is(err, streampolicy.ErrInvalidRevision) {
		t.Fatalf("invalid literal redactor error = %v", err)
	}
	revision.Streams[0].RedactionVersion = "identity-v1"
	activate(t, &engine, revision)
	batch, err = engine.AdmitIngest("alice", scopeA)
	if err != nil {
		t.Fatal(err)
	}
	input := []string{"Unicode 雪🦉", "", "JSON \" \\"}
	if out, err := batch.RedactBatch(t.Context(), input); err != nil || !slices.Equal(input, out) {
		t.Fatalf("identity redaction = %q, %v", out, err)
	}
}
