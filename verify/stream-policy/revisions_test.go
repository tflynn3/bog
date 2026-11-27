package streampolicy

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"
)

func runRevisions(t *testing.T, adapters RevisionAdapters) error {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return RunPolicyRevisions(ctx, adapters)
}

func TestPolicyRevisionScenario(t *testing.T) {
	t.Parallel()
	for range 8 {
		t.Run("independent target", func(t *testing.T) {
			t.Parallel()
			target := newSynthetic()
			if err := runRevisions(t, target.revisionAdapters()); err != nil {
				t.Fatal(err)
			}
			if len(target.frames) != 5 || len(target.artifacts) != 2 {
				t.Fatalf("got %d durable commits and %d published artifacts, want 5 and 2", len(target.frames), len(target.artifacts))
			}
			if got := target.frames[0].Outcome.Headers["Bog-Disposition"][0]; got != "synthetic-commit-0001" {
				t.Fatal("original committed disposition changed")
			}
			if got := target.frames[4].Rows[0].Line; got != postChangeRetained+" "+newReplacement {
				t.Fatal("new append did not use the revised policy")
			}
		})
	}
}

func TestMaskingRevisionCursorWitnesses(t *testing.T) {
	t.Parallel()
	const original, changed, restored = "fixture-masking-v1", "fixture-masking-v2", "fixture-masking-v3"
	for _, tc := range []struct {
		name, phase     string
		current, issued string
		afterRevocation bool
		reissue         string
		want            error
	}{
		{name: "single-use cursors"},
		{name: "accepts original after change", current: changed, issued: original,
			phase: "old masking revision must invalidate", want: ErrViolation},
		{name: "accepts original after regrant", current: changed, issued: original, afterRevocation: true,
			phase: "regranted query must still reject old masking revision", want: ErrViolation},
		{name: "revives original after restore", current: restored, issued: original,
			phase: "restored masking rules must retain a new revision (original cursor)", want: ErrViolation},
		{name: "accepts changed cursor after restore", current: restored, issued: changed,
			phase: "restored masking rules must retain a new revision (changed cursor)", want: ErrViolation},
		{name: "reissues a masking witness", reissue: "masking",
			phase: "masking scenario requires distinct cursors", want: ErrIncomplete},
		{name: "reissues a consumed grant witness", reissue: "grant",
			phase: "masking scenario requires distinct cursors", want: ErrIncomplete},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			faults := &revisionFaults{}
			adapters := faults.adapters()
			target := faults.syntheticTarget
			base := newScenario(adapters.Adapters)
			s := revisionScenario{scenario: base, model: newPolicyModel(base.fixture), policy: adapters.Policy}
			if err := s.update(ctx, nil); err != nil {
				t.Fatal(err)
			}
			if err := s.checkIngest(ctx); err != nil {
				t.Fatal(err)
			}
			owner := Target{alice, s.fixture.Streams[0].Selector}
			var first string
			if tc.reissue == "grant" {
				faults.query = func(request QueryRequest, page *Page) error {
					if request.Target == owner && page.NextCursor != "" {
						first = page.NextCursor
					}
					if request.Target == owner && request.Cursor == first && page.Status == http.StatusOK {
						delete(target.cursors, request.Cursor)
					}
					return nil
				}
			}
			if err := s.checkGrantRevisions(ctx); err != nil {
				t.Fatal(err)
			}

			var sawRevocation bool
			presented := make(map[string]bool)
			faults.query = func(request QueryRequest, page *Page) error {
				if tc.reissue != "" && page.NextCursor != "" {
					if first == "" {
						first = page.NextCursor
					} else {
						page.NextCursor = first
						page.Headers["Bog-Next-Cursor"] = []string{first}
					}
				}
				if request.Cursor == "" {
					return nil
				}
				// The same modified-token control can be presented again for
				// authorization precedence. Issued witnesses are single-use.
				if presented[request.Cursor] {
					t.Error("masking scenario presented an issued cursor twice")
				}
				held, issued := target.cursors[request.Cursor]
				if !issued {
					return nil
				}
				presented[request.Cursor] = true
				defer delete(target.cursors, request.Cursor)
				if request.Target != owner {
					return nil
				}
				sawRevocation = sawRevocation || page.Status == http.StatusForbidden
				current := target.streamPolicy(s.fixture.Streams[0]).MaskingVersion
				if page.Status == http.StatusBadRequest && current == tc.current && held.maskingVersion == tc.issued &&
					(!tc.afterRevocation || sawRevocation) {
					held.maskingVersion = current
					target.cursors[request.Cursor] = held
					var err error
					*page, err = target.Query(ctx, request)
					return err
				}
				return nil
			}
			err := s.checkMaskingRevisions(ctx)
			if tc.want == nil {
				if err != nil {
					t.Fatal(err)
				}
			} else if !errors.Is(err, tc.want) || !strings.Contains(err.Error(), tc.phase) {
				t.Fatalf("got %v, want %v at %q", err, tc.want, tc.phase)
			}
		})
	}
}

type policyFunc func(context.Context, PolicyRevision) (PolicyAck, error)

func (f policyFunc) ApplyPolicy(ctx context.Context, revision PolicyRevision) (PolicyAck, error) {
	return f(ctx, revision)
}

type publicationFunc func(context.Context, Target, func(context.Context) error) (ExportPublication, error)

func (f publicationFunc) ObserveExportPublication(ctx context.Context, target Target, barrier func(context.Context) error) (ExportPublication, error) {
	return f(ctx, target, barrier)
}

type recoveryFunc func(context.Context) (RetryRecovery, error)

func (f recoveryFunc) ObserveRetryRecovery(ctx context.Context) (RetryRecovery, error) {
	return f(ctx)
}

func TestPolicyRevisionEvidenceFailures(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, phase string
		want        error
		configure   func(*revisionFaults)
	}{
		{"missing acknowledgment", "initial policy acknowledgment", ErrIncomplete, func(f *revisionFaults) {
			f.ack = func(_ PolicyRevision, ack *PolicyAck) { ack.Complete = false }
		}},
		{"stale acknowledgment", "query grants between pages", ErrViolation, func(f *revisionFaults) {
			f.ack = func(revision PolicyRevision, ack *PolicyAck) {
				if revision.Number == 2 {
					ack.Number--
				}
			}
		}},
		{"ignored grant expansion", "expanded grant must preserve cursor scope", ErrViolation, func(f *revisionFaults) {
			f.policy = func(revision *PolicyRevision) {
				if revision.Number == 2 {
					for i := range revision.Grants {
						if revision.Grants[i].Target == (Target{bob, "fixture-b"}) {
							revision.Grants[i].Query = false
						}
					}
				}
			}
		}},
		{"cursor validated before revoked grant", "revoked query must deny", ErrViolation, func(f *revisionFaults) {
			f.query = func(request QueryRequest, page *Page) error {
				if request.Principal == alice && request.Cursor != "" && page.Status == http.StatusForbidden {
					page.Response = response(http.StatusBadRequest, "invalid cursor\n")
				}
				return nil
			}
		}},
		{"revoked query leaks cursor in header", "revoked query must deny", ErrViolation, func(f *revisionFaults) {
			f.query = func(request QueryRequest, page *Page) error {
				if request.Principal == alice && request.Cursor != "" && page.Status == http.StatusForbidden {
					page.Headers = map[string][]string{"Bog-Debug": {"/cursor/" + request.Cursor}}
				}
				return nil
			}
		}},
		{"scope leak in raw query bytes", "expanded scope first page", ErrViolation, func(f *revisionFaults) {
			f.query = func(request QueryRequest, page *Page) error {
				if request.Target == (Target{bob, "fixture-b"}) && page.Status == http.StatusOK {
					page.Body = append(page.Body, ndjson("2026-01-01T00:00:04Z", "retained-bob-a "+replacement)...)
				}
				return nil
			}
		}},
		{"unparsed extra row in raw query bytes", "expanded scope first page", ErrViolation, func(f *revisionFaults) {
			f.query = func(request QueryRequest, page *Page) error {
				if request.Target == (Target{bob, "fixture-b"}) && page.Status == http.StatusOK {
					page.Body = append(page.Body, ndjson("2026-01-01T00:00:04Z", "unexpected fixture row")...)
				}
				return nil
			}
		}},
		{"old masking revision accepted", "old masking revision must invalidate", ErrViolation, func(f *revisionFaults) {
			f.policy = func(revision *PolicyRevision) { revision.Streams[0].MaskingVersion = "unchanged" }
		}},
		{"regrant invalidates current masking cursor", "regranted query must resume current masking revision", ErrViolation, func(f *revisionFaults) {
			revoked := false
			f.policy = func(revision *PolicyRevision) {
				if revision.Streams[0].MaskingVersion != "fixture-masking-v2" {
					return
				}
				for _, grant := range revision.Grants {
					if grant.Target == (Target{alice, "fixture-a"}) {
						if !grant.Query {
							revoked = true
						} else if revoked {
							revision.Streams[0].MaskingVersion = "fixture-regrant-version"
						}
					}
				}
			}
		}},
		{"raw masking input leaked in header", "fresh query must match", ErrViolation, func(f *revisionFaults) {
			f.query = func(request QueryRequest, page *Page) error {
				if request.Contains == maskedLine && page.Status == http.StatusOK {
					page.Headers = map[string][]string{"Bog-Debug": {"/log/retained%2Dalice%2Da"}}
				}
				return nil
			}
		}},
		{"masked filter matches original text", "fresh query must match and page over masked rows", ErrViolation, func(f *revisionFaults) {
			f.query = func(request QueryRequest, page *Page) error {
				if request.Contains == maskedInput && page.Status == http.StatusOK {
					page.Rows = []Row{{"2026-01-01T00:00:01Z", maskedLine + " " + replacement}}
					page.Body = []byte(ndjson(page.Rows[0].Timestamp, page.Rows[0].Line))
				}
				return nil
			}
		}},
		{"restored query retains masking on the wire", "fresh query must match and page over restored rows", ErrViolation, func(f *revisionFaults) {
			f.query = func(request QueryRequest, page *Page) error {
				if request.Contains == maskedInput && page.Status == http.StatusOK && len(page.Rows) == 1 {
					page.Body = []byte(ndjson(page.Rows[0].Timestamp, maskedLine+" "+replacement))
				}
				return nil
			}
		}},
		{"masking revision reused after reset", "restored masking rules must retain a new revision", ErrViolation, func(f *revisionFaults) {
			f.policy = func(revision *PolicyRevision) {
				if revision.Streams[0].MaskingVersion == "fixture-masking-v3" {
					revision.Streams[0].MaskingVersion = "fixture-masking-v1"
				}
			}
		}},
		{"restoration invalidates unaffected cursor", "unaffected stream must resume after restoring rules", ErrViolation, func(f *revisionFaults) {
			f.policy = func(revision *PolicyRevision) {
				if revision.Streams[0].MaskingVersion == "fixture-masking-v3" {
					revision.Streams[1].MaskingVersion = "fixture-masking-v3"
				}
			}
		}},
		{"hidden artifact publication", "export revoked before publication", ErrViolation, func(f *revisionFaults) {
			f.publication = func(_ int, result *ExportPublication) {
				if result.Status == http.StatusForbidden {
					result.Published = append(result.Published, "private-artifact")
				}
			}
		}},
		{"unobserved publication", "revocation at publication barrier", ErrIncomplete, func(f *revisionFaults) {
			f.publication = func(number int, result *ExportPublication) { result.Complete = number == 1 }
		}},
		{"revoked download leaks authenticated manifest", "revoked retrieval must return uniform denial", ErrViolation, func(f *revisionFaults) {
			f.fetch = func(target Target, _ string, artifact *Artifact) {
				if target.Principal == exporter && artifact.Status == http.StatusForbidden {
					artifact.Manifest.Version = "fixture-v1"
				}
			}
		}},
		{"artifact leaks original escaped secret", "authorized scoped artifact", ErrViolation, func(f *revisionFaults) {
			f.fetch = func(_ Target, _ string, artifact *Artifact) {
				if artifact.Status == http.StatusOK {
					artifact.Bytes = append(artifact.Bytes, ndjson("2026-01-01T00:00:01Z", escaped)...)
				}
			}
		}},
		{"retry state synthesized from rows only", "initial restart", ErrIncomplete, func(f *revisionFaults) {
			f.recovery = func(result *RetryRecovery) { result.Complete = false }
		}},
		{"retry outcome split from append batch", "initial restart", ErrViolation, func(f *revisionFaults) {
			f.recovery = func(result *RetryRecovery) { result.Commits[0].Records = nil }
		}},
		{"recovered redaction revision overwritten", "initial restart", ErrViolation, func(f *revisionFaults) {
			f.recovery = func(result *RetryRecovery) { result.Commits[0].RedactionVersion = newRedactionVersion }
		}},
		{"authorized retry returns a new disposition", "authorized retry must retain", ErrViolation, func(f *revisionFaults) {
			f.ingest = func(request IngestRequest, result *Response) {
				if f.restarts > 0 && request.Principal == alice && request.Key == key && result.Status == http.StatusNoContent {
					result.Headers = map[string][]string{"Bog-Disposition": {"unexpected-disposition"}}
				}
			}
		}},
		{"revoked retry reveals original disposition", "revoked retry must deny", ErrViolation, func(f *revisionFaults) {
			f.ingest = func(request IngestRequest, result *Response) {
				if f.restarts > 0 && request.Principal == bob && request.Key == key && result.Status == http.StatusForbidden {
					result.Headers = map[string][]string{"Bog-Disposition": {"synthetic-commit-0004"}}
				}
			}
		}},
		{"retry adds second durable append", "restart after retry and fresh append", ErrViolation, func(f *revisionFaults) {
			f.replay = func(result *Committed) {
				if f.restarts == 2 {
					result.Records = append(result.Records, result.Records[0])
				}
			}
		}},
		{"new input uses stale redactor", "restart after retry and fresh append", ErrViolation, func(f *revisionFaults) {
			f.policy = func(revision *PolicyRevision) {
				for i := range revision.Streams[0].Redactions {
					revision.Streams[0].Redactions[i].Replacement = replacement
				}
			}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			faults := &revisionFaults{}
			tc.configure(faults)
			err := runRevisions(t, faults.adapters())
			if !errors.Is(err, tc.want) || !strings.Contains(err.Error(), tc.phase) {
				t.Fatalf("got %v, want %v at %q", err, tc.want, tc.phase)
			}
			if strings.Contains(err.Error(), escaped) || strings.Contains(err.Error(), key) {
				t.Fatal("diagnostic leaked fixture input")
			}
		})
	}
}

type revisionFaults struct {
	observedTarget
	policy       func(*PolicyRevision)
	ack          func(PolicyRevision, *PolicyAck)
	publication  func(int, *ExportPublication)
	recovery     func(*RetryRecovery)
	restarts     int
	publications int
}

func (f *revisionFaults) adapters() RevisionAdapters {
	f.syntheticTarget = newSynthetic()
	return RevisionAdapters{Adapters{f, f, f, f}, f, f, f}
}

func (f *revisionFaults) ApplyPolicy(ctx context.Context, revision PolicyRevision) (PolicyAck, error) {
	if f.policy != nil {
		f.policy(&revision)
	}
	ack, err := f.syntheticTarget.ApplyPolicy(ctx, revision)
	if err == nil && f.ack != nil {
		f.ack(revision, &ack)
	}
	return ack, err
}

func (f *revisionFaults) ObserveExportPublication(ctx context.Context, target Target, barrier func(context.Context) error) (ExportPublication, error) {
	f.publications++
	result, err := f.syntheticTarget.ObserveExportPublication(ctx, target, barrier)
	if err == nil && f.publication != nil {
		f.publication(f.publications, &result)
	}
	return result, err
}

func (f *revisionFaults) ObserveRetryRecovery(ctx context.Context) (RetryRecovery, error) {
	result, err := f.syntheticTarget.ObserveRetryRecovery(ctx)
	if err == nil && f.recovery != nil {
		f.recovery(&result)
	}
	return result, err
}

func (f *revisionFaults) RestartAndReplay(ctx context.Context) (Committed, error) {
	f.restarts++
	return f.observedTarget.RestartAndReplay(ctx)
}

func TestMissingPolicyRevisionCapabilities(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		drop func(*RevisionAdapters)
	}{
		{"policy", func(a *RevisionAdapters) { a.Policy = nil }},
		{"publication", func(a *RevisionAdapters) { a.Publication = nil }},
		{"recovery", func(a *RevisionAdapters) { a.Recovery = nil }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			target := newSynthetic()
			adapters := target.revisionAdapters()
			tc.drop(&adapters)
			if err := runRevisions(t, adapters); !errors.Is(err, ErrIncomplete) {
				t.Fatalf("got %v, want incomplete evidence", err)
			}
			if len(target.committed) != 0 {
				t.Fatal("missing capability modified target")
			}
		})
	}
}

func TestPublicationBarrierObservation(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		run  publicationFunc
	}{
		{"no callback", func(context.Context, Target, func(context.Context) error) (ExportPublication, error) {
			return ExportPublication{Complete: true}, nil
		}},
		{"duplicate callback", func(ctx context.Context, _ Target, barrier func(context.Context) error) (ExportPublication, error) {
			if err := barrier(ctx); err != nil {
				return ExportPublication{}, err
			}
			return ExportPublication{}, barrier(ctx)
		}},
		{"unsupported with sensitive error", func(context.Context, Target, func(context.Context) error) (ExportPublication, error) {
			return ExportPublication{}, errors.New(escaped)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			adapters := newSynthetic().revisionAdapters()
			adapters.Publication = tc.run
			err := runRevisions(t, adapters)
			if !errors.Is(err, ErrIncomplete) || strings.Contains(err.Error(), escaped) {
				t.Fatalf("got %v, want sanitized incomplete evidence", err)
			}
		})
	}
}

func TestPolicyCancellationAtPublication(t *testing.T) {
	t.Parallel()
	target := newSynthetic()
	adapters := target.revisionAdapters()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	adapters.Policy = policyFunc(func(ctx context.Context, revision PolicyRevision) (PolicyAck, error) {
		for _, grant := range revision.Grants {
			if grant.Principal == exporter && !grant.Export {
				cancel()
				return PolicyAck{}, ctx.Err()
			}
		}
		return target.ApplyPolicy(ctx, revision)
	})
	err := RunPolicyRevisions(ctx, adapters)
	if !errors.Is(err, ErrIncomplete) || !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v, want incomplete evidence and cancellation", err)
	}
	if len(target.artifacts) != 2 {
		t.Fatalf("canceled attempt published an artifact, inventory size %d", len(target.artifacts))
	}
	if bytes.Contains(target.committed, []byte(plain)) {
		t.Fatal("committed bytes contain raw fixture input")
	}
}
