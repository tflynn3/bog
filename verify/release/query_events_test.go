package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestValidateQueryQualification(t *testing.T) {
	t.Parallel()
	id := identity{SchemaVersion: 1, Revision: testRevision, BinarySHA256: strings.Repeat("a", 64), Fixture: capacityFixtureID}
	modelEvents := passingQueryEvents(t, pinnedQueryTests, []string{pinnedQueryReplay, pinnedQuery104}, id)
	handlerEvents := passingQueryEvents(t, queryHandlerTests, nil, id)
	pinnedLimit := queryEventLine(t, "pass", "TestPinnedQueryModel104/retained/opening-limit-10", "")
	ordinary := queryEventLine(t, "pass", "TestPinnedQueryModelReplay/ordinary", "")
	packagePass := queryEventLine(t, "pass", "", "")
	lifecycle := queryEventLine(t, "pass", "TestQuery104CaptureLifecycle/legacy-retirement", "")
	identity104 := queryIdentityEvent(t, pinnedQuery104, id)
	for _, tc := range []struct {
		name     string
		model    string
		handlers string
		want     string
	}{
		{"pass", modelEvents, handlerEvents, ""},
		{"missing baseline replay", strings.Replace(modelEvents, ordinary, "", 1), handlerEvents, "TestPinnedQueryModelReplay/ordinary reported 0 passes"},
		{"missing #104 retained opening", strings.Replace(modelEvents, pinnedLimit, "", 1), handlerEvents, "TestPinnedQueryModel104/retained/opening-limit-10 reported 0 passes"},
		{"skipped pinned child despite passing parent", strings.Replace(modelEvents, pinnedLimit, strings.Replace(pinnedLimit, `"pass"`, `"skip"`, 1), 1), handlerEvents, "opening-limit-10 reported skip"},
		{"duplicate pinned child", modelEvents + pinnedLimit, handlerEvents, "opening-limit-10 reported 2 passes"},
		{"missing handler lifecycle", modelEvents, strings.Replace(handlerEvents, lifecycle, "", 1), "legacy-retirement reported 0 passes"},
		{"failed handler despite passing parent", modelEvents, handlerEvents + queryEventLine(t, "fail", "TestQuery104CaptureLifecycle/legacy-retirement", ""), "legacy-retirement reported failure"},
		{"wrong package", modelEvents, strings.Replace(handlerEvents, lifecycle, strings.Replace(lifecycle, queryEventPackage, "bog/other", 1), 1), "legacy-retirement reported 0 passes"},
		{"missing package completion", strings.Replace(modelEvents, packagePass, "", 1), handlerEvents, queryEventPackage + " reported 0 passes"},
		{"skipped unrelated test", modelEvents + queryEventLine(t, "skip", "TestOptional", ""), handlerEvents, ""},
		{"missing identity", strings.Replace(modelEvents, identity104, "", 1), handlerEvents, "TestPinnedQueryModel104 reported 0 candidate identities"},
		{"duplicate identity", modelEvents + identity104, handlerEvents, "TestPinnedQueryModel104 reported 2 candidate identities"},
		{"wrong revision", strings.Replace(modelEvents, id.Revision, strings.Repeat("b", 40), 1), handlerEvents, "TestPinnedQueryModelReplay candidate identity does not match"},
		{"wrong #104 digest", strings.Replace(modelEvents, identity104, strings.Replace(identity104, id.BinarySHA256, strings.Repeat("b", 64), 1), 1), handlerEvents, "TestPinnedQueryModel104 candidate identity does not match"},
		{"truncated JSON", modelEvents + `{"Action":`, handlerEvents, "decode Go test event"},
		{"oversize event", modelEvents + strings.Repeat(" ", 1<<16), handlerEvents, "read Go test events"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			model, handlers := filepath.Join(dir, "model.jsonl"), filepath.Join(dir, "handlers.jsonl")
			writeFixture(t, model, []byte(tc.model))
			writeFixture(t, handlers, []byte(tc.handlers))
			err := validateQueryQualification(t.Context(), model, handlers, id)
			if tc.want == "" {
				if err != nil {
					t.Fatal(err)
				}
			} else if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("validation = %v, want %q", err, tc.want)
			}
		})
	}
	t.Run("both evidence paths required", func(t *testing.T) {
		if err := validateQueryQualification(t.Context(), "model.jsonl", "", id); err == nil || !strings.Contains(err.Error(), "requires both") {
			t.Fatalf("missing handler path: %v", err)
		}
	})
	t.Run("both files missing", func(t *testing.T) {
		dir := t.TempDir()
		model, handlers := filepath.Join(dir, "model.jsonl"), filepath.Join(dir, "handlers.jsonl")
		err := validateQueryQualification(t.Context(), model, handlers, id)
		if !errors.Is(err, os.ErrNotExist) || !strings.Contains(err.Error(), model) || !strings.Contains(err.Error(), handlers) {
			t.Fatalf("missing events: %v", err)
		}
	})
}

func passingQueryEvents(t *testing.T, leaves, pinned []string, id identity) string {
	t.Helper()
	var stream strings.Builder
	for _, test := range pinned {
		stream.WriteString(queryIdentityEvent(t, test, id))
	}
	seen := make(map[string]bool)
	for _, leaf := range leaves {
		for test := leaf; test != ""; {
			if !seen[test] {
				stream.WriteString(queryEventLine(t, "pass", test, ""))
				seen[test] = true
			}
			separator := strings.LastIndexByte(test, '/')
			if separator == -1 {
				break
			}
			test = test[:separator]
		}
	}
	stream.WriteString(queryEventLine(t, "pass", "", ""))
	return stream.String()
}

func queryIdentityEvent(t *testing.T, test string, id identity) string {
	t.Helper()
	return queryEventLine(t, "output", test, fmt.Sprintf("    pinned_release_linux_test.go:80: candidate revision=%s binary_sha256=%s\n", id.Revision, id.BinarySHA256))
}

func queryEventLine(t *testing.T, action, test, output string) string {
	t.Helper()
	event, err := json.Marshal(queryTestEvent{Action: action, Package: queryEventPackage, Test: test, Output: output})
	if err != nil {
		t.Fatal(err)
	}
	return string(event) + "\n"
}

func TestQueryEventReadBounds(t *testing.T) {
	t.Parallel()
	id := identity{}
	pass := queryEventLine(t, "pass", "", "")
	irrelevant := queryEventLine(t, "output", "TestOptional", "diagnostic")
	t.Run("aggregate limit includes ignored events", func(t *testing.T) {
		stream := pass + strings.Repeat(irrelevant, 20)
		for _, excess := range []int{0, 1, len(irrelevant)} {
			// A reader without Stat, as when a file grows after open.
			limit := int64(len(stream) - excess)
			err := validateQueryEvents(t.Context(), strings.NewReader(stream), limit, nil, nil, id)
			if excess == 0 && err != nil {
				t.Fatalf("exact-size stream rejected: %v", err)
			}
			if excess != 0 && (!errors.Is(err, errArtifactOversized) || !strings.Contains(err.Error(), fmt.Sprint(limit))) {
				t.Fatalf("stream %d bytes over budget = %v", excess, err)
			}
		}
	})
	t.Run("line limit", func(t *testing.T) {
		const newline = "\n"
		line := strings.TrimSuffix(irrelevant, newline)
		exact := line + strings.Repeat(" ", maxQueryEventLineBytes-len(line))
		for _, tc := range []struct {
			name, wire string
			want       error
		}{
			{"exact with newline", exact + newline, nil},
			{"exact without newline", exact, nil},
			{"one over with newline", exact + " " + newline, errArtifactOversized},
			{"one over without newline", exact + " ", errArtifactOversized},
		} {
			t.Run(tc.name, func(t *testing.T) {
				err := validateQueryEvents(t.Context(), strings.NewReader(pass+tc.wire), maxQueryEventBytes, nil, nil, id)
				if !errors.Is(err, tc.want) || (err != nil && !strings.Contains(err.Error(), "line 2")) {
					t.Fatalf("validation = %v; want %v on line 2", err, tc.want)
				}
			})
		}
	})
	t.Run("canceled while collecting evidence", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		source := io.MultiReader(strings.NewReader(pass), artifactTestReader(func([]byte) (int, error) {
			cancel()
			return 0, io.EOF
		}))
		err := validateQueryEvents(ctx, source, maxQueryEventBytes, nil, nil, id)
		if !errors.Is(err, context.Canceled) || !strings.Contains(err.Error(), "read Go test events at line 2") {
			t.Fatalf("validation = %v; want cancellation after the passing event", err)
		}
	})
}

func TestMalformedQueryEvents(t *testing.T) {
	t.Parallel()
	pass := queryEventLine(t, "pass", "", "")
	for _, tc := range []struct {
		name, line, detail string
	}{
		{"syntax", `{"Action":`, "malformed JSON object"},
		{"blank", "", "malformed JSON object"},
		{"null root", "null", "expected JSON object"},
		{"array", "[]", "malformed JSON object"},
		{"missing action", `{"Package":"bog/verify/query-model"}`, "Action"},
		{"missing package", `{"Action":"pass"}`, "Package"},
		{"mis-cased package", `{"Action":"pass","package":"bog/verify/query-model"}`, "Package"},
		{"null test", `{"Action":"pass","Package":"bog/verify/query-model","Test":null}`, "Test"},
		{"nonstring test", `{"Action":"pass","Package":"bog/verify/query-model","Test":123}`, "Test"},
		{"null output", `{"Action":"output","Package":"bog/verify/query-model","Output":null}`, "Output"},
		{"repeated field", `{"Action":"fail","Action":"pass","Package":"bog/verify/query-model"}`, "malformed JSON object"},
		{"trailing value", `{"Action":"pass","Package":"bog/verify/query-model"}{}`, "malformed JSON object"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := validateQueryEvents(t.Context(), strings.NewReader(pass+tc.line+"\n"), maxQueryEventBytes, nil, nil, identity{})
			if !errors.Is(err, errArtifactMalformed) || !strings.Contains(err.Error(), "decode Go test event at line 2") ||
				!strings.Contains(err.Error(), tc.detail) {
				t.Fatalf("validation = %v; want malformed line 2 mentioning %q", err, tc.detail)
			}
		})
	}
	build := `{"Action":"build-output","ImportPath":"bog/unrelated","Output":"diagnostic"}` + "\n"
	other := `{"Action":"pass","Package":"bog/other","Extension":{"extra":true}}` + "\r\n"
	if err := validateQueryEvents(t.Context(), strings.NewReader(build+other+pass), maxQueryEventBytes, nil, nil, identity{}); err != nil {
		t.Fatalf("build diagnostics and irrelevant test events should be accepted: %v", err)
	}
}

func TestBothQueryEventFilesCheckInputs(t *testing.T) {
	t.Parallel()
	id := identity{Revision: testRevision, BinarySHA256: strings.Repeat("a", 64)}
	for _, input := range []struct {
		name  string
		model bool
	}{
		{"query model events", true},
		{"query handler events", false},
	} {
		t.Run(input.name, func(t *testing.T) {
			for _, tc := range []struct {
				name  string
				want  error
				phase string
			}{
				{"missing", os.ErrNotExist, "open"},
				{"FIFO", errArtifactNonregular, "open"},
				{"oversized", errArtifactOversized, "open"},
				{"malformed", errArtifactMalformed, "validate"},
				{"canceled", context.Canceled, "open"},
			} {
				t.Run(tc.name, func(t *testing.T) {
					dir := t.TempDir()
					model := filepath.Join(dir, "model.jsonl")
					handler := filepath.Join(dir, "handler.jsonl")
					path := handler
					if input.model {
						path = model
						writeFixture(t, handler, []byte(passingQueryEvents(t, queryHandlerTests, nil, id)))
					} else {
						writeFixture(t, model, []byte(passingQueryEvents(t, pinnedQueryTests, []string{pinnedQueryReplay, pinnedQuery104}, id)))
					}

					ctx, cancel := context.WithCancel(t.Context())
					defer cancel()
					check := func() error { return validateQueryQualification(ctx, model, handler, id) }
					var err error
					switch tc.name {
					case "FIFO":
						err = checkFIFO(t, path, check)
					case "oversized":
						writeFixture(t, path, nil)
						if err := os.Truncate(path, maxQueryEventBytes+1); err != nil {
							t.Fatal(err)
						}
						err = check()
					case "malformed":
						writeFixture(t, path, []byte("{}\n"))
						err = check()
					case "canceled":
						cancel()
						err = check()
					default:
						err = check()
					}
					prefix := fmt.Sprintf("%s: %s %q:", input.name, tc.phase, path)
					if !errors.Is(err, tc.want) || !strings.Contains(err.Error(), prefix) {
						t.Fatalf("validation = %v; want %v with %q", err, tc.want, prefix)
					}
				})
			}
		})
	}
}

func TestQueryQualificationSharesBudgetAcrossFiles(t *testing.T) {
	t.Parallel()

	id := identity{Revision: testRevision, BinarySHA256: strings.Repeat("a", 64)}
	model := passingQueryEvents(t, pinnedQueryTests, []string{pinnedQueryReplay, pinnedQuery104}, id)
	model += "{\"Action\":\"build-output\",\"ImportPath\":\"bog/other\",\"Output\":\"ignored\"}\r\n"
	handlers := passingQueryEvents(t, queryHandlerTests, nil, id)
	handlers += `{"Action":"output","Package":"bog/other","Output":"ignored"}` // No final newline.
	wireBytes := int64(len(model) + len(handlers))
	wireEvents := strings.Count(model, "\n") + strings.Count(handlers, "\n") + 1

	dir := t.TempDir()
	modelPath, handlerPath := filepath.Join(dir, "model.jsonl"), filepath.Join(dir, "handlers.jsonl")
	writeFixture(t, modelPath, []byte(model))
	writeFixture(t, handlerPath, []byte(handlers))
	for _, tc := range []struct {
		name        string
		bytes       int64
		events      int
		want        error
		diagnostics []string
	}{
		{"inclusive ceilings", wireBytes, wireEvents, nil, nil},
		{"combined byte overflow", wireBytes - 1, wireEvents, errArtifactOversized,
			[]string{fmt.Sprintf("query handler events: open %q", handlerPath), fmt.Sprint(wireBytes - 1)}},
		{"combined count overflow", wireBytes, wireEvents - 1, errQueryEventCount,
			[]string{fmt.Sprintf("query handler events: validate %q", handlerPath), fmt.Sprintf("line %d", strings.Count(handlers, "\n")+1)}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			budget := newQueryEventBudget(tc.bytes, tc.events)
			err := validateQueryQualificationWithBudget(t.Context(), modelPath, handlerPath, id, budget)
			if !errors.Is(err, tc.want) {
				t.Fatalf("validation = %v, want %v", err, tc.want)
			}
			for _, diagnostic := range tc.diagnostics {
				if !strings.Contains(err.Error(), diagnostic) {
					t.Fatalf("validation = %v, want %q", err, diagnostic)
				}
			}
			if err == nil && (budget.bytesLeft != 0 || budget.eventsLeft != 0) {
				t.Fatalf("exact input left budget %+v", budget)
			}
		})
	}
}

func TestQueryEventBudgetAfterInvalidInput(t *testing.T) {
	t.Parallel()

	failed := queryEventLine(t, "fail", "TestOptional", "")
	ignored := queryEventLine(t, "output", "TestOptional", "ignored")
	wire := failed + ignored + ignored
	budget := newQueryEventBudget(int64(len(wire)), 2)
	err := validateQueryEventsWithBudget(t.Context(), strings.NewReader(wire), budget, nil, nil, identity{})
	want := "TestOptional reported failure\nread Go test events at line 3: query event artifacts exceed event count limit (2 events)"
	if !errors.Is(err, errQueryEventCount) || err.Error() != want || budget.bytesLeft != 0 || budget.eventsLeft != 0 {
		t.Fatalf("validation = %v, budget %+v, want %q with exhausted budget", err, budget, want)
	}
}

func TestQueryEventByteBudgetAcrossReaders(t *testing.T) {
	t.Parallel()

	pass := queryEventLine(t, "pass", "", "")
	wire := pass + queryEventLine(t, "output", "TestOptional", "ignored")
	budget := newQueryEventBudget(int64(len(pass)+len(wire)-1), 10)
	if err := validateQueryEventsWithBudget(t.Context(), strings.NewReader(pass), budget, nil, nil, identity{}); err != nil {
		t.Fatal(err)
	}
	// The second reader exposes more bytes than were available to its
	// open-time check. Its unterminated token is the truncated second line.
	err := validateQueryEventsWithBudget(t.Context(), strings.NewReader(wire), budget, nil, nil, identity{})
	want := fmt.Sprintf("read Go test events at line 2: aggregate query event budget: artifact exceeds byte limit (%d bytes)", budget.byteLimit)
	if !errors.Is(err, errArtifactOversized) || err.Error() != want || !budget.byteOverflow || budget.bytesLeft != 0 {
		t.Fatalf("validation = %v, budget %+v, want %q", err, budget, want)
	}
}
