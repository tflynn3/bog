package main

import (
	"encoding/json"
	"errors"
	"fmt"
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
			err := validateQueryQualification(model, handlers, id)
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
		if err := validateQueryQualification("model.jsonl", "", id); err == nil || !strings.Contains(err.Error(), "requires both") {
			t.Fatalf("missing handler path: %v", err)
		}
	})
	t.Run("both files missing", func(t *testing.T) {
		dir := t.TempDir()
		model, handlers := filepath.Join(dir, "model.jsonl"), filepath.Join(dir, "handlers.jsonl")
		err := validateQueryQualification(model, handlers, id)
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
