package main

import (
	"encoding/json"
	"encoding/json/jsontext"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
)

const drillFixturePoint = `{"generation":"abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789","covered_offset":128,"next_ordinal":4,"committed_end":256}`
const drillFixtureBefore = `{"status":"matched","counts":{"rows":4,"reference_pages":2,"server_pages":3}}`
const drillFixtureAfter = `{"status":"matched","counts":{"rows":4,"reference_pages":2,"server_pages":4}}`
const drillFixtureNotRun = `{"status":"not_run","counts":null}`
const drillFixtureSuccess = `{"schema_version":1,"parity":true,"exit_status":0,"recovery_point":` + drillFixturePoint +
	`,"before_restart":` + drillFixtureBefore + `,"after_restart":` + drillFixtureAfter + `,"diagnostic":null}`

func drillFixtureExpectations() restoreDrillExpectations {
	return restoreDrillExpectations{
		Point: restoreDrillPoint{
			Generation:    "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789",
			CoveredOffset: 128, NextOrdinal: 4, CommittedEnd: 256,
		},
		Before: restoreDrillCounts{Rows: 4, ReferencePages: 2, ServerPages: 3},
		After:  restoreDrillCounts{Rows: 4, ReferencePages: 2, ServerPages: 4},
	}
}

func TestRestoreDrillReportFixture(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	binary, path := filepath.Join(dir, "candidate"), filepath.Join(dir, "parity.json")
	writeFixture(t, binary, []byte("pinned candidate fixture"))
	id, err := readIdentity(t.Context(), binary, testRevision, capacityFixtureID)
	if err != nil {
		t.Fatal(err)
	}
	results := report{id, map[string]outcome{
		restoredParityCase: {Status: "pass", Evidence: jsontext.Value(restoredParityJSON)},
	}}
	required := []string{restoredParityCase}
	expected := drillFixtureExpectations()
	opts := validationOptions{requireCutover: true, restoreDrill: &expected}

	if err := writeReport(path, results); err != nil {
		t.Fatal(err)
	}
	if err := validateFile(t.Context(), path, id, required, validationOptions{requireCutover: true}); err != nil {
		t.Fatalf("historical parity: %v", err)
	}
	if err := validateFile(t.Context(), path, id, required, opts); err == nil ||
		!strings.HasPrefix(err.Error(), `missing required case "restore_drill"`) {
		t.Fatalf("validation = %v, want required case", err)
	}

	results.Cases[restoreDrillCase] = outcome{Status: "pass", Evidence: jsontext.Value(drillFixtureSuccess + "\n")}
	if err := writeReport(path, results); err != nil {
		t.Fatal(err)
	}
	if err := validateFile(t.Context(), path, id, required, opts); err != nil {
		t.Fatal(err)
	}
	if err := validateFile(t.Context(), path, id, required, validationOptions{}); err == nil ||
		!strings.Contains(err.Error(), "requires independent recovery point and phase counts") {
		t.Fatalf("validation without independent expectations = %v", err)
	}
	for _, tc := range []struct {
		name string
		edit func(*identity)
	}{
		{"revision", func(id *identity) { id.Revision = strings.Repeat("1", 40) }},
		{"binary", func(id *identity) { id.BinarySHA256 = strings.Repeat("2", 64) }},
		{"fixture", func(id *identity) { id.Fixture = "another-fixture" }},
		{"schema", func(id *identity) { id.SchemaVersion++ }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			other := id
			tc.edit(&other)
			err := validateFile(t.Context(), path, other, required, opts)
			if err == nil || !strings.HasPrefix(err.Error(), "report identity mismatch") {
				t.Fatalf("validation = %v, want report identity mismatch", err)
			}
		})
	}
}

func TestRestoreDrillSuccessEvidence(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, old, new, want string
	}{
		{"success", "", "", ""},
		{"producer extensions", `"diagnostic":null`, `"diagnostic":null,"new_field":true`, ""},
		{"wrong version", `"schema_version":1`, `"schema_version":2`, "schema_version=2"},
		{"missing parity", `"parity":true,`, "", `missing or null "parity"`},
		{"parity false", `"parity":true`, `"parity":false`, "parity=false"},
		{"exit error", `"exit_status":0`, `"exit_status":4`, "exit_status=4"},
		{"missing point", `"recovery_point":` + drillFixturePoint + ",", "", `missing "recovery_point"`},
		{"null point", drillFixturePoint, "null", "requires a recovery_point"},
		{"wrong generation", "abcdef01", "0bcdef01", "recovery_point mismatch"},
		{"uppercase generation", "abcdef01", "ABCDEF01", "lowercase hexadecimal"},
		{"wrong covered offset", `"covered_offset":128`, `"covered_offset":127`, "recovery_point mismatch"},
		{"wrong next ordinal", `"next_ordinal":4`, `"next_ordinal":5`, "recovery_point mismatch"},
		{"wrong committed end", `"committed_end":256`, `"committed_end":257`, "recovery_point mismatch"},
		{"inverted offsets", `"committed_end":256`, `"committed_end":127`, "invalid WAL bounds"},
		{"numeric string", `"next_ordinal":4`, `"next_ordinal":"4"`, "next_ordinal"},
		{"negative ordinal", `"next_ordinal":4`, `"next_ordinal":-1`, "next_ordinal"},
		{"before rows", `"rows":4`, `"rows":3`, "before_restart counts mismatch"},
		{"before reference pages", `"reference_pages":2`, `"reference_pages":3`, "before_restart counts mismatch"},
		{"after server pages", `"server_pages":4`, `"server_pages":3`, "after_restart counts mismatch"},
		{"phases swapped", drillFixtureBefore, drillFixtureAfter, "before_restart counts mismatch"},
		{"after not observed", drillFixtureAfter, drillFixtureNotRun, "phase order"},
		{"missing after", `,"after_restart":` + drillFixtureAfter, "", `missing or null "after_restart"`},
		{"missing counts", drillFixtureAfter, `{"status":"matched"}`, `missing "counts"`},
		{"null counts", drillFixtureAfter, `{"status":"matched","counts":null}`, "matched requires counts"},
		{"zero accepted pages", `"server_pages":4`, `"server_pages":0`, "at least one accepted page"},
		{"wrong field spelling", `"reference_pages"`, `"Reference_Pages"`, `missing or null "reference_pages"`},
		{"duplicate count", `"server_pages":4`, `"server_pages":2,"server_pages":4`, "duplicate"},
		{"null count field", `"server_pages":4`, `"server_pages":null`, `missing or null "server_pages"`},
		{"missing diagnostic", `,"diagnostic":null`, "", `missing "diagnostic"`},
		{"diagnostic with success", `"diagnostic":null`, `"diagnostic":{"phase":"after_restart","code":"mismatch","message":"row 1"}`, "diagnostic=null"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			raw := strings.Replace(drillFixtureSuccess, tc.old, tc.new, 1)
			err := validateDrillFixtureReport(t, "pass", raw, nil)
			if tc.want == "" {
				if err != nil {
					t.Fatal(err)
				}
			} else if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("validation = %v, want %q", err, tc.want)
			}
		})
	}
}

func TestRestoreDrillFailureEvidence(t *testing.T) {
	t.Parallel()
	const noCounts = `{"status":"failed","counts":null}`
	const partial = `{"status":"failed","counts":{"rows":1,"reference_pages":1,"server_pages":1}}`
	const fullCounts = `{"status":"failed","counts":{"rows":4,"reference_pages":2,"server_pages":4}}`
	const detail = "stop failed\nprocess group teardown failed"
	const message = "recorded drill problem"
	for _, tc := range []struct {
		name, point, before, after, phase, code string
		exit                                    int
		inconsistent                            string
	}{
		{"before verification failure", "null", noCounts, drillFixtureNotRun, drillBeforeRestart, "backup_verification_failed", 3, ""},
		{"before exhausted retries", drillFixturePoint, partial, drillFixtureNotRun, drillBeforeRestart, "retries_exhausted", 4, ""},
		{"before mismatch", drillFixturePoint, partial, drillFixtureNotRun, drillBeforeRestart, "mismatch", 5, ""},
		{"boundary cancellation", drillFixturePoint, drillFixtureBefore, drillFixtureNotRun, drillRestartBoundary, "canceled", 4, ""},
		{"after reference changed", drillFixturePoint, drillFixtureBefore, noCounts, drillAfterRestart, "backup_verification_failed", 3, ""},
		{"after exhausted budget with full rows", drillFixturePoint, drillFixtureBefore, fullCounts, drillAfterRestart, "page_budget_exhausted", 4, ""},
		{"after mismatch", drillFixturePoint, drillFixtureBefore, partial, drillAfterRestart, "mismatch", 5, ""},
		{"before reads after", drillFixturePoint, partial, drillFixtureAfter, drillBeforeRestart, "mismatch", 5, "phase order"},
		{"boundary claims mismatch", drillFixturePoint, drillFixtureBefore, drillFixtureNotRun, drillRestartBoundary, "mismatch", 5, "cannot occur at restart_boundary"},
		{"boundary with counts after", drillFixturePoint, drillFixtureBefore, strings.Replace(drillFixtureAfter, "matched", "not_run", 1), drillRestartBoundary, "canceled", 4, "not_run requires null counts"},
		{"boundary missing point", "null", drillFixtureBefore, drillFixtureNotRun, drillRestartBoundary, "canceled", 4, "requires the recovery_point"},
		{"after skipped", drillFixturePoint, drillFixtureBefore, drillFixtureNotRun, drillAfterRestart, "canceled", 4, "phase order"},
		{"after ran before matched", drillFixturePoint, partial, partial, drillAfterRestart, "mismatch", 5, "phase order"},
		{"verification failure with after counts", drillFixturePoint, drillFixtureBefore, partial, drillAfterRestart, "backup_verification_failed", 3, "requires null after_restart.counts"},
		{"mismatch without comparison", drillFixturePoint, drillFixtureBefore, noCounts, drillAfterRestart, "mismatch", 5, "mismatch requires counts"},
		{"inconsistent code", drillFixturePoint, drillFixtureBefore, partial, drillAfterRestart, "mismatch", 4, "inconsistent with exit_status=4"},
		{"unknown phase", drillFixturePoint, drillFixtureBefore, partial, "later", "deadline_exceeded", 4, "unknown restore drill diagnostic.phase"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			raw := fmt.Sprintf(`{"schema_version":1,"parity":false,"exit_status":%d,"recovery_point":%s,"before_restart":%s,"after_restart":%s,"diagnostic":{"phase":%q,"code":%q,"message":%q}}`,
				tc.exit, tc.point, tc.before, tc.after, tc.phase, tc.code, message)
			err := validateDrillFixtureReport(t, "fail", raw, new(detail))
			if err == nil {
				t.Fatal("failure evidence accepted")
			}
			header := `case "restore_drill": expected pass, got "fail": ` + detail
			recorded := fmt.Sprintf("restore drill diagnostic %s (%s, exit_status=%d): %s", tc.phase, tc.code, tc.exit, message)
			if tc.inconsistent == "" {
				if want := header + "\n" + recorded; err.Error() != want {
					t.Fatalf("validation = %v, want %q", err, want)
				}
			} else if !strings.HasPrefix(err.Error(), header+"\n") ||
				!strings.Contains(err.Error(), tc.inconsistent) || !strings.HasSuffix(err.Error(), recorded) {
				t.Fatalf("validation = %v, want recorded detail and %q", err, tc.inconsistent)
			}
		})
	}
	t.Run("passing drill cannot be wrapped as failed", func(t *testing.T) {
		err := validateDrillFixtureReport(t, "fail", drillFixtureSuccess, nil)
		if err == nil || !strings.Contains(err.Error(), "failed restore drill requires parity=false and a diagnostic") {
			t.Fatalf("validation = %v", err)
		}
	})
	t.Run("startup may fail without JSON output", func(t *testing.T) {
		err := validateDrillFixtureReport(t, "fail", "", new(detail))
		if want := `case "restore_drill": expected pass, got "fail": ` + detail; err == nil || err.Error() != want {
			t.Fatalf("validation = %v, want %q", err, want)
		}
	})
}

func validateDrillFixtureReport(t *testing.T, status, evidence string, detail *string) error {
	t.Helper()
	id := identity{1, testRevision, strings.Repeat("a", 64), capacityFixtureID}
	raw, err := json.Marshal(report{id, map[string]outcome{
		restoreDrillCase: {Status: status, Evidence: jsontext.Value(evidence), Detail: detail},
	}})
	if err != nil {
		t.Fatal(err)
	}
	expected := drillFixtureExpectations()
	return validateReportWithOptions(raw, id, nil, validationOptions{requireCutover: true, restoreDrill: &expected})
}
