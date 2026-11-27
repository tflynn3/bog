package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"encoding/json/jsontext"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const testRevision = "0ab9ea87c20ebf0c585f384523059abb65cb79d8"

const proofJSON = `{"errno":"ENOSPC","disposable_local_filesystem":true,"observed_enospc":true,"recovered_after_space_freed":true}`

const healthyToFullJSON = `"healthy_to_full":{"readiness_status":200,"readiness_reason":"ready","ingest_status":204,"replayed_after_restart":true}`
const fullToHealthyJSON = `"full_to_healthy":{"readiness_status":503,"readiness_reason":"low_disk_headroom","ingest_status":503,"post_ingest_reason":"wal_rollback","post_ingest_category":"enospc"}`

func TestValidateReport(t *testing.T) {
	t.Parallel()
	id := identity{1, testRevision, strings.Repeat("a", 64), "test-fixture"}
	prefix := fmt.Sprintf(`{"schema_version":1,"revision":%q,"binary_sha256":%q,"fixture":%q,`, id.Revision, id.BinarySHA256, id.Fixture)
	caseJSON := `"enospc":{"status":"pass","evidence":` + proofJSON + `}`
	valid := prefix + `"cases":{"sigkill":{"status":"pass"},"recovery":{"status":"pass"},` + caseJSON + `}}`
	for _, tc := range []struct {
		name, from, to, problem string
	}{
		{"valid", "", "", ""},
		{"producer extensions", `"cases":{`, `"metrics":{"latencies":[0.1,42]},"cases":{"optional":{"status":"skip","detail":"unsupported"},`, ""},
		{"empty", valid, "", "malformed"},
		{"trailing value", valid, valid + `{}`, "malformed"},
		{"invalid utf8", "test-fixture", "\xff", "malformed"},
		{"malformed", valid, valid[:len(valid)-1], "malformed"},
		{"missing schema", `"schema_version":1,`, "", "schema_version"},
		{"schema type", `"schema_version":1`, `"schema_version":"1"`, "schema_version"},
		{"schema null", `"schema_version":1`, `"schema_version":null`, "schema_version"},
		{"missing revision", fmt.Sprintf(`"revision":%q,`, id.Revision), "", "revision"},
		{"missing digest", fmt.Sprintf(`"binary_sha256":%q,`, id.BinarySHA256), "", "binary_sha256"},
		{"missing fixture", fmt.Sprintf(`"fixture":%q,`, id.Fixture), "", "fixture"},
		{"schema mismatch", `"schema_version":1`, `"schema_version":2`, "identity mismatch"},
		{"revision mismatch", id.Revision, strings.Repeat("9", 40), "identity mismatch"},
		{"short revision", id.Revision, id.Revision[:7], "identity mismatch"},
		{"digest mismatch", id.BinarySHA256, strings.Repeat("b", 64), "identity mismatch"},
		{"uppercase digest", id.BinarySHA256, strings.ToUpper(id.BinarySHA256), "identity mismatch"},
		{"fixture mismatch", id.Fixture, "other-fixture", "identity mismatch"},
		{"duplicate identity", `"fixture":`, `"fixture":"wrong","fixture":`, "duplicate"},
		{"wrong spelling", `"fixture":`, `"Fixture":`, "fixture"},
		{"missing cases", valid, prefix[:len(prefix)-1] + "}", "cases"},
		{"null cases", valid, prefix + `"cases":null}`, "cases"},
		{"array cases", valid, prefix + `"cases":[]}`, "cases"},
		{"missing case", "," + caseJSON, "", "missing required case"},
		{"failed case", `"sigkill":{"status":"pass"}`, `"sigkill":{"status":"fail"}`, "expected pass"},
		{"skipped case", `"recovery":{"status":"pass"}`, `"recovery":{"status":"skip"}`, "expected pass"},
		{"invalid status", `"sigkill":{"status":"pass"}`, `"sigkill":{"status":"ok"}`, "unknown status"},
		{"missing status", `"sigkill":{"status":"pass"}`, `"sigkill":{}`, "status"},
		{"null status", `"sigkill":{"status":"pass"}`, `"sigkill":{"status":null}`, "status"},
		{"nonobject case", `"sigkill":{"status":"pass"}`, `"sigkill":"pass"`, "sigkill"},
		{"duplicate case", caseJSON, caseJSON + "," + caseJSON, "duplicate"},
		{"escaped duplicate case", caseJSON, caseJSON + "," + strings.Replace(caseJSON, "enospc", `enos\u0070c`, 1), "duplicate"},
		{"duplicate outcomes", `"status":"pass"`, `"status":"fail","status":"pass"`, "duplicate"},
		{"malformed detail", `"status":"pass"`, `"status":"pass","detail":false`, "detail"},
		{"missing evidence", caseJSON, `"enospc":{"status":"pass"}`, "kernel ENOSPC evidence"},
		{"null evidence", proofJSON, "null", "kernel ENOSPC evidence"},
		{"wrong errno", `"ENOSPC"`, `"EDQUOT"`, "require observed kernel ENOSPC"},
		{"numeric errno", `"ENOSPC"`, "28", "kernel ENOSPC evidence"},
		{"no observed ENOSPC", `"observed_enospc":true`, `"observed_enospc":false`, "require observed kernel ENOSPC"},
		{"no disposable filesystem", `"disposable_local_filesystem":true`, `"disposable_local_filesystem":false`, "require observed kernel ENOSPC"},
		{"no post-free recovery", `"recovered_after_space_freed":true`, `"recovered_after_space_freed":false`, "require observed kernel ENOSPC"},
		{"duplicate kernel evidence", `"errno":"ENOSPC"`, `"errno":"EDQUOT","errno":"ENOSPC"`, "duplicate"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			input := strings.Replace(valid, tc.from, tc.to, 1)
			err := validateReport([]byte(input), id, durabilityCases)
			if tc.problem == "" && err != nil || tc.problem != "" && (err == nil || !strings.Contains(err.Error(), tc.problem)) {
				t.Fatalf("wanted %q, got %v", tc.problem, err)
			}
		})
	}
}

func TestValidateRetargetEvidence(t *testing.T) {
	t.Parallel()
	id := identity{1, testRevision, strings.Repeat("a", 64), capacityFixtureID}
	modern := healthyToFullJSON + "," + fullToHealthyJSON
	for _, tc := range []struct {
		name, directions, problem string
		strict                    bool
	}{
		{"historical", "", "", false},
		{"native requires directions", "", "healthy_to_full", true},
		{"native success", modern, "", true},
		{"compatibility success", modern, "", false},
		{"healthy direction only", healthyToFullJSON, "full_to_healthy", false},
		{"full direction only", fullToHealthyJSON, "healthy_to_full", false},
		{"null healthy direction", `"healthy_to_full":null,` + fullToHealthyJSON, "healthy_to_full", true},
		{"wrong healthy readiness status", strings.Replace(modern, "200", "503", 1), "healthy_to_full", true},
		{"wrong healthy readiness reason", strings.Replace(modern, `"ready"`, `"low_disk_headroom"`, 1), "healthy_to_full", true},
		{"healthy ingest rejected", strings.Replace(modern, "204", "503", 1), "healthy_to_full", true},
		{"missing healthy replay", strings.Replace(modern, `,"replayed_after_restart":true`, "", 1), "replayed_after_restart", true},
		{"healthy replay failed", strings.Replace(modern, `"replayed_after_restart":true`, `"replayed_after_restart":false`, 1), "healthy_to_full", true},
		{"wrong full readiness status", strings.Replace(modern, "503", "200", 1), "full_to_healthy", true},
		{"wrong full readiness reason", strings.Replace(modern, `"low_disk_headroom"`, `"ready"`, 1), "full_to_healthy", true},
		{"full ingest acknowledged", strings.Replace(modern, `"ingest_status":503`, `"ingest_status":204`, 1), "full_to_healthy", true},
		{"missing rollback reason", strings.Replace(modern, `"post_ingest_reason":"wal_rollback",`, "", 1), "post_ingest_reason", true},
		{"wrong rollback reason", strings.Replace(modern, `"wal_rollback"`, `"low_disk_headroom"`, 1), "full_to_healthy", true},
		{"wrong rollback category", strings.Replace(modern, `"enospc"`, `"io"`, 1), "full_to_healthy", true},
		{"null rollback category", strings.Replace(modern, `"enospc"`, "null", 1), "post_ingest_category", true},
		{"duplicate direction", modern + "," + fullToHealthyJSON, "duplicate", true},
		{"duplicate readiness status", strings.Replace(modern, "200", `503,"readiness_status":200`, 1), "duplicate", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			proof := proofJSON
			if tc.directions != "" {
				proof = proof[:len(proof)-1] + "," + tc.directions + "}"
			}
			raw, err := json.Marshal(report{id, map[string]outcome{
				"sigkill": {Status: "pass"}, "recovery": {Status: "pass"},
				"enospc": {Status: "pass", Evidence: jsontext.Value(proof)},
			}})
			if err != nil {
				t.Fatal(err)
			}
			err = validateReportWithOptions(raw, id, durabilityCases, validationOptions{requireRetarget: tc.strict})
			if tc.problem == "" && err != nil || tc.problem != "" && (err == nil || !strings.Contains(err.Error(), tc.problem)) {
				t.Fatalf("wanted %q, got %v", tc.problem, err)
			}
		})
	}
}

func TestValidateCommand(t *testing.T) {
	t.Setenv("BOG_RELEASE_REQUIRE_RETARGET", "")
	t.Setenv("BOG_RELEASE_REQUIRE_CUTOVER", "")
	dir := t.TempDir()
	binary := filepath.Join(dir, "candidate")
	data := []byte("prebuilt candidate")
	writeFixture(t, binary, data)
	id := identity{1, testRevision, fmt.Sprintf("%x", sha256.Sum256(data)), capacityFixtureID}
	args := []string{"validate", "-binary", binary, "-revision", id.Revision, "-fixture", id.Fixture}
	for _, fixture := range []struct {
		name  string
		cases []string
	}{
		{"admission", []string{"one_slot_saturation", "ingest_during_saturation", "cursor_resume", "work_budget"}},
		{"durability", []string{"sigkill", "recovery", "enospc"}},
		{"capacity", []string{"retained_history_capacity"}},
	} {
		path := filepath.Join(dir, fixture.name+".json")
		cases := make(map[string]outcome)
		for _, name := range fixture.cases {
			cases[name] = outcome{Status: "pass"}
		}
		if fixture.name == "durability" {
			cases["enospc"] = outcome{Status: "pass", Evidence: jsontext.Value(proofJSON)}
		}
		raw, err := json.Marshal(report{id, cases})
		if err != nil {
			t.Fatal(err)
		}
		if fixture.name == "capacity" {
			raw = capacityReport(id)
		}
		writeFixture(t, path, raw)
		args = append(args, "-"+fixture.name, path)
	}
	if err := run(t.Context(), args); err != nil {
		t.Fatal(err)
	}
	t.Setenv("BOG_RELEASE_REQUIRE_RETARGET", "1")
	if err := run(t.Context(), args); err == nil || !strings.Contains(err.Error(), "durability report: case \"enospc\": healthy_to_full") {
		t.Fatalf("native validation accepted historical durability evidence: %v", err)
	}
	proof := proofJSON[:len(proofJSON)-1] + "," + healthyToFullJSON + "," + fullToHealthyJSON + "}"
	path := filepath.Join(dir, "durability.json")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(raw), proofJSON) {
		t.Fatalf("missing original proof: %s", raw)
	}
	writeFixture(t, path, []byte(strings.Replace(string(raw), proofJSON, proof, 1)))
	if err := run(t.Context(), args); err != nil {
		t.Fatal(err)
	}
	cutoverArgs := append(append([]string(nil), args...), "-require-cutover")
	if err := run(t.Context(), cutoverArgs); err == nil || !strings.Contains(err.Error(), "cutover diagnostics") {
		t.Fatalf("cutover flag accepted archived capacity report: %v", err)
	}
	t.Setenv("BOG_RELEASE_REQUIRE_CUTOVER", "1")
	if err := run(t.Context(), args); err == nil || !strings.Contains(err.Error(), "cutover diagnostics") {
		t.Fatalf("cutover environment accepted archived capacity report: %v", err)
	}
	produced := string(producerCutoverReport(t))
	produced = strings.NewReplacer(strings.Repeat("a", 40), id.Revision, strings.Repeat("b", 64), id.BinarySHA256).Replace(produced)
	writeFixture(t, filepath.Join(dir, "capacity.json"), []byte(produced))
	if err := run(t.Context(), args); err != nil {
		t.Fatalf("cutover rejected producer evidence: %v", err)
	}
	parityPath := filepath.Join(dir, "parity.json")
	parityArgs := append(append([]string(nil), args...), "-parity", parityPath)
	parity := report{id, map[string]outcome{"restored_parity": {Status: "pass"}}}
	if err := writeReport(parityPath, parity); err != nil {
		t.Fatal(err)
	}
	if err := run(t.Context(), append(parityArgs, "-require-cutover=false")); err != nil {
		t.Fatalf("historical restored parity rejected: %v", err)
	}
	if err := run(t.Context(), parityArgs); err == nil || !strings.Contains(err.Error(), ordinaryWALCase) {
		t.Fatalf("cutover accepted parity without ordinary WAL replay: %v", err)
	}
	parity.Cases[ordinaryWALCase] = outcome{Status: "pass"}
	if err := writeReport(parityPath, parity); err != nil {
		t.Fatal(err)
	}
	if err := run(t.Context(), parityArgs); err != nil {
		t.Fatalf("cutover rejected recovery parity: %v", err)
	}
	missing := filepath.Join(dir, "capacity.json")
	if err := os.Remove(missing); err != nil {
		t.Fatal(err)
	}
	if err := run(t.Context(), args); err == nil || !strings.Contains(err.Error(), "capacity report") {
		t.Fatalf("missing report accepted: %v", err)
	}
	writeFixture(t, binary, []byte("changed candidate"))
	if err := run(t.Context(), args); err == nil ||
		!strings.Contains(err.Error(), "admission report: report identity mismatch") ||
		!strings.Contains(err.Error(), "durability report: report identity mismatch") {
		t.Fatalf("candidate change accepted: %v", err)
	}
}

func TestAdmissionPublishesStartupFailure(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	binary, path := filepath.Join(dir, "candidate"), filepath.Join(dir, "admission.json")
	writeFixture(t, binary, []byte("not an executable"))
	args := []string{"-binary", binary, "-revision", testRevision, "-fixture", "test-fixture", "-report", path}
	if err := run(context.Background(), args); err == nil {
		t.Fatal("expected startup failure")
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var got report
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatal(err)
	}
	if got.Revision != testRevision || got.Fixture != "test-fixture" || got.SchemaVersion != 1 {
		t.Fatalf("wrong identity: %+v", got.identity)
	}
	for name, status := range map[string]string{
		"one_slot_saturation": "fail", "work_budget": "fail", saturationFixtureCase: "fail",
		"ingest_during_saturation": "skip", "cursor_resume": "skip",
	} {
		if got.Cases[name].Status != status || got.Cases[name].Detail == nil {
			t.Errorf("%s: wanted %s with detail, got %+v", name, status, got.Cases[name])
		}
	}
}

func TestAdmissionReportRetainsFixtureAndProbeFailures(t *testing.T) {
	t.Parallel()
	id := identity{1, testRevision, strings.Repeat("a", 64), capacityFixtureID}
	cases := make(map[string]outcome)
	for _, name := range admissionCases {
		cases[name] = outcome{Status: "pass"}
	}
	primary := errors.New("primary probe problem")
	combined := addFixtureCleanup(primary, "/fixture", func(string) error {
		return errors.New("cleanup problem")
	})
	err := record(cases, saturationFixtureCase, nil, combined)
	if !errors.Is(err, primary) {
		t.Fatalf("record lost original probe failure: %v", err)
	}
	skip := "prerequisite failed"
	cases["cursor_resume"] = outcome{Status: "skip", Detail: &skip}
	path := filepath.Join(t.TempDir(), "admission.json")
	if err := writeReport(path, report{id, cases}); err != nil {
		t.Fatal(err)
	}
	err = validateFile(path, id, admissionCases, validationOptions{})
	if err == nil || !strings.HasPrefix(err.Error(), `case "saturation_fixture": expected pass, got "fail": primary probe problem`) ||
		!strings.Contains(err.Error(), "cleanup problem") || !strings.Contains(err.Error(), skip) {
		t.Fatalf("incorrect validation diagnostic: %v", err)
	}
	if err := writeReport(path+"/admission.json", report{id, cases}); err == nil || !strings.Contains(err.Error(), "write release report") {
		t.Fatalf("report write failure was lost: %v", err)
	}
}

func writeFixture(t *testing.T, path string, data []byte) {
	t.Helper()
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
}

func TestBudgetEvidenceKeepsLegacyFields(t *testing.T) {
	t.Parallel()
	failure := &budgetProbeFailure{
		Budget: 4, Page: 2, Limit: 1, HTTPStatus: 503, Detail: "page 2: Bog HTTP 503",
	}
	stats := budgetWalkStats{
		FirstCursorBudget: 4, RejectedBudgets: 1, Pages: 1, Rows: 2,
		FirstFailure: failure, FirstBrokenContinuation: failure,
		Probes: []budgetProbe{
			{Budget: 4, Outcome: probeIssued, Pages: 1, Rows: 2, Failure: failure},
			{Budget: 2, Outcome: probeRejected},
		},
	}
	cases := make(map[string]outcome)
	if err := record(cases, "work_budget", stats, failure); err == nil {
		t.Fatal("record passed a broken continuation")
	}
	evidence := cases["work_budget"].Evidence
	var legacy struct {
		FirstCursorBudget int `json:"first_cursor_budget"`
		RejectedBudgets   int `json:"rejected_budgets"`
		Pages             int `json:"pages"`
		Rows              int `json:"rows"`
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(evidence, &legacy); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(evidence, &fields); err != nil {
		t.Fatal(err)
	}
	if values := [4]int{legacy.FirstCursorBudget, legacy.RejectedBudgets, legacy.Pages, legacy.Rows}; values != [4]int{4, 1, 1, 2} {
		t.Fatalf("legacy fields = %v", values)
	}
	if _, hasExact := fields["exact_threshold_budget"]; hasExact {
		t.Errorf("claimed exact threshold without a clean rejection at budget 3: %s", evidence)
	}
	for _, field := range []string{"first_failure", "first_broken_continuation"} {
		var got budgetProbeFailure
		if err := json.Unmarshal(fields[field], &got); err != nil || got != *failure {
			t.Errorf("%s = %+v, error = %v", field, got, err)
		}
	}
	var probes []budgetProbe
	if err := json.Unmarshal(fields["probes"], &probes); err != nil || len(probes) != 2 ||
		probes[0].Budget != 4 || probes[0].Outcome != probeIssued || probes[1].Budget != 2 || probes[1].Outcome != probeRejected {
		t.Errorf("probe evidence = %+v, error = %v", probes, err)
	}
}
