package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"encoding/json/jsontext"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const testRevision = "0ab9ea87c20ebf0c585f384523059abb65cb79d8"

const proofJSON = `{"errno":"ENOSPC","disposable_local_filesystem":true,"observed_enospc":true,"recovered_after_space_freed":true}`

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

func TestValidateCommand(t *testing.T) {
	t.Parallel()
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
		"one_slot_saturation": "fail", "work_budget": "fail",
		"ingest_during_saturation": "skip", "cursor_resume": "skip",
	} {
		if got.Cases[name].Status != status || got.Cases[name].Detail == nil {
			t.Errorf("%s: wanted %s with detail, got %+v", name, status, got.Cases[name])
		}
	}
}

func writeFixture(t *testing.T, path string, data []byte) {
	t.Helper()
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
}
