package main

import (
	"encoding/json/jsontext"
	"strings"
	"testing"
)

const nativeENOSPCJSON = `{
	"errno":"ENOSPC","disposable_local_filesystem":true,"observed_enospc":true,"recovered_after_space_freed":true,
	"healthy_to_full":{"readiness_status":200,"readiness_reason":"ready","ingest_status":204,"replayed_after_restart":true},
	"full_to_healthy":{"readiness_status":503,"readiness_reason":"low_disk_headroom","ingest_status":503,"post_ingest_reason":"wal_rollback","post_ingest_category":"enospc"}
}`

const nativeBudgetJSON = `{
	"http_status":503,"next_cursor":false,"ndjson_rows":0,
	"fixed_budget_walk":{
		"first_cursor_budget":1,"exact_threshold_budget":1,"pages":3,"rows":3,"rejected_budgets":0,
		"probes":[{"budget":1,"outcome":"exhausted","pages":3,"rows":3}],
		"sparse_walk":{
			"first_cursor_budget":2,"exact_threshold_budget":2,"pages":3,"rows":4,"rejected_budgets":1,
			"response_byte_cap":96,"first_limit":4,"first_rows":2,"resume_limit":1,
			"probes":[{"budget":2,"outcome":"exhausted","pages":3,"rows":4},{"budget":1,"outcome":"clean_rejection","pages":0,"rows":0}]
		}
	}
}`

const nativeParityJSON = `{
	"verified_point":{"Generation":[1,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0],"CoveredOffset":16,"NextOrdinal":3,"CommittedEnd":32},
	"captured_rows":6,"post_restore_204":true,"post_append_rows":7,"held_rows_after_compaction":6,
	"post_compaction_rows":7,"restart_rows":7,"stale_cursor_rejected":true,"fixture_removed":true,
	"compaction":{"previous_generation":1,"generation":2,"covered_rows":7}
}`

func TestNativeEvidence(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, data, from, to, problem string
		validate                      func(jsontext.Value) error
	}{
		{"both ENOSPC directions", nativeENOSPCJSON, "", "", "", validateRetargets},
		{"missing healthy direction", nativeENOSPCJSON, "\"healthy_to_full\"", "\"extra\"", "healthy_to_full", validateRetargets},
		{"missing full direction", nativeENOSPCJSON, "\"full_to_healthy\"", "\"extra\"", "full_to_healthy", validateRetargets},
		{"healthy replay missing", nativeENOSPCJSON, "\"replayed_after_restart\":true", "\"replayed_after_restart\":false", "replay", validateRetargets},
		{"wrong rollback category", nativeENOSPCJSON, "\"post_ingest_category\":\"enospc\"", "\"post_ingest_category\":\"io\"", "kernel ENOSPC rollback", validateRetargets},
		{"both fixed-budget walks", nativeBudgetJSON, "", "", "", validateFixedBudget},
		{"no budget-1 proof", nativeBudgetJSON, "\"http_status\":503", "\"http_status\":200", "budget-1", validateFixedBudget},
		{"sparse limit unchanged", nativeBudgetJSON, "\"resume_limit\":1", "\"resume_limit\":4", "smaller resume limit", validateFixedBudget},
		{"walk stopped after cursor", nativeBudgetJSON, "\"outcome\":\"exhausted\"", "\"outcome\":\"issued_cursor\"", "did not exhaust", validateFixedBudget},
		{"missing adjacent rejection", nativeBudgetJSON, ",{\"budget\":1,\"outcome\":\"clean_rejection\",\"pages\":0,\"rows\":0}", "", "adjacent", validateFixedBudget},
		{"restored lifecycle", nativeParityJSON, "", "", "", validateParity},
		{"no verified tail", nativeParityJSON, "\"CommittedEnd\":32", "\"CommittedEnd\":16", "verified v1 restore point", validateParity},
		{"held walk leaked appended row", nativeParityJSON, "\"held_rows_after_compaction\":6", "\"held_rows_after_compaction\":7", "incomplete restored", validateParity},
		{"no generation change", nativeParityJSON, "\"generation\":2", "\"generation\":1", "replacement generation", validateParity},
		{"wrong compaction cutoff", nativeParityJSON, "\"covered_rows\":7", "\"covered_rows\":6", "compaction cutoff", validateParity},
		{"cleanup failed", nativeParityJSON, "\"fixture_removed\":true", "\"fixture_removed\":false", "fixture cleanup", validateParity},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := tc.validate(jsontext.Value(strings.Replace(tc.data, tc.from, tc.to, 1)))
			checkProblem(t, err, tc.problem)
		})
	}
}
