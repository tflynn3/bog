package capacityevidence

import (
	json "encoding/json/v2"
	"fmt"
	"strings"
	"testing"
)

func TestValidateRecoveredCheckpoint(t *testing.T) {
	t.Parallel()
	const acknowledged = 256
	published := CompactionObservation{State: "complete", Generation: 52, CoveredRows: 128, CommittedRows: 192}
	for _, tc := range []struct {
		name      string
		recovered *CompactionObservation
		want      string
	}{
		{"matching checkpoint with later acknowledged appends", &CompactionObservation{"complete", 52, 128, 256}, ""},
		{"missing observation", nil, "missing recovered checkpoint observation"},
		{"incomplete checkpoint with matching counters", &CompactionObservation{"running", 52, 128, 256}, `state "running", want complete`},
		{"new generation with identical rows and cutoff", &CompactionObservation{"complete", 53, 128, 256}, "generation 53, want published generation 52"},
		{"older generation with identical rows and cutoff", &CompactionObservation{"complete", 51, 128, 256}, "generation 51, want published generation 52"},
		{"different covered cutoff", &CompactionObservation{"complete", 52, 192, 256}, "covers 192 rows, want published cutoff 128"},
		{"committed count matches publication only", &CompactionObservation{"complete", 52, 128, 192}, "reports 192 committed rows, ledger has 256"},
		{"committed count exceeds the ledger", &CompactionObservation{"complete", 52, 128, 320}, "reports 320 committed rows, ledger has 256"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := ValidateRecoveredCheckpoint(published, tc.recovered, acknowledged)
			if tc.want == "" && err != nil || tc.want != "" && (err == nil || !strings.Contains(err.Error(), tc.want)) {
				t.Errorf("ValidateRecoveredCheckpoint error = %v, want %q", err, tc.want)
			}
		})
	}
}

func TestValidateRestartEvidence(t *testing.T) {
	t.Parallel()
	for _, seed := range [...]int{SmallScale, LargeScale} {
		original := observationRecord(t, seed)
		other := LargeScale
		if seed == LargeScale {
			other = SmallScale
		}
		companion := observationRecord(t, other)
		for _, tc := range []struct {
			name, field string
			value       any
			remove      bool
			want        string
		}{
			{name: "complete proof"},
			{"missing restart", "", nil, true, "completed restart proof"},
			{"null restart", "", nil, false, "completed restart proof"},
			{"failed restart", "status", "fail", false, "completed restart proof"},
			{"incomplete restart", "phase", "restart_walk", false, "completed restart proof"},
			{"contradictory restart failure", "failure", "walk failed", false, "completed restart proof"},
			{"missing original identity", "original_directory", nil, true, "missing restart directory device or inode at original"},
			{"missing device", "original_directory.device", nil, true, "missing restart directory device or inode at original"},
			{"null device", "before_restart_directory.device", nil, false, "missing restart directory device or inode at before restart"},
			{"different device", "before_restart_directory.device", 1, false, "restart directory device or inode changed at before restart"},
			{"missing inode", "restarted_directory.inode", nil, true, "missing restart directory device or inode at restarted candidate"},
			{"different inode", "restarted_directory.inode", 45, false, "restart directory device or inode changed at restarted candidate"},
			{"quoted inode", "restarted_directory.inode", "23", false, "/restarted_directory/inode"},
			{"missing after-walk identity", "after_walk_directory", nil, true, "missing restart directory device or inode at after restart walk"},
			{"different after-walk inode", "after_walk_directory.inode", 45, false, "restart directory device or inode changed at after restart walk"},
			{"missing first reap", "first_candidate_reaped", nil, true, "first candidate reaping and cgroup drain"},
			{"first subtree populated", "subtree_drained_before_restart", false, false, "first candidate reaping and cgroup drain"},
			{"missing publication", "publication", nil, true, "restart publication disagrees"},
			{"different publication generation", "publication.generation", 53, false, "restart publication disagrees"},
			{"different publication cutoff", "publication.covered_rows", seed, false, "restart publication disagrees"},
			{"publication behind final cutoff", "publication.committed_rows", seed + BatchRows, false, "restart publication disagrees"},
			{"missing recovered checkpoint", "recovered_checkpoint", nil, true, "recovered checkpoint disagrees"},
			{"null recovered checkpoint", "recovered_checkpoint", nil, false, "missing recovered checkpoint observation"},
			{"running recovered checkpoint", "recovered_checkpoint.state", "running", false, "recovered checkpoint disagrees"},
			{"different recovered generation with identical rows and cutoff", "recovered_checkpoint.generation", 53, false, "generation 53, want published generation 52"},
			{"different recovered cutoff", "recovered_checkpoint.covered_rows", seed, false, "recovered checkpoint disagrees"},
			{"recovered checkpoint without suffix", "recovered_checkpoint.committed_rows", seed + BatchRows, false, "recovered checkpoint disagrees"},
			{"missing ledger", "ledger", nil, true, "passing, exhausted restart ledger"},
			{"unchecked ledger", "ledger.status", "not_checked", false, "passing, exhausted restart ledger"},
			{"contradictory ledger failure", "ledger.failure", "page failed", false, "passing, exhausted restart ledger"},
			{"no complete pages", "ledger.complete_pages", 0, false, "passing, exhausted restart ledger"},
			{"no exhaustion observation", "ledger.exhausted", nil, true, "passing, exhausted restart ledger"},
			{"wrong acknowledged rows", "ledger.acknowledged_rows", seed, false, "restart ledger must acknowledge and match exactly"},
			{"wrong matched rows", "ledger.matched_rows", seed, false, "restart ledger must acknowledge and match exactly"},
			{"wrong acknowledged batches", "ledger.acknowledged_batches", 1, false, "restart ledger must acknowledge and match exactly"},
			{"missing expected suffix", "ledger.expected_wal_suffix_rows", nil, true, "expect and match the nonempty WAL suffix"},
			{"wrong expected suffix", "ledger.expected_wal_suffix_rows", TailRows, false, "expect and match the nonempty WAL suffix"},
			{"wrong matched suffix", "ledger.matched_wal_suffix_rows", TailRows - BatchRows - 1, false, "expect and match the nonempty WAL suffix"},
		} {
			t.Run(fmt.Sprintf("%d/%s", seed, tc.name), func(t *testing.T) {
				t.Parallel()
				data := original
				if tc.want != "" {
					type object = map[string]any
					var wire object
					if err := json.Unmarshal(original, &wire); err != nil {
						t.Fatal(err)
					}
					parent := wire["diagnostics"].(object)
					field := "restart"
					if tc.field != "" {
						parent = parent[field].(object)
						path := strings.Split(tc.field, ".")
						for _, key := range path[:len(path)-1] {
							parent = parent[key].(object)
						}
						field = path[len(path)-1]
					}
					if tc.remove {
						delete(parent, field)
					} else {
						parent[field] = tc.value
					}
					var err error
					data, err = json.Marshal(wire)
					if err != nil {
						t.Fatal(err)
					}
				}
				checkWireProof(t, data, companion, tc.want)
			})
		}
	}
}
