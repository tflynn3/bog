package capacityevidence

import (
	json "encoding/json/v2"
	"fmt"
	"strings"
	"testing"
)

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
			{"running recovered checkpoint", "recovered_checkpoint.state", "running", false, "recovered checkpoint disagrees"},
			{"different recovered generation", "recovered_checkpoint.generation", 53, false, "recovered checkpoint disagrees"},
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
