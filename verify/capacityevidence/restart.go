package capacityevidence

import (
	"errors"
	"fmt"
)

type restartEvidence struct {
	Status         string                `json:"status"`
	Phase          string                `json:"phase"`
	Failure        string                `json:"failure"`
	Original       *directoryIdentity    `json:"original_directory"`
	BeforeRestart  *directoryIdentity    `json:"before_restart_directory"`
	Restarted      *directoryIdentity    `json:"restarted_directory"`
	AfterWalk      *directoryIdentity    `json:"after_walk_directory"`
	FirstReaped    bool                  `json:"first_candidate_reaped"`
	SubtreeDrained bool                  `json:"subtree_drained_before_restart"`
	Publication    *compactionEvidence   `json:"publication"`
	Recovered      *compactionEvidence   `json:"recovered_checkpoint"`
	Ledger         restartLedgerEvidence `json:"ledger"`
}

type directoryIdentity struct {
	Device *uint64 `json:"device"`
	Inode  *uint64 `json:"inode"`
}

type restartLedgerEvidence struct {
	Status        string `json:"status"`
	Acknowledged  int    `json:"acknowledged_rows"`
	Batches       int    `json:"acknowledged_batches"`
	SuffixRows    int    `json:"expected_wal_suffix_rows"`
	Matched       int    `json:"matched_rows"`
	MatchedSuffix int    `json:"matched_wal_suffix_rows"`
	Pages         int    `json:"complete_pages"`
	Exhausted     bool   `json:"exhausted"`
	Failure       string `json:"failure"`
}

func (r *restartEvidence) validate(published compactionEvidence) error {
	if r == nil || r.Status != "pass" || r.Phase != "complete" || r.Failure != "" {
		return errors.New("require completed restart proof without failure")
	}
	for _, observation := range [...]struct {
		phase string
		dir   *directoryIdentity
	}{
		{"original", r.Original},
		{"before restart", r.BeforeRestart},
		{"restarted candidate", r.Restarted},
		{"after restart walk", r.AfterWalk},
	} {
		dir := observation.dir
		if dir == nil || dir.Device == nil || dir.Inode == nil {
			return fmt.Errorf("missing restart directory device or inode at %s", observation.phase)
		}
		if *dir.Device != *r.Original.Device || *dir.Inode != *r.Original.Inode {
			return fmt.Errorf("restart directory device or inode changed at %s", observation.phase)
		}
	}
	if !r.FirstReaped || !r.SubtreeDrained {
		return errors.New("require first candidate reaping and cgroup drain before restart")
	}
	if r.Publication == nil || *r.Publication != published {
		return errors.New("restart publication disagrees with the completed scattered compaction")
	}
	if r.Recovered == nil || *r.Recovered != published {
		return errors.New("recovered checkpoint disagrees with the completed scattered compaction")
	}
	ledger := r.Ledger
	total := published.CommittedRows
	suffix := total - published.CoveredRows
	if ledger.Status != "pass" || ledger.Failure != "" || !ledger.Exhausted || ledger.Pages <= 0 {
		return errors.New("require a passing, exhausted restart ledger with complete pages")
	}
	if ledger.Acknowledged != total || ledger.Matched != total || ledger.Batches != total/BatchRows {
		return fmt.Errorf("restart ledger must acknowledge and match exactly %d rows in %d acknowledged batches", total, total/BatchRows)
	}
	if suffix <= 0 || ledger.SuffixRows != suffix || ledger.MatchedSuffix != suffix {
		return fmt.Errorf("restart ledger must expect and match the nonempty WAL suffix of %d rows", suffix)
	}
	return nil
}
