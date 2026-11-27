package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"slices"
	"syscall"

	"bog/verify/capacityevidence"
)

// The fixture generates the entire submitted row from its append ordinal.
// Keeping the acknowledged batch ends preserves both order and duplicate
// multiplicity without retaining a second copy of the fixture payload.
// Only the fixture's single append worker writes to the ledger. The runner
// reads it after the worker has returned or been joined.
type acknowledgedRows struct {
	batchEnds []int
}

func (a *acknowledgedRows) rows() int {
	if len(a.batchEnds) == 0 {
		return 0
	}
	return a.batchEnds[len(a.batchEnds)-1]
}

func (a *acknowledgedRows) acknowledge(lo, hi int) error {
	if lo != a.rows() || hi <= lo {
		return fmt.Errorf("acknowledged batch [%d,%d) does not extend the ledger at ordinal %d", lo, hi, a.rows())
	}
	a.batchEnds = append(a.batchEnds, hi)
	return nil
}

type directoryIdentity struct {
	Device uint64 `json:"device"`
	Inode  uint64 `json:"inode"`
}

func identifyDirectory(info os.FileInfo) (*directoryIdentity, error) {
	if !info.IsDir() {
		return nil, errors.New("fixture path is not a directory")
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return nil, errors.New("fixture directory has no device or inode observation")
	}
	return &directoryIdentity{Device: uint64(stat.Dev), Inode: uint64(stat.Ino)}, nil
}

type restartEvidence struct {
	Status         string             `json:"status"`
	Phase          string             `json:"phase,omitempty"`
	Failure        string             `json:"failure,omitempty"`
	Original       *directoryIdentity `json:"original_directory,omitempty"`
	BeforeRestart  *directoryIdentity `json:"before_restart_directory,omitempty"`
	Restarted      *directoryIdentity `json:"restarted_directory,omitempty"`
	AfterWalk      *directoryIdentity `json:"after_walk_directory,omitempty"`
	FirstReaped    bool               `json:"first_candidate_reaped"`
	SubtreeDrained bool               `json:"subtree_drained_before_restart"`
	Publication    *compaction        `json:"publication,omitempty"`
	Recovered      *compaction        `json:"recovered_checkpoint,omitempty"`
	Ledger         restartLedgerProof `json:"ledger"`
}

type restartLedgerProof struct {
	Status        string `json:"status"`
	Acknowledged  int    `json:"acknowledged_rows"`
	Batches       int    `json:"acknowledged_batches"`
	SuffixRows    int    `json:"expected_wal_suffix_rows"`
	Matched       int    `json:"matched_rows"`
	MatchedSuffix int    `json:"matched_wal_suffix_rows"`
	Pages         int    `json:"complete_pages"`
	Exhausted     bool   `json:"exhausted"`
	Failure       string `json:"failure,omitempty"`
}

func newRestartEvidence() *restartEvidence {
	return &restartEvidence{Status: "not_started", Ledger: restartLedgerProof{Status: "not_checked"}}
}

func (e *restartEvidence) finish(err error) {
	if err != nil {
		e.Status, e.Failure = "fail", diagnosticError(err)
	} else {
		e.Status, e.Phase = "pass", "complete"
	}
}

// Keep the original directory open across both lifetimes so its inode
// cannot be recycled if the path is unlinked and recreated.
type restartTrial struct {
	dir      string
	handle   *os.File
	original os.FileInfo
	evidence *restartEvidence
	ledger   acknowledgedRows
}

func pinRestartTrial(dir string, e *restartEvidence) (_ *restartTrial, err error) {
	info, err := os.Lstat(dir)
	if err != nil {
		return nil, err
	}
	id, err := identifyDirectory(info)
	if err != nil {
		return nil, err
	}
	handle, err := os.OpenFile(dir, os.O_RDONLY|syscall.O_NONBLOCK, 0)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			err = errors.Join(err, handle.Close())
		}
	}()
	pinned, err := handle.Stat()
	if err != nil {
		return nil, err
	}
	if !os.SameFile(info, pinned) {
		return nil, errors.New("fixture directory changed while opening it")
	}
	e.Original = id
	return &restartTrial{dir: dir, handle: handle, original: pinned, evidence: e}, nil
}

func (t *restartTrial) compareDirectory(info os.FileInfo) (*directoryIdentity, error) {
	got, err := identifyDirectory(info)
	if err != nil {
		return nil, err
	}
	if !os.SameFile(t.original, info) {
		want := t.evidence.Original
		return got, fmt.Errorf("fixture directory changed: device/inode %d/%d, want %d/%d",
			got.Device, got.Inode, want.Device, want.Inode)
	}
	return got, nil
}

func (t *restartTrial) checkPath() (*directoryIdentity, error) {
	info, err := os.Lstat(t.dir)
	if err != nil {
		return nil, err
	}
	return t.compareDirectory(info)
}

func (t *restartTrial) prepare(f fixture, published *compaction) error {
	e := t.evidence
	e.Phase = "publication_and_ledger"
	proof := &e.Ledger
	proof.Acknowledged, proof.Batches = t.ledger.rows(), len(t.ledger.batchEnds)
	if published == nil {
		return errors.New("restart requires an observed replacement publication")
	}
	e.Publication = new(*published)
	if err := published.validatePublication(f.SeedRows+f.BatchRows, 0); err != nil {
		return err
	}
	if proof.Acknowledged != f.SeedRows+f.TailRows || published.CommittedRows > proof.Acknowledged {
		return fmt.Errorf("restart ledger has %d acknowledged rows for a fixture of %d, publication observed %d committed",
			proof.Acknowledged, f.SeedRows+f.TailRows, published.CommittedRows)
	}
	if _, ok := slices.BinarySearch(t.ledger.batchEnds, published.CoveredRows); !ok || published.CoveredRows >= proof.Acknowledged {
		return errors.New("replacement cutoff must end an acknowledged batch and leave a nonempty WAL suffix")
	}
	proof.SuffixRows = proof.Acknowledged - published.CoveredRows
	return nil
}

func (t *restartTrial) beforeStart(stopped cleanupProof, stopErr error) error {
	e := t.evidence
	e.FirstReaped, e.SubtreeDrained = stopped.CandidateReaped, stopped.SubtreeEmpty
	e.Phase = "first_shutdown"
	if stopErr != nil {
		return fmt.Errorf("stop first candidate before restart: %w", stopErr)
	}
	if !e.FirstReaped || !e.SubtreeDrained {
		return errors.New("restart requires a reaped candidate and an empty fixture cgroup")
	}
	e.Phase = "before_restart"
	var err error
	e.BeforeRestart, err = t.checkPath()
	return err
}

func checkpointObservation(c compaction) capacityevidence.CompactionObservation {
	return capacityevidence.CompactionObservation{
		State:         c.State,
		Generation:    c.Generation,
		CoveredRows:   c.CoveredRows,
		CommittedRows: c.CommittedRows,
	}
}

func (t *restartTrial) verify(ctx context.Context, s service, control string, f fixture) (err error) {
	e := t.evidence
	defer func() {
		got, checkErr := t.checkPath()
		e.AfterWalk = got
		if checkErr != nil {
			if err == nil {
				e.Phase = "after_restart_walk"
			}
			err = errors.Join(err, fmt.Errorf("after restarted workload: %w", checkErr))
		}
	}()

	// Query the already recovered checkpoint. A status read does not start
	// maintenance or write to the fixture.
	e.Phase = "recovered_checkpoint"
	current, statusErr := s.compact(ctx, control, "status", 0)
	if statusErr != nil {
		statusErr = fmt.Errorf("observe recovered checkpoint: %w", statusErr)
	} else {
		e.Recovered = &current.compaction
		recovered := checkpointObservation(*e.Recovered)
		statusErr = capacityevidence.ValidateRecoveredCheckpoint(
			checkpointObservation(*e.Publication), &recovered, e.Ledger.Acknowledged,
		)
	}
	if err := t.verifyLedger(ctx, s, f); err != nil {
		e.Phase = "restart_walk"
		return errors.Join(err, statusErr)
	}
	return statusErr
}

func (t *restartTrial) verifyLedger(ctx context.Context, s service, f fixture) error {
	e := t.evidence
	proof := &e.Ledger
	w := f.newWalk(t.ledger.rows(), false)
	w.phase, w.pageLimit = "restart_walk", 1000
	if len(w.want) != proof.Acknowledged {
		proof.Status, proof.Failure = "fail", "full restart query does not cover the acknowledged fixture"
		return errors.New(proof.Failure)
	}
	_, err := w.exhaust(ctx, s, f)
	proof.Matched, proof.Pages = w.offset, w.pages
	for _, ordinal := range w.want[:w.offset] {
		if ordinal >= e.Publication.CoveredRows {
			proof.MatchedSuffix++
		}
	}
	if err != nil {
		err = fmt.Errorf("restart walk matched %d/%d acknowledged rows including %d/%d WAL suffix rows: %w",
			proof.Matched, proof.Acknowledged, proof.MatchedSuffix, proof.SuffixRows, err)
		proof.Status, proof.Failure = "fail", diagnosticError(err)
		return err
	}
	proof.Status, proof.Exhausted = "pass", true
	return nil
}
