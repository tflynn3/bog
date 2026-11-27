package main

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

type runnerDestinations struct {
	report, proof *reportDestination
}

// openRunnerDestinations resolves both output parents before any workload.
// An empty proof path indicates standalone preflight, whose report can replace
// an earlier preflight report.
func openRunnerDestinations(reportPath, proofPath string, executables ...string) (_ runnerDestinations, err error) {
	var d runnerDestinations
	d.report, err = openReportDestination(reportPath)
	if err != nil {
		return runnerDestinations{}, err
	}
	if proofPath == "" {
		return d, nil
	}
	defer func() {
		if err != nil {
			if d.proof != nil {
				err = errors.Join(err, d.proof.Close())
			}
			err = errors.Join(err, d.report.Close())
		}
	}()
	d.proof, err = openReportDestination(proofPath)
	if err != nil {
		return runnerDestinations{}, err
	}
	if d.report.sameEntry(d.proof.parent, d.proof.name) {
		return runnerDestinations{}, errors.New("runner report and capacity proof must have separate destinations")
	}
	for _, dest := range []*reportDestination{d.report, d.proof} {
		if err := dest.requireAbsent(); err != nil {
			return runnerDestinations{}, err
		}
	}
	for _, executable := range executables {
		parent, name := filepath.Split(executable)
		if name != d.report.name && name != d.proof.name {
			continue
		}
		info, err := os.Stat(parent)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return runnerDestinations{}, fmt.Errorf("inspect parent of executable %q: %w", executable, err)
		}
		if d.report.sameEntry(info, name) || d.proof.sameEntry(info, name) {
			return runnerDestinations{}, fmt.Errorf("evidence destination must be separate from executable %q", executable)
		}
	}
	return d, nil
}
