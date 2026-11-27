package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"

	"bog/verify/reportpublication"
)

func writeReport(path string, r report) error {
	data, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		return &reportpublication.Error{Path: path, State: reportpublication.Unpublished, Err: fmt.Errorf("encode report: %w", err)}
	}
	return reportpublication.WriteFile(path, append(data, '\n'), nil)
}

type proofDestination struct {
	path, name string
	file       *os.File
	dir        reportpublication.Directory
}

// publish owns the producer's copy of the inherited directory descriptor.
// Preserve a diagnostic proof on workload failure or cancellation.
func (p proofDestination) publish(r report) error {
	state, err := writeProof(p.dir, p.name, r)
	if closeErr := p.file.Close(); closeErr != nil {
		err = errors.Join(err, fmt.Errorf("close proof directory: %w", closeErr))
	}
	if err != nil {
		return &reportpublication.Error{Path: p.path, State: state, Mode: reportpublication.CreateOnly, Err: err}
	}
	return nil
}

func writeProof(dir reportpublication.Directory, name string, r report) (reportpublication.State, error) {
	data, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		return reportpublication.Unpublished, fmt.Errorf("encode report: %w", err)
	}
	return reportpublication.Publish(context.Background(), dir, name, append(data, '\n'), reportpublication.CreateOnly)
}
