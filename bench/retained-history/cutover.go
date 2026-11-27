package main

import (
	"bog/verify/capacityevidence"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"syscall"
)

// The native gate runs this check from its own checkout. A report from an
// older candidate can pass the historical validator without cutover proof.
func validateCutoverFile(path string) (err error) {
	file, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NONBLOCK, 0)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, file.Close()) }()
	info, err := file.Stat()
	if err != nil {
		return err
	}
	const maxReport = 16 << 20
	if !info.Mode().IsRegular() || info.Size() > maxReport {
		return errors.New("capacity report must be a regular file of at most 16 MiB")
	}
	data, err := io.ReadAll(io.LimitReader(file, maxReport+1))
	if err != nil {
		return err
	}
	if len(data) > maxReport {
		return errors.New("capacity report exceeds 16 MiB")
	}
	return capacityevidence.ValidateReport(data)
}

func (r result) validateCutover() error {
	// Validate the same representation as the report file, before a scale
	// can be marked as a passing live proof.
	data, err := json.Marshal(r)
	if err != nil {
		return fmt.Errorf("encode capacity record: %w", err)
	}
	return capacityevidence.ValidateRecord(data)
}
