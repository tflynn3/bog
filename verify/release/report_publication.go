package main

import (
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

type reportPublicationState string

const (
	reportUnpublished         reportPublicationState = "replacement not published"
	reportDurabilityUncertain reportPublicationState = "replacement published, directory durability uncertain"
	reportDurablyPublished    reportPublicationState = "replacement published and synced"
)

// reportPublicationError describes this writer's own replacement. Another
// writer can replace it independently. A nil error means rename and both syncs
// succeeded, and all opened handles closed successfully.
type reportPublicationError struct {
	Path  string
	State reportPublicationState
	Err   error
}

func (e *reportPublicationError) Error() string {
	return fmt.Sprintf("write release report %q: %s: %v", e.Path, e.State, e.Err)
}

func (e *reportPublicationError) Unwrap() error { return e.Err }

type reportDirectory interface {
	OpenExclusive(name string) (reportStagingFile, error)
	Rename(from, to string) error
	Remove(name string) error
	Sync() error
	Close() error
}

type reportStagingFile interface {
	io.Writer
	Sync() error
	Close() error
}

// Operations are scoped to a publisher, so tests can inject filesystem
// failures without sharing mutable hooks with another writer.
type reportPublisher struct {
	openDir func(string) (reportDirectory, error)
}

func localReportPublisher() reportPublisher {
	return reportPublisher{openDir: openReportDirectory}
}

func createStagingReport(dir reportDirectory) (reportStagingFile, string, error) {
	for range 16 {
		name := ".release-report-" + rand.Text()
		file, err := dir.OpenExclusive(name)
		if !errors.Is(err, os.ErrExist) {
			return file, name, err
		}
	}
	return nil, "", errors.New("could not allocate a unique staging report")
}

func (p reportPublisher) writeReport(path string, results report) (err error) {
	state := reportUnpublished
	defer func() {
		if err != nil {
			err = &reportPublicationError{Path: path, State: state, Err: err}
		}
	}()

	data, err := json.MarshalIndent(results, "", "  ")
	if err != nil {
		return fmt.Errorf("encode release report: %w", err)
	}
	data = append(data, '\n')

	dirPath, name := filepath.Split(path)
	if name == "" || name == "." || name == ".." {
		return errors.New("report path must name a file")
	}
	if dirPath == "" {
		dirPath = "."
	}
	dir, err := p.openDir(dirPath)
	if err != nil {
		return fmt.Errorf("open report directory: %w", err)
	}
	defer func() {
		if closeErr := dir.Close(); closeErr != nil {
			err = errors.Join(err, fmt.Errorf("close report directory: %w", closeErr))
		}
	}()

	staged, stagedName, err := createStagingReport(dir)
	if err != nil {
		return fmt.Errorf("create staging report: %w", err)
	}
	defer func() {
		if state == reportUnpublished {
			if removeErr := dir.Remove(stagedName); removeErr != nil && !errors.Is(removeErr, os.ErrNotExist) {
				err = errors.Join(err, fmt.Errorf("remove staging report %q: %w", stagedName, removeErr))
			}
		}
	}()

	n, writeErr := staged.Write(data)
	if writeErr == nil && n != len(data) {
		writeErr = io.ErrShortWrite
	}
	if writeErr != nil {
		err = fmt.Errorf("write staging report (%d of %d bytes): %w", n, len(data), writeErr)
	} else if syncErr := staged.Sync(); syncErr != nil {
		err = fmt.Errorf("sync staging report: %w", syncErr)
	}
	if closeErr := staged.Close(); closeErr != nil {
		err = errors.Join(err, fmt.Errorf("close staging report: %w", closeErr))
	}
	if err != nil {
		return err
	}
	if err := dir.Rename(stagedName, name); err != nil {
		return fmt.Errorf("rename staging report: %w", err)
	}
	state = reportDurabilityUncertain
	if err := dir.Sync(); err != nil {
		return fmt.Errorf("sync report directory: %w", err)
	}
	state = reportDurablyPublished
	return nil
}
