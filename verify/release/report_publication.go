package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"syscall"
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
	Sync() error
	Close() error
}

type reportStagingFile interface {
	io.Writer
	reportDirectory
	Name() string
}

// Operations are scoped to a publisher, so tests can inject filesystem
// failures without sharing mutable hooks with another writer.
type reportPublisher struct {
	openDir    func(string) (reportDirectory, error)
	createTemp func(string, string) (reportStagingFile, error)
	rename     func(string, string) error
	remove     func(string) error
}

func localReportPublisher() reportPublisher {
	return reportPublisher{
		openDir: func(path string) (reportDirectory, error) {
			// Reject non-directories without opening a FIFO or device.
			return os.OpenFile(path, os.O_RDONLY|syscall.O_DIRECTORY, 0)
		},
		createTemp: func(dir, pattern string) (reportStagingFile, error) {
			return os.CreateTemp(dir, pattern)
		},
		rename: os.Rename,
		remove: os.Remove,
	}
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

	dirPath := filepath.Dir(path)
	dir, err := p.openDir(dirPath)
	if err != nil {
		return fmt.Errorf("open report directory: %w", err)
	}
	defer func() {
		if closeErr := dir.Close(); closeErr != nil {
			err = errors.Join(err, fmt.Errorf("close report directory: %w", closeErr))
		}
	}()

	staged, err := p.createTemp(dirPath, ".release-report-*")
	if err != nil {
		return fmt.Errorf("create staging report: %w", err)
	}
	stagedPath := staged.Name()
	defer func() {
		if state == reportUnpublished {
			if removeErr := p.remove(stagedPath); removeErr != nil && !errors.Is(removeErr, os.ErrNotExist) {
				err = errors.Join(err, fmt.Errorf("remove staging report %q: %w", stagedPath, removeErr))
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
	if err := p.rename(stagedPath, path); err != nil {
		return fmt.Errorf("rename staging report: %w", err)
	}
	state = reportDurabilityUncertain
	if err := dir.Sync(); err != nil {
		return fmt.Errorf("sync report directory: %w", err)
	}
	state = reportDurablyPublished
	return nil
}
