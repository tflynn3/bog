package capacityevidence

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

type ReportPublicationState string

const (
	ReportUnpublished         ReportPublicationState = "replacement not published"
	ReportDurabilityUncertain ReportPublicationState = "replacement published, directory durability uncertain"
	ReportDurablyPublished    ReportPublicationState = "replacement published and synced"
)

// ReportPublicationError describes this writer's own replacement. Another
// writer can replace it independently. A nil error means rename and both syncs
// succeeded, and all opened handles closed successfully.
type ReportPublicationError struct {
	Path  string
	State ReportPublicationState
	Err   error
}

func (e *ReportPublicationError) Error() string {
	return fmt.Sprintf("write report %q: %s: %v", e.Path, e.State, e.Err)
}

func (e *ReportPublicationError) Unwrap() error { return e.Err }

type ReportDirectory interface {
	OpenExclusive(name string) (ReportStagingFile, error)
	Rename(from, to string) error
	Remove(name string) error
	Sync() error
	Close() error
}

type ReportStagingFile interface {
	io.Writer
	Sync() error
	Close() error
}

// ReportPublisher atomically replaces a JSON report in a pinned directory.
// The zero value uses the local filesystem. OpenDir can supply alternate
// directory operations for one publisher.
type ReportPublisher struct {
	OpenDir func(string) (ReportDirectory, error)
}

func createStagingReport(dir ReportDirectory) (ReportStagingFile, string, error) {
	for range 16 {
		name := ".release-report-" + rand.Text()
		file, err := dir.OpenExclusive(name)
		if !errors.Is(err, os.ErrExist) {
			return file, name, err
		}
	}
	return nil, "", errors.New("could not allocate a unique staging report")
}

// WriteReport syncs the staging file and parent directory around the replacement.
func (p ReportPublisher) WriteReport(path string, results any) (err error) {
	state := ReportUnpublished
	defer func() {
		if err != nil {
			err = &ReportPublicationError{Path: path, State: state, Err: err}
		}
	}()

	data, err := json.MarshalIndent(results, "", "  ")
	if err != nil {
		return fmt.Errorf("encode report: %w", err)
	}
	data = append(data, '\n')

	dirPath, name := filepath.Split(path)
	if name == "" || name == "." || name == ".." {
		return errors.New("report path must name a file")
	}
	if dirPath == "" {
		dirPath = "."
	}
	openDir := p.OpenDir
	if openDir == nil {
		openDir = OpenReportDirectory
	}
	dir, err := openDir(dirPath)
	if err != nil {
		return fmt.Errorf("open report directory: %w", err)
	}
	defer func() {
		if closeErr := dir.Close(); closeErr != nil {
			err = errors.Join(err, fmt.Errorf("close report directory: %w", closeErr))
		}
	}()

	state, err = replaceReport(context.Background(), dir, name, data)
	return err
}

// replaceReport borrows the pinned dir from its caller and owns any staging
// file it creates. name must be a validated basename. Cancellation observed
// before rename leaves the destination untouched. Once rename succeeds, the
// transaction attempts directory sync even if the context is canceled.
func replaceReport(ctx context.Context, dir ReportDirectory, name string, data []byte) (state ReportPublicationState, err error) {
	state = ReportUnpublished
	if err := ctx.Err(); err != nil {
		return state, err
	}
	staged, stagedName, err := createStagingReport(dir)
	if err != nil {
		return state, fmt.Errorf("create staging report: %w", err)
	}
	defer func() {
		if state == ReportUnpublished {
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
		return state, err
	}
	if err := ctx.Err(); err != nil {
		return state, err
	}
	if err := dir.Rename(stagedName, name); err != nil {
		return state, fmt.Errorf("rename staging report: %w", err)
	}
	state = ReportDurabilityUncertain
	if err := dir.Sync(); err != nil {
		return state, fmt.Errorf("sync report directory: %w", err)
	}
	return ReportDurablyPublished, nil
}
