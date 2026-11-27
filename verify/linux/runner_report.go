package main

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

type reportDestination struct {
	path, name string
	root       *os.Root
	dir        *os.File
	parent     os.FileInfo
}

func splitReportPath(path string) (parent, name string, err error) {
	// Preserve the spelling of the parent. In link/../file, the kernel
	// resolves link before applying .., which filepath.Clean would erase.
	parent, name = filepath.Split(path)
	if name == "" || name == "." || name == ".." {
		return "", "", fmt.Errorf("report path %q must name a file", path)
	}
	if parent == "" {
		parent = "."
	}
	return parent, name, nil
}

func openReportDestination(path string) (_ *reportDestination, err error) {
	parent, name, err := splitReportPath(path)
	if err != nil {
		return nil, err
	}
	root, err := os.OpenRoot(parent)
	if err != nil {
		return nil, fmt.Errorf("open parent of report %q: %w", path, err)
	}
	d := &reportDestination{path: path, name: name, root: root}
	defer func() {
		if err != nil {
			err = errors.Join(err, d.Close())
		}
	}()
	d.dir, err = root.Open(".")
	if err != nil {
		return nil, fmt.Errorf("open sync handle for report %q: %w", path, err)
	}
	d.parent, err = d.dir.Stat()
	if err != nil {
		return nil, fmt.Errorf("stat parent of report %q: %w", path, err)
	}
	return d, nil
}

func (d *reportDestination) requireAbsent() error {
	_, err := d.root.Lstat(d.name)
	if err == nil {
		return fmt.Errorf("evidence destination already exists: %s", d.path)
	}
	if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("inspect report destination %q: %w", d.path, err)
	}
	return nil
}

func (d *reportDestination) sameEntry(parent os.FileInfo, name string) bool {
	return d.name == name && os.SameFile(d.parent, parent)
}

func (d *reportDestination) OpenExclusive(name string) (runnerStagingFile, error) {
	return d.root.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
}

func (d *reportDestination) Rename(from, to string) error { return d.root.Rename(from, to) }
func (d *reportDestination) Remove(name string) error     { return d.root.Remove(name) }
func (d *reportDestination) Sync() error                  { return d.dir.Sync() }

func (d *reportDestination) Close() error {
	var err error
	if d.dir != nil {
		err = d.dir.Close()
	}
	if err := errors.Join(err, d.root.Close()); err != nil {
		return fmt.Errorf("close parent of report %q: %w", d.path, err)
	}
	return nil
}

func (d *reportDestination) publish(ctx context.Context, result any) error {
	return writeRunnerReport(ctx, d, d.path, d.name, result)
}

type runnerStagingFile interface {
	io.Writer
	Sync() error
	Close() error
}

type runnerReportDirectory interface {
	OpenExclusive(string) (runnerStagingFile, error)
	Rename(from, to string) error
	Remove(string) error
	Sync() error
	Close() error
}

type reportPublicationState string

const (
	reportUnpublished reportPublicationState = "replacement not published"
	reportUnsynced    reportPublicationState = "replacement published, directory durability uncertain"
	reportPublished   reportPublicationState = "replacement published and synced"
)

type runnerPublicationError struct {
	path  string
	state reportPublicationState
	err   error
}

func (e *runnerPublicationError) Error() string {
	return fmt.Sprintf("write report %q: %s: %v", e.path, e.state, e.err)
}

func (e *runnerPublicationError) Unwrap() error { return e.err }

// writeRunnerReport takes ownership of the pinned directory and closes it
// even if cancellation or encoding prevents staging.
func writeRunnerReport(ctx context.Context, dir runnerReportDirectory, path, name string, result any) (err error) {
	state := reportUnpublished
	defer func() {
		err = errors.Join(err, dir.Close())
		if err != nil {
			err = &runnerPublicationError{path, state, err}
		}
	}()
	if err := ctx.Err(); err != nil {
		return err
	}
	data, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		return fmt.Errorf("encode runner report: %w", err)
	}
	staged := ".release-report-" + rand.Text()
	file, err := dir.OpenExclusive(staged)
	if err != nil {
		return fmt.Errorf("create staging report: %w", err)
	}
	defer func() {
		if state == reportUnpublished {
			if removeErr := dir.Remove(staged); removeErr != nil && !errors.Is(removeErr, os.ErrNotExist) {
				err = errors.Join(err, fmt.Errorf("remove staging report: %w", removeErr))
			}
		}
	}()

	data = append(data, '\n')
	n, err := file.Write(data)
	if err == nil && n != len(data) {
		err = io.ErrShortWrite
	}
	if err != nil {
		err = fmt.Errorf("write staging report (%d of %d bytes): %w", n, len(data), err)
	} else if syncErr := file.Sync(); syncErr != nil {
		err = fmt.Errorf("sync staging report: %w", syncErr)
	}
	if closeErr := file.Close(); closeErr != nil {
		err = errors.Join(err, fmt.Errorf("close staging report: %w", closeErr))
	}
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := dir.Rename(staged, name); err != nil {
		return fmt.Errorf("rename staging report: %w", err)
	}
	state = reportUnsynced
	if err := dir.Sync(); err != nil {
		return fmt.Errorf("sync report directory: %w", err)
	}
	state = reportPublished
	return nil
}
