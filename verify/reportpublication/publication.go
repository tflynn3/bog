// Package reportpublication durably commits encoded reports in pinned directories.
package reportpublication

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

type State string

const (
	Unpublished         State = "replacement not published"
	DurabilityUncertain State = "replacement published, directory durability uncertain"
	DurablyPublished    State = "replacement published and synced"
)

// Error identifies a report path and the state reached by this publication.
type Error struct {
	Path  string
	State State
	Err   error
	Mode  Mode
}

func (e *Error) Error() string {
	state := string(e.State)
	if e.Mode == CreateOnly {
		state = strings.Replace(state, "replacement", "report", 1)
	}
	return fmt.Sprintf("write report %q: %s: %v", e.Path, state, e.Err)
}

func (e *Error) Unwrap() error { return e.Err }

// Directory supplies operations relative to one pinned parent.
type Directory interface {
	OpenExclusive(name string) (StagingFile, error)
	Rename(from, to string) error
	Remove(name string) error
	Sync() error
}

type StagingFile interface {
	io.Writer
	Sync() error
	Close() error
}

// OpenedDirectory is a directory with a caller-owned close operation.
type OpenedDirectory interface {
	Directory
	Close() error
}

// Mode controls which destination entries a commit may install.
type Mode uint8

const (
	Replace Mode = iota
	CreateOnly
)

// SplitPath leaves the parent spelling intact for filesystem resolution.
func SplitPath(path string) (parent, name string, err error) {
	parent, name = filepath.Split(path)
	if !validBasename(name) {
		return "", "", errors.New("report path must name a file")
	}
	if parent == "" {
		parent = "."
	}
	return parent, name, nil
}

func validBasename(name string) bool {
	return name != "" && name != "." && name != ".." && filepath.Base(name) == name &&
		!strings.ContainsRune(name, 0) && filepath.VolumeName(name) == ""
}

// WriteFile replaces a report and owns the directory it opens. A nil open
// function uses the local filesystem.
func WriteFile(path string, data []byte, open func(string) (OpenedDirectory, error)) (err error) {
	state := Unpublished
	defer func() {
		if err != nil {
			err = &Error{Path: path, State: state, Err: err}
		}
	}()
	parent, name, err := SplitPath(path)
	if err != nil {
		return err
	}
	if open == nil {
		open = func(path string) (OpenedDirectory, error) { return OpenDirectory(path) }
	}
	dir, err := open(parent)
	if err != nil {
		return fmt.Errorf("open report directory: %w", err)
	}
	defer func() {
		if closeErr := dir.Close(); closeErr != nil {
			err = errors.Join(err, fmt.Errorf("close report directory: %w", closeErr))
		}
	}()
	state, err = Publish(context.Background(), dir, name, data, Replace)
	return err
}

func createStagingReport(dir Directory, destination string) (StagingFile, string, error) {
	for range 16 {
		name := ".release-report-" + rand.Text()
		if name == destination {
			continue
		}
		file, err := dir.OpenExclusive(name)
		if !errors.Is(err, os.ErrExist) {
			return file, name, err
		}
	}
	return nil, "", errors.New("could not allocate a unique staging report")
}

// Publish borrows the pinned dir and owns the staging file it creates. For
// CreateOnly, dir must provide RenameNoReplace(from, to string) error with
// atomic create-only semantics. Cancellation before commit leaves the
// destination untouched. A successful commit always attempts directory sync.
func Publish(ctx context.Context, dir Directory, name string, data []byte, mode Mode) (state State, err error) {
	state = Unpublished
	if err := ctx.Err(); err != nil {
		return state, err
	}
	if !validBasename(name) {
		return state, errors.New("report destination must be a basename")
	}
	rename := dir.Rename
	switch mode {
	case Replace:
	case CreateOnly:
		creator, ok := dir.(interface{ RenameNoReplace(from, to string) error })
		if !ok {
			return state, fmt.Errorf("create-only report publication: %w", errors.ErrUnsupported)
		}
		rename = creator.RenameNoReplace
	default:
		return state, fmt.Errorf("invalid publication mode %d", mode)
	}
	staged, stagedName, err := createStagingReport(dir, name)
	if err != nil {
		return state, fmt.Errorf("create staging report: %w", err)
	}
	defer func() {
		if state == Unpublished {
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
	if err := rename(stagedName, name); err != nil {
		return state, fmt.Errorf("rename staging report: %w", err)
	}
	state = DurabilityUncertain
	if err := dir.Sync(); err != nil {
		return state, fmt.Errorf("sync report directory: %w", err)
	}
	return DurablyPublished, nil
}
