//go:build !linux

package main

import (
	"errors"
	"os"
)

type localReportDirectory struct {
	*os.Root
	syncFile *os.File
}

func openReportDirectory(path string) (reportDirectory, error) {
	dir, err := os.OpenRoot(path)
	if err != nil {
		return nil, err
	}
	// Open the sync handle relative to the pinned directory. The caller's
	// path is resolved only by OpenRoot, even if it is retargeted concurrently.
	file, err := dir.Open(".")
	if err != nil {
		return nil, errors.Join(err, dir.Close())
	}
	return localReportDirectory{dir, file}, nil
}

func (d localReportDirectory) OpenExclusive(name string) (reportStagingFile, error) {
	return d.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
}

func (d localReportDirectory) Sync() error { return d.syncFile.Sync() }

func (d localReportDirectory) Close() error {
	return errors.Join(d.syncFile.Close(), d.Root.Close())
}
