package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"

	"bog/verify/reportpublication"
)

type reportDestination struct {
	*reportpublication.PinnedDirectory
	path, name string
	parent     os.FileInfo
	mode       reportpublication.Mode
}

func openReportDestination(path string) (_ *reportDestination, err error) {
	parent, name, err := reportpublication.SplitPath(path)
	if err != nil {
		return nil, fmt.Errorf("report path %q: %w", path, err)
	}
	dir, err := reportpublication.OpenDirectory(parent)
	if err != nil {
		return nil, fmt.Errorf("open parent of report %q: %w", path, err)
	}
	d := &reportDestination{PinnedDirectory: dir, path: path, name: name}
	defer func() {
		if err != nil {
			err = errors.Join(err, d.Close())
		}
	}()
	d.parent, err = dir.File().Stat()
	if err != nil {
		return nil, fmt.Errorf("stat parent of report %q: %w", path, err)
	}
	return d, nil
}

func (d *reportDestination) requireAbsent() error {
	_, err := d.Lstat(d.name)
	if err == nil {
		return fmt.Errorf("evidence destination already exists: %s: %w", d.path, os.ErrExist)
	}
	if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("inspect report destination %q: %w", d.path, err)
	}
	return nil
}

func (d *reportDestination) sameEntry(parent os.FileInfo, name string) bool {
	return d.name == name && os.SameFile(d.parent, parent)
}

func (d *reportDestination) Close() error {
	if err := d.PinnedDirectory.Close(); err != nil {
		return fmt.Errorf("close parent of report %q: %w", d.path, err)
	}
	return nil
}

func (d *reportDestination) publish(ctx context.Context, result any) error {
	return writeRunnerReport(ctx, d, d.path, d.name, result, d.mode)
}

func writeRunnerReport(ctx context.Context, dir reportpublication.OpenedDirectory, path, name string, result any, mode reportpublication.Mode) (err error) {
	state := reportpublication.Unpublished
	defer func() {
		err = errors.Join(err, dir.Close())
		if err != nil {
			err = &reportpublication.Error{Path: path, State: state, Mode: mode, Err: err}
		}
	}()
	if err := ctx.Err(); err != nil {
		return err
	}
	data, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		return fmt.Errorf("encode runner report: %w", err)
	}
	state, err = reportpublication.Publish(ctx, dir, name, append(data, '\n'), mode)
	return err
}
