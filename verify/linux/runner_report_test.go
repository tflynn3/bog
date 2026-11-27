package main

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"bog/verify/reportpublication"
)

func TestRunnerReportPublicationStates(t *testing.T) {
	t.Parallel()
	failure := errors.New("injected failure")
	for _, tc := range []struct {
		name, failAt, cancelAt string
		state                  reportpublication.State
		cause                  error
		operations             []string
	}{
		{"cancel before staging", "", "start", reportpublication.Unpublished, context.Canceled, []string{"close directory"}},
		{"create fails", "create", "", reportpublication.Unpublished, failure, []string{"create", "close directory"}},
		{"write fails", "write", "", reportpublication.Unpublished, failure, []string{"create", "write", "close file", "remove", "close directory"}},
		{"short write", "short", "", reportpublication.Unpublished, io.ErrShortWrite, []string{"create", "write", "close file", "remove", "close directory"}},
		{"file sync fails", "sync file", "", reportpublication.Unpublished, failure, []string{"create", "write", "sync file", "close file", "remove", "close directory"}},
		{"file close fails", "close file", "", reportpublication.Unpublished, failure, []string{"create", "write", "sync file", "close file", "remove", "close directory"}},
		{"cancel before rename", "", "close file", reportpublication.Unpublished, context.Canceled, []string{"create", "write", "sync file", "close file", "remove", "close directory"}},
		{"rename fails", "rename", "", reportpublication.Unpublished, failure, []string{"create", "write", "sync file", "close file", "rename", "remove", "close directory"}},
		{"directory sync fails", "sync directory", "", reportpublication.DurabilityUncertain, failure, []string{"create", "write", "sync file", "close file", "rename", "sync directory", "close directory"}},
		{"directory close fails", "close directory", "", reportpublication.DurablyPublished, failure, []string{"create", "write", "sync file", "close file", "rename", "sync directory", "close directory"}},
		{"cancel after rename", "", "rename", reportpublication.DurablyPublished, nil, []string{"create", "write", "sync file", "close file", "rename", "sync directory", "close directory"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			directory := t.TempDir()
			path := filepath.Join(directory, "report.json")
			if err := os.WriteFile(path, []byte("previous"), 0600); err != nil {
				t.Fatal(err)
			}
			dest, err := openReportDestination(path)
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			if tc.cancelAt == "start" {
				cancel()
			}
			ops := &reportDirectoryFixture{reportDestination: dest}
			ops.action = func(op string) error {
				ops.operations = append(ops.operations, op)
				if op == tc.cancelAt {
					cancel()
				}
				if op == tc.failAt {
					return failure
				}
				return nil
			}
			ops.short = tc.failAt == "short"
			err = writeRunnerReport(ctx, ops, path, dest.name, "new", reportpublication.Replace)
			var publication *reportpublication.Error
			if tc.cause == nil {
				if err != nil {
					t.Fatal(err)
				}
			} else if !errors.Is(err, tc.cause) || !errors.As(err, &publication) ||
				publication.State != tc.state || publication.Path != path {
				t.Fatalf("publication error = %v, want %q with %v", err, tc.state, tc.cause)
			}
			if !slices.Equal(tc.operations, ops.operations) {
				t.Fatalf("operations = %v, want %v", ops.operations, tc.operations)
			}
			want := "previous"
			if tc.state != reportpublication.Unpublished {
				want = "\"new\"\n"
			}
			if data, err := os.ReadFile(path); err != nil || string(data) != want {
				t.Fatalf("report = %q, %v, want %q", data, err, want)
			}
			if files, err := os.ReadDir(directory); err != nil || len(files) != 1 || files[0].Name() != dest.name {
				t.Fatalf("directory entries = %v, %v", files, err)
			}
		})
	}
}

type reportDirectoryFixture struct {
	*reportDestination
	action     func(string) error
	operations []string
	short      bool
}

func (d *reportDirectoryFixture) OpenExclusive(name string) (reportpublication.StagingFile, error) {
	if err := d.action("create"); err != nil {
		return nil, err
	}
	f, err := d.reportDestination.OpenExclusive(name)
	if err != nil {
		return nil, err
	}
	return reportFileFixture{f, d.action, d.short}, nil
}

func (d *reportDirectoryFixture) Rename(from, to string) error {
	if err := d.action("rename"); err != nil {
		return err
	}
	return d.reportDestination.Rename(from, to)
}

func (d *reportDirectoryFixture) Remove(name string) error {
	return errors.Join(d.action("remove"), d.reportDestination.Remove(name))
}

func (d *reportDirectoryFixture) Sync() error {
	if err := d.action("sync directory"); err != nil {
		return err
	}
	return d.reportDestination.Sync()
}

func (d *reportDirectoryFixture) Close() error {
	return errors.Join(d.action("close directory"), d.reportDestination.Close())
}

type reportFileFixture struct {
	reportpublication.StagingFile
	action func(string) error
	short  bool
}

func (f reportFileFixture) Write(data []byte) (int, error) {
	if err := f.action("write"); err != nil {
		return 0, err
	}
	if f.short {
		data = data[:len(data)-1]
	}
	return f.StagingFile.Write(data)
}

func (f reportFileFixture) Sync() error {
	if err := f.action("sync file"); err != nil {
		return err
	}
	return f.StagingFile.Sync()
}

func (f reportFileFixture) Close() error {
	return errors.Join(f.action("close file"), f.StagingFile.Close())
}

func TestRunnerReportRetargetDuringStaging(t *testing.T) {
	t.Parallel()
	original, alternate, links := t.TempDir(), t.TempDir(), t.TempDir()
	alias := filepath.Join(links, "parent")
	replacement := filepath.Join(links, "replacement")
	path := filepath.Join(alias, "report.json")
	for link, target := range map[string]string{alias: original, replacement: alternate} {
		if err := os.Symlink(target, link); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(alternate, "report.json"), []byte("decoy"), 0600); err != nil {
		t.Fatal(err)
	}
	dest, err := openReportDestination(path)
	if err != nil {
		t.Fatal(err)
	}
	dir := &reportDirectoryFixture{reportDestination: dest, action: func(op string) error {
		if op == "sync file" {
			return os.Rename(replacement, alias)
		}
		return nil
	}}
	if err := writeRunnerReport(t.Context(), dir, path, dest.name, "pinned", reportpublication.Replace); err != nil {
		t.Fatal(err)
	}
	for parent, want := range map[string]string{original: "\"pinned\"\n", alternate: "decoy"} {
		if data, err := os.ReadFile(filepath.Join(parent, dest.name)); err != nil || string(data) != want {
			t.Fatalf("%s = %q, %v, want %q", parent, data, err, want)
		}
		if entries, err := os.ReadDir(parent); err != nil || len(entries) != 1 {
			t.Fatalf("%s entries = %v, %v", parent, entries, err)
		}
	}
}
