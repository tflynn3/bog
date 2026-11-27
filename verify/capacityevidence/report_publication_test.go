package capacityevidence

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

func TestReplaceReportCancellation(t *testing.T) {
	t.Parallel()
	syncErr := errors.New("directory sync failed")
	for _, tc := range []struct {
		name, cancelAfter, operations string
		syncErr, wantErr              error
		state                         ReportPublicationState
		wantBytes                     string
	}{
		{"before staging", "", "", nil, context.Canceled, ReportUnpublished, "previous"},
		{"before rename", "close file", "create,write,sync file,close file,remove", nil, context.Canceled, ReportUnpublished, "previous"},
		{"at commit", "rename", "create,write,sync file,close file,rename,sync directory", nil, nil, ReportDurablyPublished, "replacement"},
		{"at commit with failed sync", "rename", "create,write,sync file,close file,rename,sync directory", syncErr, syncErr, ReportDurabilityUncertain, "replacement"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			if tc.cancelAfter == "" {
				cancel()
			}
			parent := t.TempDir()
			path := filepath.Join(parent, "report.json")
			if err := os.WriteFile(path, []byte("previous"), 0600); err != nil {
				t.Fatal(err)
			}
			dir := openPublicationTestDirectory(t, parent)
			var calls []string
			dir.after = func(op string) error {
				calls = append(calls, op)
				if op == tc.cancelAfter {
					cancel()
				}
				if op == "sync directory" {
					return tc.syncErr
				}
				return nil
			}

			state, err := replaceReport(ctx, dir, "report.json", []byte("replacement"))
			if state != tc.state || !errors.Is(err, tc.wantErr) {
				t.Fatalf("outcome = (%q, %v), want (%q, %v)", state, err, tc.state, tc.wantErr)
			}
			if got := strings.Join(calls, ","); got != tc.operations {
				t.Fatalf("operations = %q, want %q", got, tc.operations)
			}
			assertPublicationFile(t, path, tc.wantBytes)
			assertPublicationEntries(t, parent, "report.json")
			if err := dir.ReportDirectory.Sync(); err != nil {
				t.Fatalf("caller no longer owns a usable directory: %v", err)
			}
		})
	}
}

func TestWriteReportStagingFailures(t *testing.T) {
	t.Parallel()
	createErr := errors.New("create failed")
	writeErr := errors.New("write failed")
	closeFileErr := errors.New("file close failed")
	removeErr := errors.New("remove failed")
	closeDirErr := errors.New("directory close failed")
	for _, tc := range []struct {
		name       string
		collisions int
		createErr  error
		after      map[string]error
		operations string
		wantText   string
		wantCauses []error
	}{
		{
			name: "collision budget exhausted", collisions: 16,
			wantText: "create staging report: could not allocate a unique staging report",
		},
		{
			name: "non-collision stops retries", collisions: 1, createErr: createErr,
			wantText: "create staging report: create failed", wantCauses: []error{createErr},
		},
		{
			name:       "joined staging and directory failures",
			after:      map[string]error{"write": writeErr, "close file": closeFileErr, "remove": removeErr, "close directory": closeDirErr},
			operations: "write,close file,remove,",
			wantText:   "write staging report (6 of 6 bytes): write failed\nclose staging report: file close failed\nremove staging report %q: remove failed\nclose report directory: directory close failed",
			wantCauses: []error{writeErr, closeFileErr, removeErr, closeDirErr},
		},
		{
			name:       "absent staging entry during cleanup",
			after:      map[string]error{"close file": closeFileErr, "remove": os.ErrNotExist},
			operations: "write,sync file,close file,remove,",
			wantText:   "close staging report: file close failed", wantCauses: []error{closeFileErr},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			parent := t.TempDir()
			path := filepath.Join(parent, "report.json")
			if err := os.WriteFile(path, []byte("prior"), 0600); err != nil {
				t.Fatal(err)
			}
			calls := []string{}
			var attempted []string
			p := ReportPublisher{OpenDir: func(path string) (ReportDirectory, error) {
				dir, err := OpenReportDirectory(path)
				if err != nil {
					return nil, err
				}
				return &publicationTestDirectory{
					ReportDirectory: dir,
					after: func(op string) error {
						calls = append(calls, op)
						return tc.after[op]
					},
					beforeCreate: func(name string) error {
						attempted = append(attempted, name)
						if len(attempted) <= tc.collisions {
							return &os.PathError{Op: "open", Path: name, Err: os.ErrExist}
						}
						return tc.createErr
					},
				}, nil
			}}
			err := p.WriteReport(path, "new")
			wantAttempts := tc.collisions + 1
			if tc.collisions == 16 {
				wantAttempts = 16
			}
			if len(attempted) != wantAttempts {
				t.Fatalf("staging attempts = %d, want %d", len(attempted), wantAttempts)
			}
			wantText := tc.wantText
			if strings.Contains(wantText, "%q") {
				wantText = fmt.Sprintf(wantText, attempted[len(attempted)-1])
			}
			var outcome *ReportPublicationError
			if !errors.As(err, &outcome) || outcome.Path != path || outcome.State != ReportUnpublished || outcome.Err.Error() != wantText {
				t.Fatalf("publication error = %v, want unpublished error for %q with cause %q", err, path, wantText)
			}
			for _, cause := range tc.wantCauses {
				if !errors.Is(err, cause) {
					t.Errorf("error %v lost cause %v", err, cause)
				}
			}
			if got, want := strings.Join(calls, ","), strings.Repeat("create,", wantAttempts)+tc.operations+"close directory"; got != want {
				t.Fatalf("operations = %q, want %q", got, want)
			}
			assertPublicationFile(t, path, "prior")
			assertPublicationEntries(t, parent, "report.json")
		})
	}
}

func TestWriteReportPreservesPathAndEncoding(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	parent := filepath.Join(root, "selected")
	subdir := filepath.Join(parent, "child")
	if err := os.MkdirAll(subdir, 0700); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(root, "alias")
	if err := os.Symlink(subdir, link); err != nil {
		t.Fatal(err)
	}
	decoy := filepath.Join(root, "report.json")
	if err := os.WriteFile(decoy, []byte("decoy"), 0600); err != nil {
		t.Fatal(err)
	}
	path := link + "/../report.json"
	selected := filepath.Join(parent, "report.json")
	if err := os.WriteFile(selected, []byte("previous"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := (ReportPublisher{}).WriteReport(path, map[string]string{"value": "<ok>"}); err != nil {
		t.Fatal(err)
	}
	assertPublicationFile(t, selected, "{\n  \"value\": \"\\u003cok\\u003e\"\n}\n")
	assertPublicationFile(t, decoy, "decoy")
	info, err := os.Stat(selected)
	if err != nil {
		t.Fatal(err)
	}
	if got := info.Mode().Perm(); !info.Mode().IsRegular() || got != 0600 {
		t.Fatalf("mode = %v, want regular file with permissions 0600", info.Mode())
	}
	assertPublicationEntries(t, parent, "child", "report.json")
}

func TestWriteReportClosePreservesCommitState(t *testing.T) {
	t.Parallel()
	closeErr := errors.New("directory close failed")
	syncErr := errors.New("directory sync failed")
	for _, tc := range []struct {
		name    string
		syncErr error
		state   ReportPublicationState
	}{
		{"synced", nil, ReportDurablyPublished},
		{"sync failed", syncErr, ReportDurabilityUncertain},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			parent := t.TempDir()
			path := filepath.Join(parent, "report.json")
			p := ReportPublisher{OpenDir: func(path string) (ReportDirectory, error) {
				dir, err := OpenReportDirectory(path)
				if err != nil {
					return nil, err
				}
				return &publicationTestDirectory{ReportDirectory: dir, after: func(op string) error {
					switch op {
					case "sync directory":
						return tc.syncErr
					case "close directory":
						return closeErr
					default:
						return nil
					}
				}}, nil
			}}
			err := p.WriteReport(path, "new")
			var outcome *ReportPublicationError
			if !errors.As(err, &outcome) || outcome.Path != path || outcome.State != tc.state {
				t.Fatalf("publication error = %v, want %q for %q", err, tc.state, path)
			}
			if !errors.Is(err, closeErr) || errors.Is(err, syncErr) != (tc.syncErr != nil) {
				t.Fatalf("publication error lost an operation failure: %v", err)
			}
			assertPublicationFile(t, path, "\"new\"\n")
			assertPublicationEntries(t, parent, "report.json")
		})
	}
}

type publicationTestDirectory struct {
	ReportDirectory
	beforeCreate func(string) error
	after        func(string) error
}

func openPublicationTestDirectory(t *testing.T, parent string) *publicationTestDirectory {
	t.Helper()
	dir, err := OpenReportDirectory(parent)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := dir.Close(); err != nil {
			t.Error(err)
		}
	})
	return &publicationTestDirectory{ReportDirectory: dir}
}

func (d *publicationTestDirectory) OpenExclusive(name string) (ReportStagingFile, error) {
	if err := d.after("create"); err != nil {
		return nil, err
	}
	if d.beforeCreate != nil {
		if err := d.beforeCreate(name); err != nil {
			return nil, err
		}
	}
	f, err := d.ReportDirectory.OpenExclusive(name)
	if err != nil {
		return nil, err
	}
	return publicationTestFile{f, d.after}, nil
}

func (d *publicationTestDirectory) Rename(from, to string) error {
	return errors.Join(d.ReportDirectory.Rename(from, to), d.after("rename"))
}

func (d *publicationTestDirectory) Remove(name string) error {
	return errors.Join(d.ReportDirectory.Remove(name), d.after("remove"))
}

func (d *publicationTestDirectory) Sync() error {
	return errors.Join(d.ReportDirectory.Sync(), d.after("sync directory"))
}

func (d *publicationTestDirectory) Close() error {
	return errors.Join(d.ReportDirectory.Close(), d.after("close directory"))
}

type publicationTestFile struct {
	ReportStagingFile
	after func(string) error
}

func (f publicationTestFile) Write(p []byte) (int, error) {
	n, err := f.ReportStagingFile.Write(p)
	return n, errors.Join(err, f.after("write"))
}

func (f publicationTestFile) Sync() error {
	return errors.Join(f.ReportStagingFile.Sync(), f.after("sync file"))
}

func (f publicationTestFile) Close() error {
	return errors.Join(f.ReportStagingFile.Close(), f.after("close file"))
}

func assertPublicationFile(t *testing.T, path, want string) {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil || string(data) != want {
		t.Fatalf("read %q = (%q, %v), want %q", path, data, err, want)
	}
}

func assertPublicationEntries(t *testing.T, parent string, want ...string) {
	t.Helper()
	entries, err := os.ReadDir(parent)
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, entry := range entries {
		names = append(names, entry.Name())
	}
	if !slices.Equal(names, want) {
		t.Fatalf("directory entries = %q, want %q", names, want)
	}
}
