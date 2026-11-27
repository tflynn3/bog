package main

import (
	"bytes"
	"encoding/json"
	"encoding/json/jsontext"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
)

var publicationReports = []struct {
	name     string
	required []string
}{
	{"admission", admissionCases},
	{"parity", []string{restoredParityCase, ordinaryWALCase}},
}

func publicationFixture(required []string, marker string) report {
	id := identity{schemaVersion, testRevision, strings.Repeat("a", 64), capacityFixtureID}
	cases := make(map[string]outcome, len(required))
	for _, name := range required {
		cases[name] = outcome{Status: "pass", Detail: &marker}
	}
	return report{id, cases}
}

func TestReportPublicationStages(t *testing.T) {
	t.Parallel()
	fault := errors.New("injected publication failure")
	for _, producer := range publicationReports {
		t.Run(producer.name, func(t *testing.T) {
			t.Parallel()
			for _, tc := range []struct {
				stage, diagnostic, operations string
				state                         reportPublicationState
				wantErr                       error
			}{
				{"encode", "encode release report", "", reportUnpublished, nil},
				{"open directory", "open report directory", "open directory", reportUnpublished, fault},
				{"create", "create staging report", "open directory,create,close directory", reportUnpublished, fault},
				{"write", "write staging report", "open directory,create,write,close file,remove,close directory", reportUnpublished, fault},
				{"short write", "write staging report", "open directory,create,write,close file,remove,close directory", reportUnpublished, io.ErrShortWrite},
				{"sync file", "sync staging report", "open directory,create,write,sync file,close file,remove,close directory", reportUnpublished, fault},
				{"close file", "close staging report", "open directory,create,write,sync file,close file,remove,close directory", reportUnpublished, fault},
				{"rename", "rename staging report", "open directory,create,write,sync file,close file,rename,remove,close directory", reportUnpublished, fault},
				{"sync directory", "sync report directory", "open directory,create,write,sync file,close file,rename,sync directory,close directory", reportDurabilityUncertain, fault},
				{"close directory", "close report directory", "open directory,create,write,sync file,close file,rename,sync directory,close directory", reportDurablyPublished, fault},
			} {
				t.Run(tc.stage, func(t *testing.T) {
					t.Parallel()
					dir := t.TempDir()
					path := filepath.Join(dir, producer.name+".json")
					prior := publicationFixture(producer.required, "prior attempt")
					previous, err := json.Marshal(prior)
					if err != nil {
						t.Fatal(err)
					}
					previous = append(previous, ' ', '\n', '\n')
					writeFixture(t, path, previous)

					replacement := publicationFixture(producer.required, "new attempt")
					if tc.stage == "encode" {
						replacement.Cases[producer.required[0]] = outcome{Evidence: jsontext.Value(`{"invalid":}`)}
					}
					ops := publicationFaults{t: t, dir: dir, path: path, stage: tc.stage, fault: fault}
					err = ops.publisher().writeReport(path, replacement)
					assertPublicationError(t, err, path, tc.state, tc.diagnostic, tc.wantErr)
					if got := strings.Join(ops.calls, ","); got != tc.operations {
						t.Fatalf("publication operations = %q, want %q", got, tc.operations)
					}
					got := readPublishedReport(t, path)
					if tc.state == reportUnpublished {
						if !bytes.Equal(got, previous) {
							t.Fatalf("previous bytes changed: got %q, want %q", got, previous)
						}
					} else {
						assertPublishedBytes(t, got, replacement, producer.required)
					}
					assertReportDirectory(t, dir, filepath.Base(path))

					retry := publicationFixture(producer.required, "successful retry")
					if err := writeReport(path, retry); err != nil {
						t.Fatal(err)
					}
					assertPublishedBytes(t, readPublishedReport(t, path), retry, producer.required)
					assertReportDirectory(t, dir, filepath.Base(path))
				})
			}
		})
	}
}

func TestReportPublicationFailedFirstAttemptLeavesNoReport(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "admission.json")
	fault := errors.New("rename failed")
	ops := publicationFaults{t: t, dir: dir, path: path, stage: "rename", fault: fault}
	err := ops.publisher().writeReport(path, publicationFixture(admissionCases, "first attempt"))
	assertPublicationError(t, err, path, reportUnpublished, "rename staging report", fault)
	assertReportDirectory(t, dir)
}

func TestReportPublicationKeepsCleanupFailures(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "parity.json")
	renameErr := errors.New("rename failed")
	removeErr := errors.New("staging removal failed")
	closeErr := errors.New("directory close failed")
	publisher := localReportPublisher()
	publisher.rename = func(_, _ string) error { return renameErr }
	publisher.remove = func(path string) error { return errors.Join(os.Remove(path), removeErr) }
	publisher.openDir = func(path string) (reportDirectory, error) {
		dir, err := os.Open(path)
		if err != nil {
			return nil, err
		}
		return faultDirectory{sync: dir.Sync, close: func() error {
			return errors.Join(dir.Close(), closeErr)
		}}, nil
	}
	err := publisher.writeReport(path, publicationFixture(publicationReports[1].required, "first attempt"))
	assertPublicationError(t, err, path, reportUnpublished, "rename staging report", renameErr)
	for _, cleanupErr := range []error{removeErr, closeErr} {
		if !errors.Is(err, cleanupErr) {
			t.Errorf("lost cleanup error %v in %v", cleanupErr, err)
		}
	}
	assertReportDirectory(t, dir)
}

func TestConcurrentReportPublication(t *testing.T) {
	t.Parallel()
	for _, producer := range publicationReports {
		t.Run(producer.name, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			path := filepath.Join(dir, producer.name+".json")
			initial := publicationFixture(producer.required, "previous")
			if err := writeReport(path, initial); err != nil {
				t.Fatal(err)
			}
			const writers = 8
			initialBytes := expectedReportBytes(t, initial)
			reports := make([]report, writers)
			whole := make(map[string]bool, writers+1)
			for i := range reports {
				reports[i] = publicationFixture(producer.required, fmt.Sprintf("%d-%s", i, strings.Repeat("x", (i+1)*4096)))
				whole[string(expectedReportBytes(t, reports[i]))] = true
			}
			start, published := make(chan struct{}), make(chan struct{})
			var wg sync.WaitGroup
			for _, results := range reports {
				wg.Go(func() {
					<-start
					if err := writeReport(path, results); err != nil {
						t.Errorf("concurrent publication: %v", err)
					}
				})
			}
			go func() {
				wg.Wait()
				close(published)
			}()
			readerOK := make(chan error, 1)
			go func() {
				close(start)
				for {
					got, err := os.ReadFile(path)
					if err != nil {
						readerOK <- err
						return
					}
					if !whole[string(got)] && !bytes.Equal(got, initialBytes) {
						readerOK <- fmt.Errorf("reader observed an incomplete report (%d bytes)", len(got))
						return
					}
					select {
					case <-published:
						readerOK <- nil
						return
					default:
					}
				}
			}()
			<-published
			if err := <-readerOK; err != nil {
				t.Fatal(err)
			}
			got := readPublishedReport(t, path)
			if !whole[string(got)] {
				t.Fatal("final report does not match any concurrent replacement")
			}
			if err := validateReport(got, initial.identity, producer.required); err != nil {
				t.Fatal(err)
			}
			assertReportDirectory(t, dir, filepath.Base(path))
		})
	}
}

type publicationFaults struct {
	t                *testing.T
	dir, path, stage string
	fault            error
	calls            []string
}

func (f *publicationFaults) call(name string) error {
	f.calls = append(f.calls, name)
	if name == f.stage {
		return f.fault
	}
	return nil
}

func (f *publicationFaults) publisher() reportPublisher {
	return reportPublisher{
		openDir: func(path string) (reportDirectory, error) {
			if err := f.call("open directory"); err != nil {
				return nil, err
			}
			dir, err := os.Open(path)
			if err != nil {
				return nil, err
			}
			return faultDirectory{
				sync: func() error {
					if err := f.call("sync directory"); err != nil {
						return err
					}
					return dir.Sync()
				},
				close: func() error { return errors.Join(dir.Close(), f.call("close directory")) },
			}, nil
		},
		createTemp: func(dir, pattern string) (reportStagingFile, error) {
			if err := f.call("create"); err != nil {
				return nil, err
			}
			if dir != f.dir {
				f.t.Errorf("staging directory = %q, want %q", dir, f.dir)
			}
			file, err := os.CreateTemp(dir, pattern)
			if err != nil {
				return nil, err
			}
			return faultStagingFile{
				File: file,
				write: func(data []byte) (int, error) {
					err := f.call("write")
					if err != nil || f.stage == "short write" {
						n, shortErr := file.Write(data[:len(data)/2])
						return n, errors.Join(shortErr, err)
					}
					return file.Write(data)
				},
				faultDirectory: faultDirectory{
					sync: func() error {
						if err := f.call("sync file"); err != nil {
							return err
						}
						return file.Sync()
					},
					close: func() error { return errors.Join(file.Close(), f.call("close file")) },
				},
			}, nil
		},
		rename: func(from, to string) error {
			if filepath.Dir(from) != f.dir || to != f.path {
				f.t.Errorf("unexpected rename from %q to %q", from, to)
			}
			if err := f.call("rename"); err != nil {
				return err
			}
			return os.Rename(from, to)
		},
		remove: func(path string) error { return errors.Join(os.Remove(path), f.call("remove")) },
	}
}

type faultDirectory struct {
	sync, close func() error
}

func (d faultDirectory) Sync() error  { return d.sync() }
func (d faultDirectory) Close() error { return d.close() }

type faultStagingFile struct {
	*os.File
	faultDirectory
	write func([]byte) (int, error)
}

func (f faultStagingFile) Write(data []byte) (int, error) { return f.write(data) }
func (f faultStagingFile) Sync() error                    { return f.faultDirectory.Sync() }
func (f faultStagingFile) Close() error                   { return f.faultDirectory.Close() }

func assertPublicationError(t *testing.T, err error, path string, state reportPublicationState, diagnostic string, cause error) {
	t.Helper()
	var publicationErr *reportPublicationError
	if !errors.As(err, &publicationErr) || publicationErr.State != state || publicationErr.Path != path {
		t.Fatalf("expected publication error for %q with state %q, got %v", path, state, err)
	}
	if !strings.Contains(err.Error(), diagnostic) || (cause != nil && !errors.Is(err, cause)) {
		t.Fatalf("expected diagnostic %q and cause %v, got %v", diagnostic, cause, err)
	}
}

func expectedReportBytes(t *testing.T, results report) []byte {
	t.Helper()
	data, err := json.MarshalIndent(results, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	return append(data, '\n')
}

func assertPublishedBytes(t *testing.T, data []byte, results report, required []string) {
	t.Helper()
	if err := validateReport(data, results.identity, required); err != nil {
		t.Fatal(err)
	}
	if want := expectedReportBytes(t, results); !bytes.Equal(data, want) {
		t.Fatalf("published bytes = %s, want %s", data, want)
	}
}

func readPublishedReport(t *testing.T, path string) []byte {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func assertReportDirectory(t *testing.T, dir string, names ...string) {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, entry := range entries {
		got = append(got, entry.Name())
	}
	if !slices.Equal(names, got) {
		t.Fatalf("report directory entries = %v, want %v", got, names)
	}
}
