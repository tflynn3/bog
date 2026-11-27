package client

import (
	"context"
	"errors"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"testing"
	"time"
)

const exportCrashTarget = "http://bog.test"

func TestFileExportProcessInterruptedReplacement(t *testing.T) {
	for _, tc := range []struct {
		name          string
		journal       bool
		beforeRename  bool
		wantRequests  []string
		wantDelivered []int
	}{
		{"opening journal before rename", true, true, []string{"", fixtureCursor}, []int{1, 2}},
		{"opening journal before directory sync", true, false, []string{fixtureCursor}, []int{1, 2}},
		{"acknowledgment before rename", false, true, []string{fixtureCursor}, []int{1, 2}},
		{"acknowledgment before directory sync", false, false, []string{fixtureCursor}, []int{2}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			path := filepath.Join(dir, "Progress")
			resume := path
			if exportCaseAlias(t, dir) {
				resume = filepath.Join(dir, "progress")
			}
			store := newTestExportStore(t, resume)
			c := newTestExportClient(t, exportCrashTarget)
			var requests []string
			c.http.Transport = queryRoundTripper(func(r *http.Request) (*http.Response, error) {
				cursor := r.URL.Query().Get("cursor")
				requests = append(requests, cursor)
				if cursor == "" {
					return exportTestResponse(contractRow("opening"), fixtureCursor), nil
				}
				return exportTestResponse(contractRow("continuation"), ""), nil
			})
			var delivered []int
			session := newTestExport(t, c, Query{}, 2, store, func(_ context.Context, p ExportPage) error {
				delivered = append(delivered, p.Number)
				want := "continuation"
				if p.Number == 1 {
					want = "opening"
				}
				if !slices.Equal(exportLines(p.Page), []string{want}) {
					t.Errorf("page %d has rows %q, want %q", p.Number, exportLines(p.Page), want)
				}
				return nil
			})
			if !tc.journal {
				writeLegacyExportFile(t, path, crashOpeningProgress(session.binding))
			}

			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestFileExportCrashHelper$")
			cmd.Env = append(cmd.Environ(), "BOG_TEST_EXPORT_CRASH_PATH="+path, "GORACE=atexit_sleep_ms=0")
			if tc.beforeRename {
				cmd.Env = append(cmd.Env, "BOG_TEST_EXPORT_CRASH_BEFORE_RENAME=1")
			}
			cmd.WaitDelay = time.Second
			cmd.Stdout, cmd.Stderr = os.Stderr, os.Stderr
			if err := cmd.Start(); err != nil {
				t.Fatal(err)
			}
			waited := false
			defer func() {
				if !waited {
					cancel()
					if err := cmd.Wait(); err == nil {
						t.Error("child unexpectedly exited successfully")
					}
				}
			}()
			if err := waitForExportCrashGate(ctx, path+".ready"); err != nil {
				t.Fatalf("child did not reach interruption point: %v", err)
			}
			stage := filepath.Join(dir, exportStageName("Progress"))
			if tc.beforeRename {
				data, err := os.ReadFile(stage)
				if err != nil || !strings.HasPrefix(string(data), exportFileMagic) {
					t.Fatalf("stage missing or incomplete at interruption gate: %v", err)
				}
				if tc.journal && !strings.Contains(string(data), contractRow("opening")) {
					t.Fatal("stage does not contain the opening page")
				}
			} else {
				assertExportStageMissing(t, stage)
			}
			if _, _, err := store.Load(ctx, DefaultMaxPage); !errors.Is(err, ErrExportCheckpointBusy) {
				t.Errorf("recovery during active save: %v", err)
			}
			if err := store.Save(ctx, crashOpeningProgress(session.binding)); !errors.Is(err, ErrExportCheckpointBusy) {
				t.Errorf("competing save: %v", err)
			}
			if tc.beforeRename {
				if _, err := os.Lstat(stage); err != nil {
					t.Fatalf("competing access changed the active stage: %v", err)
				}
			}
			if err := cmd.Process.Kill(); err != nil {
				t.Fatal(err)
			}
			err := cmd.Wait()
			waited = true
			var exit *exec.ExitError
			if !errors.As(err, &exit) || exit.Sys().(syscall.WaitStatus).Signal() != syscall.SIGKILL {
				t.Fatalf("expected a killed child: %v", err)
			}

			if err := session.Run(ctx); err != nil {
				t.Fatal(err)
			}
			assertExportStageMissing(t, stage)
			if p := loadTestExport(t, store); !p.Complete || p.AcknowledgedPages != 2 || p.Opening != nil {
				t.Fatalf("checkpoint after resuming: %+v", p)
			}
			if !slices.Equal(requests, tc.wantRequests) || !slices.Equal(delivered, tc.wantDelivered) {
				t.Errorf("requested %q and delivered %v, want %q and %v", requests, delivered, tc.wantRequests, tc.wantDelivered)
			}
		})
	}
}

func TestFileExportCrashHelper(t *testing.T) {
	path := os.Getenv("BOG_TEST_EXPORT_CRASH_PATH")
	if path == "" {
		return
	}
	store := newTestExportStore(t, path)
	lease, err := store.Acquire()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := lease.Close(); err != nil {
			t.Error(err)
		}
	})
	session := newTestExport(t, newTestExportClient(t, exportCrashTarget), Query{}, 2, lease, func(context.Context, ExportPage) error {
		return errors.New("unexpected sink delivery")
	})
	progress, found, err := lease.Load(t.Context(), DefaultMaxPage)
	if err != nil {
		t.Fatal(err)
	}
	if found {
		if progress.Opening == nil {
			t.Fatal("expected a journaled opening page")
		}
		progress = progress.advance(progress.Opening.NextCursor)
	} else {
		progress = crashOpeningProgress(session.binding)
	}
	hold := func() error {
		if err := os.WriteFile(path+".ready", nil, 0600); err != nil {
			return err
		}
		select {
		case <-t.Context().Done():
			return t.Context().Err()
		case <-time.After(15 * time.Second):
			return errors.New("parent did not kill the child at the checkpoint gate")
		}
	}
	rename, syncDir := (*os.Root).Rename, syncExportDirectory
	if os.Getenv("BOG_TEST_EXPORT_CRASH_BEFORE_RENAME") != "" {
		rename = func(*os.Root, string, string) error { return hold() }
	} else {
		syncDir = func(*os.Root) error { return hold() }
	}
	t.Fatalf("checkpoint save returned unexpectedly: %v", lease.save(t.Context(), progress, rename, syncDir))
}

func crashOpeningProgress(binding [32]byte) ExportProgress {
	return ExportProgress{Version: ExportProgressVersion, Binding: binding,
		Opening: &ExportOpeningPage{Body: []byte(contractRow("opening")), NextCursor: fixtureCursor}}
}

func waitForExportCrashGate(ctx context.Context, marker string) error {
	poll := time.NewTicker(10 * time.Millisecond)
	defer poll.Stop()
	for {
		if _, err := os.Stat(marker); !errors.Is(err, os.ErrNotExist) {
			return err
		}
		select {
		case <-poll.C:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}
