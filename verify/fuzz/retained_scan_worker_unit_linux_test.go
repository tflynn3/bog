package fuzz

import (
	"context"
	"errors"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"bog/internal/checkpoint"
	"bog/internal/history"
)

func TestScanMemoryAddressLimit(t *testing.T) {
	const baseline = 17 << 30
	const limit = baseline + scanMemoryAddressGrowth
	for _, tc := range []struct {
		name      string
		baseline  uint64
		inherited syscall.Rlimit
		want      uint64
	}{
		{"unlimited", baseline, syscall.Rlimit{Cur: math.MaxUint64, Max: math.MaxUint64}, limit},
		{"exact limit", baseline, syscall.Rlimit{Cur: limit, Max: limit}, limit},
		{"soft cap", baseline, syscall.Rlimit{Cur: limit - 1, Max: limit}, 0},
		{"hard cap", baseline, syscall.Rlimit{Cur: limit, Max: limit - 1}, 0},
		{"missing baseline", 0, syscall.Rlimit{Cur: limit, Max: limit}, 0},
		{"addition overflow", math.MaxUint64, syscall.Rlimit{Cur: math.MaxUint64, Max: math.MaxUint64}, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := scanMemoryAddressLimit(tc.baseline, tc.inherited)
			if got != tc.want || (err != nil) != (tc.want == 0) {
				t.Fatalf("address limit = %d, %v, want %d", got, err, tc.want)
			}
		})
	}
}

func TestScanMemoryWorkerFailures(t *testing.T) {
	for _, tc := range []struct {
		name, readers, want string
		started             bool
	}{
		{"invalid request", "08", "harness startup: invalid measurement concurrency", false},
		{"missing fixture", "8", "reader workload:", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), corpusDeadline)
			defer cancel()
			cmd := exec.CommandContext(ctx, "/proc/self/exe", "-test.run=^TestRetainedScanMemoryWorker$", "-test.count=1")
			cmd.Env = append(cmd.Environ(), scanMemoryWorkerEnv+"="+tc.readers, scanMemoryCgroupEnv+"=", "GOMAXPROCS=8")
			cmd.Dir = t.TempDir()
			cmd.WaitDelay = time.Second
			output, err := runBoundedReplayWorker(cmd)
			var exit *exec.ExitError
			if !errors.As(err, &exit) || ctx.Err() != nil || !strings.Contains(string(output), tc.want) {
				t.Fatalf("worker wanted %q, got %v (context %v)\n%s", tc.want, err, ctx.Err(), output)
			}
			var startup scanMemoryStartup
			startErr := decodeScanMemoryRecord(output, scanMemoryStartupPrefix, &startup)
			if (startErr == nil) != tc.started || strings.Contains(string(output), scanMemoryReportPrefix) {
				t.Fatalf("unexpected worker records: startup=%v\n%s", startErr, output)
			}
			if tc.started {
				must(t, startup.validate())
			}
		})
	}
}

func TestScanMemoryCatalogParserCleanup(t *testing.T) {
	for _, tc := range []struct {
		name, diagnostic string
		edit             func(*testing.T, files, checkpoint.Manifest)
	}{
		{"oversized manifest", "invalid manifest size", func(_ *testing.T, image files, _ checkpoint.Manifest) {
			image[manifestFile] = make([]byte, maxArtifactSize)
		}},
		{"oversized row declaration", "invalid row data", func(t *testing.T, image files, m checkpoint.Manifest) {
			segment := &m.Segments[0]
			block := blockEdit(image[segment.Name], func(b []byte) {
				le.PutUint32(b[blockHeader+20:blockHeader+rowHeader], math.MaxUint32)
			})
			image[segment.Name] = block
			segment.SHA256 = digest(block)
			envelope, _, err := retainedEnvelope(m)
			must(t, err)
			image[manifestFile] = envelope
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			image, _, manifest := scanMemoryUnitImage(t)
			tc.edit(t, image, manifest)
			path, err := filepath.EvalSymlinks(t.TempDir())
			must(t, err)
			for name, contents := range image {
				must(t, os.WriteFile(filepath.Join(path, name), contents, 0600))
			}
			b := checkpoint.NewBudget(checkpoint.Limits{checkpoint.PinnedGenerations: 1, checkpoint.LiveCaptures: 1})
			catalog := history.NewCatalog(b)
			defer func() { must(t, catalog.Close()) }()
			before := scanMemoryTotalAlloc()
			gen, err := catalog.Replace(t.Context(), path)
			after := scanMemoryTotalAlloc()
			if gen != (history.Generation{}) || !errors.Is(err, history.ErrIntegrity) || !strings.Contains(err.Error(), tc.diagnostic) {
				t.Fatalf("malformed catalog = %+v, %v, want %q", gen, err, tc.diagnostic)
			}
			must(t, checkScanMemoryBytes("malformed catalog allocation", before, after, 1<<20))
			must(t, checkRetainedReaderBudget(b))
			must(t, checkRetainedReaderFDs(path, nil))

			valid, _, _ := scanMemoryUnitImage(t)
			for name, contents := range valid {
				must(t, os.WriteFile(filepath.Join(path, name), contents, 0600))
			}
			gen, err = catalog.Replace(t.Context(), path)
			must(t, err)
			must(t, catalog.Close())
			select {
			case <-gen.Drained:
			default:
				t.Fatal("retry generation still pinned")
			}
			must(t, checkRetainedReaderBudget(b))
			must(t, checkRetainedReaderFDs(path, nil))
		})
	}
}
