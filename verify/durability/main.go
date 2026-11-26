// Command durability verifies a pinned Bog binary on Linux.
package main

import (
	"context"
	"crypto/sha256"
	"debug/buildinfo"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"regexp"
	"runtime"
	"syscall"
	"time"
)

type outcome struct {
	Status   string          `json:"status"`
	Detail   string          `json:"detail,omitempty"`
	Evidence *enospcEvidence `json:"evidence,omitempty"`
}

type report struct {
	SchemaVersion int                `json:"schema_version"`
	Revision      string             `json:"revision"`
	BinarySHA256  string             `json:"binary_sha256"`
	Fixture       string             `json:"fixture"`
	Cases         map[string]outcome `json:"cases"`
}

var revisionPattern = regexp.MustCompile(`^[a-f0-9]{40}$`)
var digestPattern = regexp.MustCompile(`^[a-f0-9]{64}$`)

func pinBinary(source, copyPath, revision, expectedDigest string) (string, error) {
	f, err := os.OpenFile(source, os.O_RDONLY|syscall.O_NONBLOCK, 0)
	if err != nil {
		return "", err
	}
	info, err := f.Stat()
	if err == nil && (!info.Mode().IsRegular() || info.Mode()&0111 == 0) {
		err = errors.New("candidate must be an executable regular file")
	}
	if err != nil {
		return "", errors.Join(err, f.Close())
	}
	dst, err := os.OpenFile(copyPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0700)
	if err != nil {
		return "", errors.Join(err, f.Close())
	}
	hash := sha256.New()
	_, err = io.Copy(io.MultiWriter(dst, hash), f)
	if err := errors.Join(err, dst.Close(), f.Close()); err != nil {
		return "", err
	}
	digest := fmt.Sprintf("%x", hash.Sum(nil))
	if expectedDigest != "" && digest != expectedDigest {
		return "", fmt.Errorf("candidate SHA-256 %s differs from expected %s", digest, expectedDigest)
	}
	build, err := buildinfo.ReadFile(copyPath)
	if err != nil {
		return "", fmt.Errorf("read candidate build information: %w", err)
	}
	settings := make(map[string]string)
	for _, setting := range build.Settings {
		settings[setting.Key] = setting.Value
	}
	if settings["vcs.revision"] != revision || settings["vcs.modified"] != "false" || settings["vcs"] != "git" {
		return "", fmt.Errorf("require clean Git build at %s, got revision %q (vcs.modified=%q)", revision, settings["vcs.revision"], settings["vcs.modified"])
	}
	if settings["GOOS"] != "linux" || settings["GOARCH"] != runtime.GOARCH {
		return "", fmt.Errorf("candidate targets %s/%s, require linux/%s", settings["GOOS"], settings["GOARCH"], runtime.GOARCH)
	}
	return digest, nil
}

func writeReport(path string, results report) (err error) {
	f, err := os.CreateTemp(filepath.Dir(path), ".durability-report-*")
	if err != nil {
		return err
	}
	encoder := json.NewEncoder(f)
	encoder.SetIndent("", "  ")
	if err = errors.Join(encoder.Encode(results), f.Close()); err == nil {
		err = os.Rename(f.Name(), path)
		if err == nil {
			return nil
		}
	}
	return errors.Join(err, os.Remove(f.Name()))
}

func summarize(cases map[string]outcome, names ...string) outcome {
	for _, name := range names {
		if result := cases[name]; result.Status != "pass" {
			return outcome{Status: result.Status, Detail: name + ": " + result.Detail}
		}
	}
	return outcome{Status: "pass"}
}

func run(ctx context.Context, args []string) (err error) {
	flags := flag.NewFlagSet("durability", flag.ContinueOnError)
	binary := flags.String("binary", "", "absolute path to the candidate binary")
	revision := flags.String("revision", "", "full candidate Git commit SHA")
	fixture := flags.String("fixture", "", "release fixture ID")
	output := flags.String("report", "", "report output path")
	digest := flags.String("sha256", "", "expected candidate SHA-256 (optional)")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 || !filepath.IsAbs(*binary) || !revisionPattern.MatchString(*revision) || *fixture == "" || *output == "" {
		return errors.New("require -binary (absolute), -revision (40 lowercase hex digits), -fixture and -report")
	}
	if *digest != "" && !digestPattern.MatchString(*digest) {
		return errors.New("-sha256 must contain 64 lowercase hex digits")
	}
	if runtime.GOOS != "linux" {
		return errors.New("durability verification requires Linux")
	}
	candidateInfo, err := os.Stat(*binary)
	if err != nil {
		return err
	}
	if existing, err := os.Stat(*output); err == nil {
		if os.SameFile(existing, candidateInfo) {
			return errors.New("-report refers to the candidate binary")
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	work, err := os.MkdirTemp("", "bog-durability-*")
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, os.RemoveAll(work)) }()
	pinned := filepath.Join(work, "bog")
	sum, err := pinBinary(*binary, pinned, *revision, *digest)
	if err != nil {
		return err
	}
	results := report{1, *revision, sum, *fixture, make(map[string]outcome)}
	client := &probeClient
	defer client.CloseIdleConnections()
	for _, check := range []struct {
		name string
		run  func(context.Context, *fixtureServer) error
	}{
		{"sigkill_acknowledged", checkSIGKILL},
		{"torn_suffix", checkTornSuffix},
		{"committed_corruption", checkCorruption},
		{"tied_backfill_replay", checkBackfill},
		{"stale_cursor", checkStaleCursor},
	} {
		dir := filepath.Join(work, check.name)
		s := &fixtureServer{binary: pinned, dir: dir}
		caseErr := os.Mkdir(dir, 0700)
		if caseErr == nil {
			caseCtx, cancel := context.WithTimeout(ctx, 45*time.Second)
			caseErr = check.run(caseCtx, s)
			cancel()
		}
		err = errors.Join(err, results.record(check.name, errors.Join(caseErr, s.stop())))
	}
	proof, diskErr := checkENOSPC(ctx, pinned)
	err = errors.Join(err, results.record("enospc_recovery", diskErr))
	disk := results.Cases["enospc_recovery"]
	disk.Evidence = &proof
	results.Cases["enospc_recovery"] = disk
	results.Cases["enospc"] = disk
	results.Cases["sigkill"] = summarize(results.Cases, "sigkill_acknowledged")
	results.Cases["recovery"] = summarize(results.Cases, "torn_suffix", "committed_corruption", "tied_backfill_replay", "stale_cursor")
	return errors.Join(err, writeReport(*output, results))
}

func (r report) record(name string, err error) error {
	if err == nil {
		r.Cases[name] = outcome{Status: "pass"}
		return nil
	}
	r.Cases[name] = outcome{Status: "fail", Detail: err.Error()}
	return fmt.Errorf("%s: %w", name, err)
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	err := run(ctx, os.Args[1:])
	stop()
	if err != nil && !errors.Is(err, flag.ErrHelp) {
		log.Fatal(err)
	}
}
