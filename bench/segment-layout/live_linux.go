package main

import (
	"context"
	"crypto/sha256"
	"debug/buildinfo"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"syscall"
	"time"

	"bog/verify/listener"
)

func pinBinary(source, target, revision string) (digest string, err error) {
	if sha, err := hex.DecodeString(revision); err != nil || len(sha) != 20 {
		return "", errors.New("require a full candidate Git revision")
	}
	src, err := os.OpenFile(source, os.O_RDONLY|syscall.O_NONBLOCK, 0)
	if err != nil {
		return "", err
	}
	defer func() { err = errors.Join(err, src.Close()) }()
	stat, err := src.Stat()
	if err != nil {
		return "", err
	}
	if !stat.Mode().IsRegular() {
		return "", errors.New("candidate binary must be a regular file")
	}
	dst, err := os.OpenFile(target, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0500)
	if err != nil {
		return "", err
	}
	hash := sha256.New()
	_, copyErr := io.Copy(io.MultiWriter(dst, hash), src)
	if err := errors.Join(copyErr, dst.Close()); err != nil {
		return "", err
	}
	info, err := buildinfo.ReadFile(target)
	if err != nil {
		return "", err
	}
	settings := make(map[string]string)
	for _, entry := range info.Settings {
		settings[entry.Key] = entry.Value
	}
	if info.Path != "bog/cmd/bog" || settings["vcs"] != "git" || settings["vcs.revision"] != revision ||
		settings["vcs.modified"] != "false" || settings["GOOS"] != "linux" || settings["GOARCH"] != runtime.GOARCH {
		return "", fmt.Errorf("candidate must be clean bog/cmd/bog linux/%s at %s", runtime.GOARCH, revision)
	}
	return fmt.Sprintf("%x", hash.Sum(nil)), nil
}

func measureLive(ctx context.Context, cfg config, dir string, input fixtureInput, denominators map[string]uint64) (liveReport, error) {
	binary := filepath.Join(dir, "candidate")
	digest, err := pinBinary(cfg.binary, binary, cfg.binaryRevision)
	if err != nil {
		return liveReport{}, err
	}
	result := liveReport{Status: "measured", Revision: cfg.binaryRevision, BinarySHA256: digest}
	for _, burst := range input.backfills() {
		data := filepath.Join(dir, "server-"+burst.name)
		if err := os.Mkdir(data, 0700); err != nil {
			return liveReport{}, err
		}
		trialCtx, cancel := context.WithTimeout(ctx, cfg.timeout)
		sample, unavailable, err := liveTrial(trialCtx, binary, data, input, burst, denominators[burst.name], cfg.cold)
		cancel()
		if err != nil {
			return liveReport{}, fmt.Errorf("%s live compaction: %w", burst.name, err)
		}
		if err := os.RemoveAll(data); err != nil {
			return liveReport{}, err
		}
		if unavailable != "" {
			if len(result.Measurements) != 0 {
				return liveReport{}, errors.New("candidate control disappeared between trials")
			}
			result.Status, result.Reason = "unavailable", unavailable
			break
		}
		result.Measurements = append(result.Measurements, sample)
	}
	return result, nil
}

func liveTrial(ctx context.Context, binary, dir string, input fixtureInput, burst backfill, denominator uint64, queryParity bool) (publicationMeasurement, string, error) {
	if err := withSeededPublication(ctx, dir, input, nil); err != nil {
		return publicationMeasurement{}, "", err
	}
	body, _, err := encodeBatch(burst.batch)
	if err != nil {
		return publicationMeasurement{}, "", err
	}
	count := len(input.rows) + len(burst.batch)
	rows := input.withBackfill(burst)
	var sample publicationMeasurement
	var unavailable string
	err = withCandidate(ctx, binary, dir, func(s liveService) (err error) {
		initial, status, err := s.control(ctx, http.MethodGet, "status", len(input.rows))
		if err != nil {
			return err
		}
		if status == http.StatusNotFound || status == http.StatusNotImplemented {
			unavailable = fmt.Sprintf("loopback compaction control returned HTTP %d", status)
			return nil
		}
		if status != http.StatusOK || initial.State == "running" || *initial.CoveredRows != len(input.rows) ||
			*initial.CommittedRows != len(input.rows) {
			return fmt.Errorf("unexpected initial compaction state (HTTP %d)", status)
		}
		walPath := filepath.Join(dir, "bog.wal")
		walBefore, err := os.Stat(walPath)
		if err != nil {
			return err
		}
		status, response, err := s.request(ctx, http.MethodPost, "/ingest", body)
		if err != nil {
			return err
		}
		if status != http.StatusNoContent || len(response) != 0 {
			return fmt.Errorf("backfill acknowledgment HTTP %d: %.200s", status, response)
		}
		walAfter, err := os.Stat(walPath)
		if err != nil {
			return err
		}
		if walAfter.Size() <= walBefore.Size() || uint64(walAfter.Size()-walBefore.Size()) != denominator {
			return errors.New("acknowledged backfill WAL bytes differ from component workload")
		}
		before, err := processIO(strconv.Itoa(s.pid), "write_bytes")
		if err != nil {
			return err
		}
		start := time.Now()
		if err := s.advance(ctx, count, *initial.Generation); err != nil {
			return err
		}
		elapsed := time.Since(start)
		after, err := processIO(strconv.Itoa(s.pid), "write_bytes")
		if err != nil {
			return err
		}
		if after < before {
			return errors.New("server write_bytes counter decreased")
		}
		if queryParity {
			if err := s.checkPublishedQueries(ctx, rows); err != nil {
				return err
			}
		}
		if err := s.requireListener(); err != nil {
			return err
		}
		sample = publicationMeasurement{
			Scenario: burst.name, CoveredRows: count, ElapsedNS: elapsed.Nanoseconds(), HTTPQueryParity: queryParity,
			WriteAmplification: writeAmplification(after-before, denominator, liveBoundary),
		}
		return nil
	})
	if err != nil || unavailable != "" {
		return sample, unavailable, err
	}
	return sample, "", verifyPublished(dir, rows)
}

func withCandidate(ctx context.Context, binary, dir string, run func(liveService) error) (err error) {
	reserved, err := net.ListenTCP("tcp4", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		return err
	}
	addr := reserved.Addr().(*net.TCPAddr)
	if err := reserved.Close(); err != nil {
		return err
	}
	childCtx, stop := context.WithCancel(ctx)
	defer stop()
	cmd := exec.CommandContext(childCtx, binary, "-addr", addr.String(), "-data-dir", dir)
	cmd.Env = append(cmd.Environ(), "BOG_CAPACITY_CONTROL=1")
	cmd.WaitDelay = 5 * time.Second
	cmd.Cancel = func() error {
		if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
			return err
		}
		// A clean exit after our shutdown request is successful.
		return os.ErrProcessDone
	}
	var log childLog
	cmd.Stderr, cmd.Stdout = &log, &log
	if err := cmd.Start(); err != nil {
		return err
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	defer func() {
		stop()
		waitErr := <-done
		if failure := errors.Join(err, waitErr, ctx.Err()); failure != nil {
			err = fmt.Errorf("candidate: %w %.8192s", failure, string(log))
		}
	}()
	transport := &http.Transport{}
	defer transport.CloseIdleConnections()
	s := liveService{
		client: &http.Client{
			Transport: transport, Timeout: 5 * time.Second,
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		},
		base: "http://" + addr.String(), pid: cmd.Process.Pid, port: addr.Port,
	}
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	startup, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	for {
		owned, err := listener.Owns(s.pid, s.port)
		if err != nil {
			return err
		}
		if owned {
			status, _, err := s.request(startup, http.MethodGet, "/healthz", nil)
			if err != nil {
				return fmt.Errorf("startup health probe: %w", err)
			}
			if status != http.StatusOK {
				return fmt.Errorf("startup health probe HTTP %d", status)
			}
			if err := s.requireListener(); err != nil {
				return err
			}
			return run(s)
		}
		select {
		case waitErr := <-done:
			done <- waitErr // The deferred cleanup owns the final result.
			return errors.New("candidate exited before startup")
		case <-startup.Done():
			return fmt.Errorf("candidate startup: %w", startup.Err())
		case <-ticker.C:
		}
	}
}

func (s liveService) requireListener() error {
	owned, err := listener.Owns(s.pid, s.port)
	if err != nil {
		return err
	}
	if !owned {
		return errors.New("candidate lost loopback listener")
	}
	return nil
}
