package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"
)

func run(ctx context.Context, args []string) (err error) {
	flags := flag.NewFlagSet("durability", flag.ContinueOnError)
	binary := flags.String("binary", "", "absolute path to the pinned Bog executable")
	revision := flags.String("revision", "", "full candidate commit SHA")
	fixture := flags.String("fixture", "", "fixture identifier")
	output := flags.String("report", "", "JSON evidence output path")
	benchmark := flags.Bool("benchmark", false, "measure the seeded 128,000-row fixture")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *output == "" || flags.NArg() != 0 {
		return errors.New("require -report and no positional arguments")
	}
	r, err := newReport(*binary, *revision, *fixture)
	if err != nil {
		return err
	}
	if out, statErr := os.Stat(*output); statErr == nil {
		source, err := os.Stat(*binary)
		if err != nil {
			return err
		}
		if os.SameFile(out, source) {
			return errors.New("-report cannot replace the candidate binary")
		}
	} else if !errors.Is(statErr, os.ErrNotExist) {
		return statErr
	}

	defer func() { err = errors.Join(err, r.write(*output)) }()
	root, err := os.MkdirTemp("", "bog-durability-")
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, os.RemoveAll(root)) }()
	if *benchmark {
		ctx, cancel := context.WithTimeout(ctx, 10*time.Minute)
		defer cancel()
		diag := newCaseDiagnostics()
		r.Diagnostics["benchmark"] = diag
		proof, err := runCase(ctx, *binary, filepath.Join(root, "benchmark"), benchmarkCase, diag)
		return r.record("benchmark", proof, err)
	}
	for _, c := range []struct {
		name  string
		probe func(context.Context, *server) (any, error)
	}{{"sigkill", sigkillCase}, {"recovery", recoveryCase}} {
		caseCtx, cancel := context.WithTimeout(ctx, time.Minute)
		diag := newCaseDiagnostics()
		r.Diagnostics[c.name] = diag
		proof, caseErr := runCase(caseCtx, *binary, filepath.Join(root, c.name), c.probe, diag)
		cancel()
		err = errors.Join(err, r.record(c.name, proof, caseErr))
	}
	caseCtx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	diag := newCaseDiagnostics()
	r.Diagnostics["enospc"] = diag
	proof, caseErr := diskCase(caseCtx, *binary, root, diag)
	return errors.Join(err, r.record("enospc", proof, caseErr))
}

func runCase(ctx context.Context, binary, dir string, probe func(context.Context, *server) (any, error), diag *caseDiagnostics) (_ any, err error) {
	if err := os.Mkdir(dir, 0700); err != nil {
		return nil, err
	}
	s, err := newServer(binary, dir)
	if err != nil {
		return nil, err
	}
	s.diagnostics = diag
	defer func() { err = errors.Join(err, s.close()) }()
	if err := s.start(ctx); err != nil {
		return nil, fmt.Errorf("start candidate: %w", err)
	}
	return probe(ctx, s)
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	err := run(ctx, os.Args[1:])
	stop()
	if err != nil && !errors.Is(err, flag.ErrHelp) {
		log.Fatal(err)
	}
}
