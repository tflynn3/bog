package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"runtime/debug"
	"syscall"
	"time"

	"bog/internal/httpapi"
)

type config struct {
	dir, addr                          string
	maxResponseBytes, queryConcurrency int
	queryWorkBudget                    uint64
}

func parseFlags(args []string, output io.Writer) (config, error) {
	var c config
	fs := flag.NewFlagSet("bog", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	fs.StringVar(&c.dir, "data-dir", ".", "existing data directory")
	fs.StringVar(&c.addr, "addr", "127.0.0.1:8080", "HTTP listen address")
	fs.IntVar(&c.maxResponseBytes, "query-max-response-bytes", httpapi.DefaultQueryMaxResponseBytes, "maximum encoded NDJSON query response bytes")
	fs.IntVar(&c.queryConcurrency, "query-concurrency", httpapi.DefaultQueryConcurrency, "maximum concurrent GET /query requests (503 when full)")
	fs.Uint64Var(&c.queryWorkBudget, "query-work-budget", httpapi.DefaultQueryWorkBudget, "maximum work units per query, including scans, encoding, and response bytes (503 on exhaustion)")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			fs.SetOutput(output)
			fs.Usage()
		}
		return c, err
	}
	if fs.NArg() != 0 {
		fs.SetOutput(output)
		fs.Usage()
		return c, fmt.Errorf("unexpected argument %q", fs.Arg(0))
	}
	if c.maxResponseBytes <= 0 {
		return c, errors.New("query-max-response-bytes must be positive")
	}
	if c.queryConcurrency <= 0 {
		return c, errors.New("query-concurrency must be positive")
	}
	if c.queryWorkBudget == 0 {
		return c, errors.New("query-work-budget must be positive")
	}
	return c, nil
}

func run(c config) error {
	startup, cancelStartup := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancelStartup()
	s, err := openRetained(startup, c.dir)
	if err != nil {
		return fmt.Errorf("open data directory %q: %w", c.dir, err)
	}
	closeUnstarted := func(err error) error {
		return errors.Join(err, s.catalog.Close(), s.owner.Close())
	}
	probeDir, err := s.readyDirectory()
	if err != nil {
		return closeUnstarted(fmt.Errorf("open readiness directory: %w", err))
	}
	addr := c.addr
	if addr == "" {
		addr = ":http"
	}
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return closeUnstarted(errors.Join(fmt.Errorf("listen on %q: %w", addr, err), probeDir.Close()))
	}
	s.startMaintenance(os.Getenv("BOG_CAPACITY_CONTROL") == "1" && loopback(listener))
	probeCtx, stopProbes := context.WithCancel(context.Background())
	handler, api := newReadyHandler(probeCtx, c, s, probeDir)
	server := &http.Server{
		Handler:           handler,
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
		MaxHeaderBytes:    1 << 20,
	}
	served := make(chan error, 1)
	go func() { served <- server.Serve(listener) }()
	var serveErr error
	select {
	case <-startup.Done():
	case serveErr = <-served:
	}
	s.stopMaintenance()
	stopProbes()
	probeErr := probeDir.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := server.Shutdown(ctx); err != nil {
		return errors.Join(probeErr, fmt.Errorf("drain HTTP: %w (WAL lock held until exit)", err))
	}
	if serveErr == nil {
		serveErr = <-served
	}
	if errors.Is(serveErr, http.ErrServerClosed) {
		serveErr = nil
	} else if serveErr != nil {
		serveErr = fmt.Errorf("serve HTTP: %w", serveErr)
	}
	if err := api.Close(ctx); err != nil {
		return errors.Join(serveErr, probeErr, fmt.Errorf("drain query captures: %w (WAL lock held until exit)", err))
	}
	if err := s.drain(ctx); err != nil {
		return errors.Join(serveErr, probeErr, fmt.Errorf("drain retained storage: %w (WAL lock held until exit)", err))
	}
	return errors.Join(serveErr, probeErr, s.owner.Close())
}

func main() {
	// Keep transient decode and compaction garbage from accumulating near
	// the deployment limit when the default Go GC targets a growing heap.
	if _, set := os.LookupEnv("GOMEMLIMIT"); !set {
		debug.SetMemoryLimit(88 << 20)
	}
	c, err := parseFlags(os.Args[1:], os.Stderr)
	if errors.Is(err, flag.ErrHelp) {
		return
	}
	if err != nil {
		log.Print(err)
		os.Exit(2)
	}
	err = run(c)
	if err != nil {
		log.Print(err)
		os.Exit(1)
	}
}
