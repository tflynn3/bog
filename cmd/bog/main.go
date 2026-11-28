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

const defaultRetainedQueryWork = 1 << 30

func parseFlags(args []string, output io.Writer) (config, error) {
	var c config
	fs := flag.NewFlagSet("bog", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	fs.StringVar(&c.dir, "data-dir", ".", "existing data directory")
	fs.StringVar(&c.addr, "addr", "127.0.0.1:8080", "HTTP listen address")
	fs.IntVar(&c.maxResponseBytes, "query-max-response-bytes", httpapi.DefaultQueryMaxResponseBytes, "maximum encoded NDJSON query response bytes")
	fs.IntVar(&c.queryConcurrency, "query-concurrency", httpapi.DefaultQueryConcurrency, "maximum concurrent GET /query requests (503 when full)")
	fs.Uint64Var(&c.queryWorkBudget, "query-work-budget", defaultRetainedQueryWork, "maximum work units per query, including scans, encoding, and response bytes (503 on exhaustion)")
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
	storage, err := openRetained(c.dir)
	if err != nil {
		return fmt.Errorf("open data directory %q: %w", c.dir, err)
	}
	closeStartup := func(cause error) error {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		return errors.Join(cause, storage.close(ctx))
	}
	probe, err := storage.root.Open(".")
	if err != nil {
		return closeStartup(fmt.Errorf("open readiness directory: %w", err))
	}
	ownerInfo, ownerErr := storage.dir.Stat()
	probeInfo, probeErr := probe.Stat()
	if err := errors.Join(ownerErr, probeErr); err != nil {
		return closeStartup(errors.Join(fmt.Errorf("stat readiness directory: %w", err), probe.Close()))
	}
	if !os.SameFile(ownerInfo, probeInfo) {
		return closeStartup(errors.Join(errors.New("readiness directory differs from WAL directory"), probe.Close()))
	}

	addr := c.addr
	if addr == "" {
		addr = ":http"
	}
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(signals)
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return closeStartup(errors.Join(fmt.Errorf("listen on %q: %w", addr, err), probe.Close()))
	}
	capacity := capacityEnabled(listener)
	probeCtx, stopProbes := context.WithCancel(context.Background())
	defer stopProbes()
	handler, api := newReadyHandler(probeCtx, c, storage, probe, capacity)
	storage.startMaintenance(capacity)
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
	var serveErr, storageErr error
	select {
	case <-signals:
	case serveErr = <-served:
	case storageErr = <-storage.fatal:
	}
	storage.stopMaintenance()
	stopProbes()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := server.Shutdown(ctx); err != nil {
		return errors.Join(storageErr, fmt.Errorf("shutdown HTTP server (WAL remains open until exit): %w", err), probe.Close())
	}
	if serveErr == nil {
		serveErr = <-served
	}
	if errors.Is(serveErr, http.ErrServerClosed) {
		serveErr = nil
	}
	if serveErr != nil {
		serveErr = fmt.Errorf("serve HTTP: %w", serveErr)
	}
	apiErr := api.Close(ctx)
	probeErr = probe.Close()
	if ctx.Err() != nil {
		return errors.Join(serveErr, storageErr, apiErr, probeErr, ctx.Err())
	}
	return errors.Join(serveErr, storageErr, apiErr, probeErr, storage.close(ctx))
}

func main() {
	c, err := parseFlags(os.Args[1:], os.Stderr)
	if errors.Is(err, flag.ErrHelp) {
		return
	}
	if err != nil {
		log.Print(err)
		os.Exit(2)
	}
	// Leave resident headroom for sockets, stacks and filesystem activity.
	// An operator-supplied lower Go heap target still takes precedence.
	const heapTarget = 72 << 20
	if debug.SetMemoryLimit(-1) > heapTarget {
		debug.SetMemoryLimit(heapTarget)
	}
	err = run(c)
	if err != nil {
		log.Print(err)
		os.Exit(1)
	}
}
