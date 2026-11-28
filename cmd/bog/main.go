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
	signals, stopSignals := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stopSignals()
	rt, err := openRuntime(signals, c.dir)
	if err != nil {
		return fmt.Errorf("open data directory %q: %w", c.dir, err)
	}
	shutdownBudget := 30 * time.Second
	unwind := func(cause error) error {
		ctx, cancel := context.WithTimeout(context.Background(), shutdownBudget)
		defer cancel()
		return errors.Join(cause, rt.close(ctx))
	}
	probe, err := rt.wal.OpenProbeDirectory()
	if err != nil {
		return unwind(fmt.Errorf("open readiness directory: %w", err))
	}
	addr := c.addr
	if addr == "" {
		addr = ":http"
	}
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return unwind(errors.Join(fmt.Errorf("listen on %q: %w", addr, err), probe.Close()))
	}
	probeCtx, stopProbes := context.WithCancel(context.Background())
	defer stopProbes()
	handler, api := newReadyHandler(probeCtx, c, rt, probe, listener.Addr())
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
	case <-signals.Done():
	case serveErr = <-served:
	case storageErr = <-rt.fatal:
	}
	stopProbes()
	rt.stop()
	probeClosed := make(chan error, 1)
	go func() { probeClosed <- probe.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), shutdownBudget)
	defer cancel()
	if err := server.Shutdown(ctx); err != nil {
		return errors.Join(storageErr, fmt.Errorf("shutdown HTTP server (WAL remains open until exit): %w", err))
	}
	if serveErr == nil {
		serveErr = <-served
	}
	if errors.Is(serveErr, http.ErrServerClosed) {
		serveErr = nil
	}
	if err := api.Close(ctx); err != nil {
		return errors.Join(serveErr, storageErr, fmt.Errorf("drain query captures (WAL remains open until exit): %w", err))
	}
	select {
	case err := <-probeClosed:
		if err != nil {
			return errors.Join(serveErr, storageErr, fmt.Errorf("close readiness directory: %w", err))
		}
	case <-ctx.Done():
		return errors.Join(serveErr, storageErr, ctx.Err())
	}
	if storageErr != nil {
		return errors.Join(serveErr, storageErr)
	}
	return errors.Join(serveErr, rt.close(ctx))
}

func main() {
	debug.SetMemoryLimit(96 << 20)
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
