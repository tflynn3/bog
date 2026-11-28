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
	storage, err := openRetained(context.Background(), c.dir)
	if err != nil {
		return fmt.Errorf("open data directory %q: %w", c.dir, err)
	}
	closeStartup := func(cause error) error {
		return errors.Join(cause, storage.closeStorage(), storage.dir.Close())
	}
	addr := c.addr
	if addr == "" {
		addr = ":http"
	}
	probe, err := storage.dir.OpenProbe()
	if err != nil {
		return closeStartup(fmt.Errorf("open pinned readiness directory: %w", err))
	}
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return closeStartup(errors.Join(fmt.Errorf("listen on %q: %w", addr, err), probe.Close()))
	}
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(signals)
	probeCtx, stopProbes := context.WithCancel(context.Background())
	defer stopProbes()
	handler, api := newReadyHandler(probeCtx, c, storage, probe)
	if bound, ok := listener.Addr().(*net.TCPAddr); ok && bound.IP.To4() != nil && bound.IP.IsLoopback() && os.Getenv("BOG_CAPACITY_CONTROL") == "1" {
		storage.manual = true
		handler = storage.capacityControl(handler, api)
	}
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
	case <-signals:
	case serveErr = <-served:
	}
	stopProbes()
	storage.stopMaintenance()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := server.Shutdown(ctx); err != nil {
		// Keep the WAL locked while handlers might still be using it.
		return fmt.Errorf("shutdown HTTP server (WAL remains open until exit): %w", err)
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
	// File.Close can wait on an in-flight Fstatfs. The descriptor is
	// independent of the WAL lease, and the canceled probe cannot start
	// another sample. Keep a stalled filesystem out of the storage drain.
	probeClosed := make(chan error, 1)
	go func() { probeClosed <- probe.Close() }()
	if err := api.Close(ctx); err != nil {
		return errors.Join(serveErr, fmt.Errorf("drain HTTP captures (WAL remains locked until exit): %w", err))
	}
	storageDrained := make(chan error, 1)
	go func() { storageDrained <- storage.drain(ctx) }()
	select {
	case err = <-storageDrained:
	case <-ctx.Done():
		err = ctx.Err()
	}
	if err != nil {
		return errors.Join(serveErr, fmt.Errorf("drain storage (WAL remains locked until exit): %w", err))
	}
	err = storage.dir.Close()
	select {
	case probeErr := <-probeClosed:
		err = errors.Join(err, probeErr)
	default:
	}
	return errors.Join(serveErr, err)
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
	if _, configured := os.LookupEnv("GOMEMLIMIT"); !configured {
		// Leave headroom for stacks, metadata and file-backed kernel memory.
		debug.SetMemoryLimit(88 << 20)
	}
	err = run(c)
	if err != nil {
		log.Print(err)
		os.Exit(1)
	}
}
