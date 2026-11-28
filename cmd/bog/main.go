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
	"bog/internal/readiness"
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

func run(c config) (err error) {
	r, err := openRetained(context.Background(), c.dir)
	if err != nil {
		return fmt.Errorf("open data directory %q: %w", c.dir, err)
	}
	// Drain errors deliberately keep the owner and its lock until exit.
	release := true
	var probe *os.File
	defer func() {
		if release {
			if probe != nil {
				err = errors.Join(err, probe.Close())
			}
			err = errors.Join(err, r.close(), r.owner.Close())
		}
	}()
	drain := func(ctx context.Context, cause error) error {
		if drainErr := r.drain(ctx); drainErr != nil {
			release = false
			return errors.Join(cause, fmt.Errorf("drain storage: %w", drainErr))
		}
		return cause
	}
	startupFailure := func(cause error) error {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		return drain(ctx, cause)
	}
	probe, err = r.owner.Root().Open(".")
	if err != nil {
		return startupFailure(fmt.Errorf("open pinned readiness directory: %w", err))
	}
	storageInfo, storageErr := r.owner.Directory().Stat()
	probeInfo, probeErr := probe.Stat()
	if err := errors.Join(storageErr, probeErr); err != nil {
		return startupFailure(fmt.Errorf("check pinned readiness directory: %w", err))
	}
	if !os.SameFile(storageInfo, probeInfo) {
		return startupFailure(errors.New("readiness directory does not match the WAL directory"))
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
		return startupFailure(fmt.Errorf("listen on %q: %w", addr, err))
	}
	probeCtx, stopProbes := context.WithCancel(context.Background())
	defer stopProbes()
	source := &retainedPages{storage: r}
	resources := httpapi.DefaultQueryResourceLimits()
	resources.ScanBytes = 144 << 20
	resources.ResponseBytes = max(resources.ResponseBytes, uint64(c.maxResponseBytes))
	var api *httpapi.API
	handler := readiness.NewHandlerForDirectory(probeCtx, probe, r, func(appender readiness.Appender) http.Handler {
		api = httpapi.NewWithPageSourceAndQueryResources(source, appender, c.maxResponseBytes, c.queryConcurrency, c.queryWorkBudget, resources)
		return api
	})
	if addr, ok := listener.Addr().(*net.TCPAddr); ok && addr.IP.To4() != nil && addr.IP.IsLoopback() && os.Getenv("BOG_CAPACITY_CONTROL") == "1" {
		mux := http.NewServeMux()
		mux.Handle("/", handler)
		mux.HandleFunc("/_capacity/compaction", func(w http.ResponseWriter, req *http.Request) {
			r.control(api, w, req)
		})
		handler = mux
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
	r.stopAdmission()
	stopProbes()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := server.Shutdown(ctx); err != nil {
		release = false
		return fmt.Errorf("shutdown HTTP server (WAL remains open until exit): %w", err)
	}
	if serveErr == nil {
		serveErr = <-served
	}
	if errors.Is(serveErr, http.ErrServerClosed) {
		serveErr = nil
	}
	if err := api.Close(ctx); err != nil {
		release = false
		return errors.Join(serveErr, fmt.Errorf("drain query captures: %w", err))
	}
	return drain(ctx, serveErr)
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
		debug.SetMemoryLimit(72 << 20)
	}
	err = run(c)
	if err != nil {
		log.Print(err)
		os.Exit(1)
	}
}
