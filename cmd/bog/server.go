package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"bog/internal/checkpoint"
	"bog/internal/httpapi"
	"bog/internal/readiness"
	"bog/internal/store"
)

func run(c config) error {
	r, err := openRetained(c.dir)
	if err != nil {
		return fmt.Errorf("open data directory %q: %w", c.dir, err)
	}
	probe, err := r.OpenProbeDirectory()
	if err != nil {
		return errors.Join(fmt.Errorf("open readiness directory %q: %w", c.dir, err), r.close())
	}
	addr := c.addr
	if addr == "" {
		addr = ":http"
	}
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return errors.Join(fmt.Errorf("listen on %q: %w", addr, err), probe.Close(), r.close())
	}
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(signals)
	probeCtx, stopProbes := context.WithCancel(context.Background())
	defer stopProbes()

	source := retainedPages{r}
	// Allow bounded concurrent small scans and one full ordinary page.
	limits := httpapi.QueryResourceLimits{
		ResponseBytes: uint64(max(httpapi.DefaultQueryMaxResponseBytes, c.maxResponseBytes)),
		ScanBytes:     144 << 20,
		Captures:      64,
	}
	var api *httpapi.API
	handler := readiness.NewHandlerForDirectory(probeCtx, probe, r, func(appender readiness.Appender) http.Handler {
		api = httpapi.NewWithPageSourceAndQueryResources(source, appender,
			c.maxResponseBytes, c.queryConcurrency, c.queryWorkBudget, limits)
		return api
	})
	if loopbackControl(listener) {
		mux := http.NewServeMux()
		mux.HandleFunc("/_capacity/compaction", r.capacityControl(api))
		mux.Handle("/", handler)
		handler = mux
	}
	var stopping atomic.Bool
	server := &http.Server{
		Handler: http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			if stopping.Load() {
				http.Error(w, "server draining", http.StatusServiceUnavailable)
				return
			}
			if req.Method == http.MethodPost && req.URL.Path == "/ingest" && req.ContentLength <= store.MaxBatch {
				length := req.ContentLength
				if length < 0 {
					length = store.MaxBatch
				}
				// Body growth, decoding scratch and rows before Append
				// transfers the immutable backing into the suffix budget.
				charge, err := r.budget.Reserve(req.Context(), checkpoint.IngestMemoryBytes, uint64(6*length+(64<<10)))
				if err != nil {
					http.Error(w, "busy", http.StatusServiceUnavailable)
					return
				}
				defer charge.Release()
			}
			handler.ServeHTTP(w, req)
		}),
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
		MaxHeaderBytes:    1 << 20,
	}
	served := make(chan error, 1)
	go func() { served <- server.Serve(listener) }()
	var serveErr error
	var serveReturned bool
	select {
	case <-signals:
	case serveErr = <-served:
		serveReturned = true
	case serveErr = <-r.failed:
	}
	stopping.Store(true)
	stopProbes()
	r.stop()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := server.Shutdown(ctx); err != nil {
		return errors.Join(serveErr, fmt.Errorf("drain HTTP (WAL remains locked until exit): %w", err))
	}
	if !serveReturned {
		if err := <-served; !errors.Is(err, http.ErrServerClosed) {
			serveErr = errors.Join(serveErr, err)
		}
	}
	if err := api.Close(ctx); err != nil {
		return errors.Join(serveErr, fmt.Errorf("drain query captures (WAL remains locked until exit): %w", err))
	}
	if err := errors.Join(r.drain(ctx), probe.Close()); err != nil {
		return errors.Join(serveErr, fmt.Errorf("drain storage (WAL remains locked until exit): %w", err))
	}
	return errors.Join(serveErr, r.close())
}

func loopbackControl(listener net.Listener) bool {
	tcp, ok := listener.Addr().(*net.TCPAddr)
	return os.Getenv("BOG_CAPACITY_CONTROL") == "1" && ok && tcp.IP.To4() != nil && tcp.IP.IsLoopback()
}
