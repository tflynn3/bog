package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"bog/internal/httpapi"
	"bog/internal/store"
	"bog/internal/wal"
)

func main() {
	dir := flag.String("data-dir", ".", "existing data directory")
	addr := flag.String("addr", "127.0.0.1:8080", "HTTP listen address")
	maxResponseBytes := flag.Int("query-max-response-bytes", httpapi.DefaultQueryMaxResponseBytes, "maximum encoded NDJSON query response bytes")
	queryConcurrency := flag.Int("query-concurrency", httpapi.DefaultQueryConcurrency, "maximum concurrent GET /query requests (503 when full)")
	queryWorkBudget := flag.Uint64("query-work-budget", httpapi.DefaultQueryWorkBudget, "maximum work units per query (503 on exhaustion, charges include candidate rows and decoded bytes)")
	flag.Parse()
	if *maxResponseBytes <= 0 {
		log.Fatal("query-max-response-bytes must be positive")
	}
	if *queryConcurrency <= 0 {
		log.Fatal("query-concurrency must be positive")
	}
	if *queryWorkBudget == 0 {
		log.Fatal("query-work-budget must be positive")
	}
	s := &store.Store{}
	w, err := wal.Open(*dir, s)
	if err != nil {
		log.Fatal(err)
	}
	server := &http.Server{Addr: *addr, Handler: httpapi.NewWithQueryLimits(s, w, *maxResponseBytes, *queryConcurrency, *queryWorkBudget), ReadHeaderTimeout: 10 * time.Second, ReadTimeout: 30 * time.Second, WriteTimeout: 30 * time.Second, IdleTimeout: 60 * time.Second, MaxHeaderBytes: 1 << 20}
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(signals)
	served := make(chan error, 1)
	go func() { served <- server.ListenAndServe() }()
	var serveErr error
	select {
	case <-signals:
	case serveErr = <-served:
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := server.Shutdown(ctx); err != nil {
		// Do not close the WAL or release the lock while handlers may be active.
		log.Printf("shutdown: %v", err)
		os.Exit(1)
	}
	if err := w.Close(); err != nil {
		log.Fatal(err)
	}
	if serveErr != nil && serveErr != http.ErrServerClosed {
		log.Fatal(serveErr)
	}
}
