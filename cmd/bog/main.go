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
	queryConcurrency := flag.Int("query-concurrency", 8, "maximum active queries (excess requests are rejected)")
	queryTimeout := flag.Duration("query-timeout", 10*time.Second, "query execution and response write deadline")
	queryRows := flag.Int64("query-max-rows", 1000000, "maximum query candidate and index visits")
	queryBytes := flag.Int64("query-max-match-bytes", 256<<20, "maximum bytes presented to literal matching")
	queryResultBytes := flag.Int64("query-max-result-bytes", 16<<20, "maximum estimated encoded query response bytes")
	flag.Parse()
	if *queryConcurrency < 1 || *queryTimeout <= 0 || *queryRows < 1 || *queryBytes < 1 || *queryResultBytes < 1 {
		log.Fatal("query limits must be positive")
	}
	queryConfig := httpapi.QueryConfig{Concurrency: *queryConcurrency, Timeout: *queryTimeout, Limits: store.QueryLimits{Rows: *queryRows, MatchBytes: *queryBytes, ResultBytes: *queryResultBytes}}
	s := &store.Store{}
	w, err := wal.Open(*dir, s)
	if err != nil {
		log.Fatal(err)
	}
	server := &http.Server{Addr: *addr, Handler: httpapi.NewWithQueryConfig(s, w, queryConfig), ReadHeaderTimeout: 10 * time.Second, ReadTimeout: 30 * time.Second, WriteTimeout: 30 * time.Second, IdleTimeout: 60 * time.Second, MaxHeaderBytes: 1 << 20}
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
