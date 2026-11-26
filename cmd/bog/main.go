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
	config := httpapi.DefaultQueryConfig()
	flag.IntVar(&config.Concurrency, "query-concurrency", config.Concurrency, "maximum active queries")
	flag.Int64Var(&config.Limits.Work, "query-work-budget", config.Limits.Work, "maximum query work units")
	flag.Int64Var(&config.Limits.Rows, "query-max-rows", config.Limits.Rows, "maximum candidate rows examined")
	flag.Int64Var(&config.Limits.MatchBytes, "query-max-match-bytes", config.Limits.MatchBytes, "maximum bytes presented to literal matching")
	flag.Int64Var(&config.Limits.ResultBytes, "query-max-result-bytes", config.Limits.ResultBytes, "maximum estimated response bytes")
	flag.DurationVar(&config.Timeout, "query-timeout", config.Timeout, "maximum query duration")
	flag.Parse()
	if config.Concurrency < 1 || config.Timeout <= 0 || config.Limits.Work <= 0 || config.Limits.Rows <= 0 || config.Limits.MatchBytes <= 0 || config.Limits.ResultBytes <= 0 {
		log.Fatal("query limits must be positive")
	}
	s := &store.Store{}
	w, err := wal.Open(*dir, s)
	if err != nil {
		log.Fatal(err)
	}
	server := &http.Server{Addr: *addr, Handler: httpapi.NewWithQueryConfig(s, w, config), ReadHeaderTimeout: 10 * time.Second, ReadTimeout: 30 * time.Second, WriteTimeout: 30 * time.Second, IdleTimeout: 60 * time.Second, MaxHeaderBytes: 1 << 20}
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
