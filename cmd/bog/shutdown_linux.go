package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func run(c config) error {
	s, err := openStorage(context.Background(), c.dir)
	if err != nil {
		return fmt.Errorf("open data directory %q: %w", c.dir, err)
	}
	probe, err := openProbe(s)
	if err != nil {
		return errors.Join(fmt.Errorf("open readiness directory: %w", err), closeStartup(s))
	}
	addr := c.addr
	if addr == "" {
		addr = ":http"
	}
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return errors.Join(fmt.Errorf("listen on %q: %w", addr, err), probe.Close(), closeStartup(s))
	}
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(signals)
	probeCtx, stopProbes := context.WithCancel(context.Background())
	defer stopProbes()
	handler, api := newReadyHandler(probeCtx, c, s, probe)
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
	var cause, serveErr error
	serveReturned := false
	select {
	case <-signals:
	case cause = <-s.fatal:
	case serveErr = <-served:
		serveReturned = true
	}

	s.stop()
	stopProbes()
	ctx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	if err := drainServer(ctx, server, api, s, probe); err != nil {
		return errors.Join(cause, serveErr, err)
	}
	if !serveReturned {
		serveErr = <-served
	}
	if s.fenced != nil {
		cause = s.fenced
	}
	if serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
		cause = errors.Join(cause, fmt.Errorf("serve HTTP: %w", serveErr))
	}
	return errors.Join(cause, s.log.Close())
}

func closeStartup(s *storageOwner) error {
	s.stop()
	ctx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	if err := s.drain(ctx); err != nil {
		return err
	}
	return s.log.Close()
}

type httpDrainer interface {
	Shutdown(context.Context) error
}

type captureDrainer interface {
	Close(context.Context) error
}

type storageDrainer interface {
	drain(context.Context) error
}

func drainServer(ctx context.Context, server httpDrainer, api captureDrainer, s storageDrainer, probe io.Closer) error {
	if err := server.Shutdown(ctx); err != nil {
		return fmt.Errorf("shutdown HTTP server: %w", err)
	}
	if err := api.Close(ctx); err != nil {
		return fmt.Errorf("drain query captures: %w", err)
	}
	if err := s.drain(ctx); err != nil {
		return fmt.Errorf("drain retained storage: %w", err)
	}
	if err := probe.Close(); err != nil {
		return fmt.Errorf("close readiness directory: %w", err)
	}
	return nil
}
