package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"

	"bog/internal/client"
)

// restartBoundary lets the operator control the endpoint lifecycle. It
// returns once the restored server has been restarted and is ready at the
// original URL, or when the wait fails. Implementations must honor ctx.
type restartBoundary interface {
	WaitForRestart(ctx context.Context) error
}

type signalRestart struct {
	out io.Writer
}

func (s signalRestart) WaitForRestart(ctx context.Context) error {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGUSR1)
	defer signal.Stop(signals)
	if err := ctx.Err(); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(s.out, "restart the restored server now. When it is ready, run kill -USR1 %d\n", os.Getpid()); err != nil {
		return fmt.Errorf("write restart prompt: %w", err)
	}
	select {
	case <-ctx.Done():
	case <-signals:
	}
	return ctx.Err()
}

func drillBackup(ctx context.Context, opts options, server *client.Client, restart restartBoundary, out io.Writer) (int, error) {
	before, point, status, err := compareRestore(ctx, opts, server, &opts.retry, nil)
	if err != nil {
		return status, fmt.Errorf("before restart: %w", err)
	}
	server.CloseIdleConnections()
	if err := ctx.Err(); err != nil {
		return exitIncomplete, fmt.Errorf("restart boundary: %w", err)
	}
	if _, err := fmt.Fprintf(out, "before restart matched: %s %s\n", comparisonCounts(before), formatPoint(point)); err != nil {
		return exitIncomplete, fmt.Errorf("before restart: write result: %w", err)
	}
	if err := restart.WaitForRestart(ctx); err != nil {
		return exitIncomplete, fmt.Errorf("restart boundary: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return exitIncomplete, fmt.Errorf("restart boundary: %w", err)
	}
	after, _, status, err := compareRestore(ctx, opts, server, &opts.retry, &point)
	if err != nil {
		return status, fmt.Errorf("after restart: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return exitIncomplete, fmt.Errorf("after restart: %w", err)
	}
	return report(out, fmt.Sprintf("restore drill matched after restart: %s %s\n",
		comparisonCounts(after), formatPoint(point)))
}
