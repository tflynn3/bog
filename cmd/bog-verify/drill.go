package main

import (
	"context"
	"errors"
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
	prompt := fmt.Sprintf("restart the restored server now. When it is ready, run kill -USR1 %d\n", os.Getpid())
	if err := writeText(s.out, prompt); err != nil {
		return fmt.Errorf("write restart prompt: %w", err)
	}
	select {
	case <-ctx.Done():
	case <-signals:
	}
	return ctx.Err()
}

func drillBackup(ctx context.Context, opts options, server *client.Client, restart restartBoundary, out io.Writer) (int, error) {
	var beforeRestart func(drillCounts, recoveryPoint) error
	if !opts.json {
		beforeRestart = func(counts drillCounts, point recoveryPoint) error {
			_, err := report(out, fmt.Sprintf("before restart matched: %s %s\n", counts, point))
			return err
		}
	}
	result, err := drillRestore(ctx, opts, server, restart, beforeRestart)
	if opts.json {
		if writeErr := writeDrillResult(out, result); writeErr != nil {
			return exitIncomplete, errors.Join(err, writeErr)
		}
	}
	if err != nil || opts.json {
		return result.ExitStatus, err
	}
	return report(out, fmt.Sprintf("restore drill matched after restart: %s %s\n",
		result.AfterRestart.Counts, result.RecoveryPoint))
}

func drillRestore(ctx context.Context, opts options, server *client.Client, restart restartBoundary, beforeRestart func(drillCounts, recoveryPoint) error) (drillResult, error) {
	result := drillResult{
		SchemaVersion: drillSchemaVersion,
		BeforeRestart: drillPhaseResult{Status: phaseNotRun},
		AfterRestart:  drillPhaseResult{Status: phaseNotRun},
	}
	before, point, status, err := compareRestore(ctx, opts, server, &opts.retry, nil)
	if point != nil {
		result.RecoveryPoint = resultPoint(*point)
	}
	result.BeforeRestart.recordCounts(before)
	if err != nil {
		result.BeforeRestart.Status = phaseFailed
		return result.fail(phaseBeforeRestart, status, fmt.Errorf("before restart: %w", err))
	}
	result.BeforeRestart.Status = phaseMatched

	server.CloseIdleConnections()
	if err := ctx.Err(); err != nil {
		return result.fail(phaseRestartBoundary, exitIncomplete, fmt.Errorf("restart boundary: %w", err))
	}
	if beforeRestart != nil {
		if err := beforeRestart(*result.BeforeRestart.Counts, *result.RecoveryPoint); err != nil {
			return result.fail(phaseBeforeRestart, exitIncomplete, fmt.Errorf("before restart: %w", err))
		}
	}
	if err := restart.WaitForRestart(ctx); err != nil {
		return result.fail(phaseRestartBoundary, exitIncomplete, fmt.Errorf("restart boundary: %w", err))
	}
	if err := ctx.Err(); err != nil {
		return result.fail(phaseRestartBoundary, exitIncomplete, fmt.Errorf("restart boundary: %w", err))
	}
	after, _, status, err := compareRestore(ctx, opts, server, &opts.retry, point)
	result.AfterRestart.recordCounts(after)
	if ctxErr := ctx.Err(); err == nil && ctxErr != nil {
		status, err = exitIncomplete, ctxErr
	}
	if err != nil {
		result.AfterRestart.Status = phaseFailed
		return result.fail(phaseAfterRestart, status, fmt.Errorf("after restart: %w", err))
	}
	result.AfterRestart.Status = phaseMatched
	result.Parity = true
	return result, nil
}
