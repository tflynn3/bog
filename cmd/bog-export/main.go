package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"
	"time"

	"bog/internal/client"
)

const (
	exitOK         = 0
	exitIncomplete = 1
	exitUsage      = 2

	// Help, usage errors, and a last diagnostic after an export has
	// already been canceled get a short best-effort reporting window.
	diagnosticWindow = 200 * time.Millisecond
)

func main() {
	if len(os.Args) == 2 && os.Args[1] == outputWorkerArg {
		os.Exit(runOutputWorker())
	}
	// Report closed pipelines (including worker channels) as write errors.
	signal.Ignore(syscall.SIGPIPE)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	status := run(ctx, os.Args[1:], os.Stdout, os.Stderr)
	stop()
	os.Exit(status)
}

func run(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	opts, err := parseOptions(args)
	if err != nil {
		reportCtx, cancel := context.WithTimeout(ctx, diagnosticWindow)
		defer cancel()
		if errors.Is(err, flag.ErrHelp) {
			return writeDiagnostic(reportCtx, stderr, exitOK, []byte(usage))
		}
		return writeDiagnostic(reportCtx, stderr, exitUsage, fmt.Appendf(nil, "bog-export: %v (see bog-export -h)\n", err))
	}

	ctx, cancel := context.WithTimeout(ctx, opts.timeout)
	defer cancel()
	err = outputCancellation(ctx, export(ctx, opts, stdout))
	if err == nil {
		return exitOK
	}
	reportCtx := ctx
	if outputCancellation(ctx, nil) != nil {
		// The export has stopped. Allow one brief diagnostic attempt even
		// after cancellation so an available stderr can explain the exit.
		var stop context.CancelFunc
		reportCtx, stop = context.WithTimeout(context.WithoutCancel(ctx), diagnosticWindow)
		defer stop()
	}
	return writeDiagnostic(reportCtx, stderr, exitIncomplete, fmt.Appendf(nil, "bog-export: export incomplete: %v\n", err))
}

func writeDiagnostic(ctx context.Context, stderr io.Writer, status int, message []byte) int {
	output, err := openOutput(ctx, stderr)
	if err != nil {
		return exitIncomplete
	}
	writeErr := output.WriteRecord(message)
	if err := errors.Join(writeErr, output.Close()); err != nil {
		return exitIncomplete
	}
	return status
}

func export(ctx context.Context, opts options, out io.Writer) error {
	server, err := client.New(opts.baseURL, nil)
	if err != nil {
		return fmt.Errorf("configure query client: %w", err)
	}
	defer server.CloseIdleConnections()
	server.MaxPageBytes = opts.maxPageBytes
	walk, err := server.NewBoundedWalkWithContext(ctx, opts.query, opts.maxPages)
	if err != nil {
		return fmt.Errorf("prepare query walk: %w", err)
	}
	return exportPages(ctx, walk, opts.retry, out)
}

func exportPages(ctx context.Context, walk *client.ContextBoundedWalk, retry client.PageRetry, out io.Writer) (resultErr error) {
	var output *recordOutput
	for pageNumber := 1; ; pageNumber++ {
		page, err := walk.NextPageWithRetry(retry)
		if err == io.EOF {
			return outputCancellation(ctx, nil)
		}
		if err != nil {
			return fmt.Errorf("get query page %d: %w", pageNumber, err)
		}
		if len(page.Rows) > 0 && output == nil {
			output, err = openOutput(ctx, out)
			if err != nil {
				return fmt.Errorf("prepare stdout: %w", err)
			}
			defer func() {
				if err := output.Close(); err != nil {
					resultErr = errors.Join(resultErr, fmt.Errorf("close stdout output: %w", err))
				}
			}()
		}
		for i, row := range page.Rows {
			if err := writeRow(output, row); err != nil {
				return fmt.Errorf("write stdout (page %d, row %d): %w", pageNumber, i+1, err)
			}
		}
	}
}

func writeRow(out *recordOutput, row client.Row) error {
	if err := outputCancellation(out.ctx, nil); err != nil {
		return err
	}
	record, err := json.Marshal(row)
	if err != nil {
		return fmt.Errorf("encode NDJSON record: %w", err)
	}
	return out.WriteRecord(append(record, '\n'))
}
