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
		os.Exit(runOutputWorker(os.Stdout))
	}
	// Report closed pipelines (including worker channels) as write errors.
	signal.Ignore(syscall.SIGPIPE)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	status := run(ctx, os.Args[1:], os.Stdout, os.Stderr)
	stop()
	os.Exit(status)
}

func run(ctx context.Context, args []string, stdout, stderr io.Writer) (status int) {
	opts, err := parseOptions(args)
	if err != nil {
		reportCtx, cancel := context.WithTimeout(ctx, diagnosticWindow)
		defer cancel()
		if errors.Is(err, flag.ErrHelp) {
			return reportStatus(exitOK, writeDiagnostic(reportCtx, stderr, []byte(usage)))
		}
		return reportStatus(exitUsage, writeDiagnostic(reportCtx, stderr,
			fmt.Appendf(nil, "bog-export: %v (see bog-export -h)\n", err)))
	}

	ctx, cancel := context.WithTimeout(ctx, opts.timeout)
	defer cancel()

	// Prepare stderr before stdout so a partial startup failure can still
	// be reported. Do not resolve or exec another worker after the query starts.
	report := func(ctx context.Context, message []byte) error {
		return writeDiagnostic(ctx, stderr, message)
	}
	if stderr == os.Stderr {
		worker, err := openInheritedWorker(ctx, os.Stderr)
		if err != nil {
			// There is no safe inherited writer for reporting this failure.
			return exitIncomplete
		}
		report = worker.writeRecord
		defer func() { status = reportStatus(status, worker.close()) }()
	}

	err = outputCancellation(ctx, export(ctx, opts, stdout))
	if err == nil {
		return exitOK
	}
	reportCtx := ctx
	if outputCancellation(ctx, nil) != nil {
		// After timeout or cancellation, allow one brief diagnostic attempt
		// using the already-started stderr worker.
		var stop context.CancelFunc
		reportCtx, stop = context.WithTimeout(context.WithoutCancel(ctx), diagnosticWindow)
		defer stop()
	}
	return reportStatus(exitIncomplete, report(reportCtx, fmt.Appendf(nil, "bog-export: export incomplete: %v\n", err)))
}

func reportStatus(status int, err error) int {
	if err != nil && status == exitOK {
		return exitIncomplete
	}
	return status
}

func writeDiagnostic(ctx context.Context, stderr io.Writer, message []byte) error {
	output, err := openOutput(ctx, stderr)
	if err != nil {
		return err
	}
	return errors.Join(output.WriteRecord(message), output.Close())
}

func export(ctx context.Context, opts options, out io.Writer) (resultErr error) {
	server, err := client.New(opts.baseURL, nil)
	if err != nil {
		return fmt.Errorf("configure query client: %w", err)
	}
	defer server.CloseIdleConnections()
	server.MaxPageBytes = opts.maxPageBytes

	var progress *checkpointProgress
	if opts.checkpoint != "" {
		progress, err = openCheckpoint(opts.checkpoint, opts.resume, out)
		if err != nil {
			return fmt.Errorf("prepare checkpointed export: %w", err)
		}
		defer func() {
			if err := progress.close(); err != nil {
				resultErr = errors.Join(resultErr, fmt.Errorf("close export checkpoint lock: %w", err))
			}
		}()
	}

	output, err := openOutput(ctx, out)
	if err != nil {
		return fmt.Errorf("prepare stdout: %w", err)
	}
	defer func() {
		if err := output.Close(); err != nil {
			resultErr = errors.Join(resultErr, fmt.Errorf("close stdout output: %w", err))
		}
	}()

	if progress != nil {
		return exportCheckpointed(ctx, server, opts, progress, output)
	}
	walk, err := server.NewBoundedWalkWithContext(ctx, opts.query, opts.maxPages)
	if err != nil {
		return fmt.Errorf("prepare query walk: %w", err)
	}
	for pageNumber := 1; ; pageNumber++ {
		page, err := walk.NextPageWithRetry(opts.retry)
		if err == io.EOF {
			return outputCancellation(ctx, nil)
		}
		if err != nil {
			return fmt.Errorf("get query page %d: %w", pageNumber, err)
		}
		if err := writePage(output, pageNumber, page.Rows); err != nil {
			return err
		}
	}
}

func writePage(out *recordOutput, pageNumber int, rows []client.Row) error {
	for i, row := range rows {
		if err := writeRow(out, row); err != nil {
			return fmt.Errorf("write stdout (page %d, row %d): %w", pageNumber, i+1, err)
		}
	}
	return nil
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
