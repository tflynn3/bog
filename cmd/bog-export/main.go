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

	"bog/internal/client"
)

const (
	exitOK         = 0
	exitIncomplete = 1
	exitUsage      = 2
)

func main() {
	// Report a closed pipeline as a stdout write failure.
	signal.Ignore(syscall.SIGPIPE)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	status := run(ctx, os.Args[1:], os.Stdout, os.Stderr)
	stop()
	os.Exit(status)
}

func run(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	opts, err := parseOptions(args)
	if errors.Is(err, flag.ErrHelp) {
		if err := writeOnce(stderr, []byte(usage)); err != nil {
			return exitIncomplete
		}
		return exitOK
	}
	if err != nil {
		return reportError(stderr, exitUsage, fmt.Errorf("%w (see bog-export -h)", err))
	}
	if err := export(ctx, opts, stdout); err != nil {
		return reportError(stderr, exitIncomplete, fmt.Errorf("export incomplete: %w", err))
	}
	return exitOK
}

func reportError(stderr io.Writer, status int, err error) int {
	if err := writeOnce(stderr, fmt.Appendf(nil, "bog-export: %v\n", err)); err != nil {
		return exitIncomplete
	}
	return status
}

func export(ctx context.Context, opts options, out io.Writer) error {
	ctx, cancel := context.WithTimeout(ctx, opts.timeout)
	defer cancel()
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
	var readyToWrite bool
	for pageNumber := 1; ; pageNumber++ {
		page, err := walk.NextPageWithRetry(retry)
		if err == io.EOF {
			return ctx.Err()
		}
		if err != nil {
			return fmt.Errorf("get query page %d: %w", pageNumber, err)
		}
		if len(page.Rows) > 0 && !readyToWrite {
			writer, finish, err := prepareOutput(ctx, out)
			if err != nil {
				return err
			}
			out, readyToWrite = writer, true
			if finish != nil {
				defer func() {
					if err := finish(); err != nil {
						resultErr = errors.Join(resultErr, err)
					}
				}()
			}
		}
		for i, row := range page.Rows {
			if err := writeRow(ctx, out, row); err != nil {
				return fmt.Errorf("write stdout (page %d, row %d): %w", pageNumber, i+1, err)
			}
		}
	}
}

func writeRow(ctx context.Context, out io.Writer, row client.Row) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	record, err := json.Marshal(row)
	if err != nil {
		return fmt.Errorf("encode NDJSON record: %w", err)
	}
	record = append(record, '\n')
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := writeOnce(out, record); err != nil {
		return errors.Join(err, ctx.Err())
	}
	return ctx.Err()
}

func writeOnce(out io.Writer, data []byte) error {
	n, err := out.Write(data)
	if err != nil {
		return err
	}
	if n != len(data) {
		return io.ErrShortWrite
	}
	return nil
}
