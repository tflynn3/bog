package main

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"time"

	"bog/internal/bogclient"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
func run() error {
	flags := flag.NewFlagSet("bogctl", flag.ContinueOnError)
	endpoint := flags.String("url", "http://127.0.0.1:8080", "Bog endpoint (use a process-specific address for pagination)")
	timeout := flags.Duration("timeout", 30*time.Second, "request timeout")
	start := flags.String("start", "", "inclusive RFC3339 query start")
	end := flags.String("end", "", "exclusive RFC3339 query end")
	contains := flags.String("contains", "", "literal filter")
	limit := flags.Int("limit", 1000, "rows per query page (1-1000)")
	if err := flags.Parse(os.Args[1:]); err != nil {
		return err
	}
	if flags.NArg() != 1 {
		return errors.New("usage: bogctl [flags] ingest|query")
	}
	c, err := bogclient.New(*endpoint)
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	switch flags.Arg(0) {
	case "ingest":
		return ingest(ctx, c, os.Stdin, *timeout)
	case "query":
		s, err := time.Parse(time.RFC3339Nano, *start)
		if err != nil {
			return fmt.Errorf("start: %w", err)
		}
		e, err := time.Parse(time.RFC3339Nano, *end)
		if err != nil {
			return fmt.Errorf("end: %w", err)
		}
		if *limit < 1 || *limit > 1000 {
			return errors.New("limit must be 1-1000")
		}
		q := bogclient.Query{Start: s, End: e, Contains: *contains, Limit: *limit}
		cursor := ""
		for {
			request, cancel := context.WithTimeout(ctx, *timeout)
			body, next, err := c.Query(request, q, cursor)
			if err != nil {
				cancel()
				return err
			}
			_, err = io.Copy(os.Stdout, body)
			closeErr := body.Close()
			cancel()
			if err != nil {
				return err
			}
			if closeErr != nil {
				return closeErr
			}
			if next == "" {
				return nil
			}
			cursor = next
		}
	default:
		return errors.New("expected ingest or query")
	}
}

// Ingest splits only on line boundaries; each transmitted batch is atomic.
func ingest(ctx context.Context, c *bogclient.Client, input io.Reader, timeout time.Duration) error {
	r := bufio.NewReader(input)
	batch := make([]byte, 0, bogclient.MaxBatch)
	send := func() error {
		if len(batch) == 0 {
			return nil
		}
		req, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()
		if err := c.Ingest(req, batch); err != nil {
			return err
		}
		batch = batch[:0]
		return nil
	}
	for {
		var line []byte
		var err error
		for {
			var part []byte
			part, err = r.ReadSlice('\n')
			if len(line)+len(part) > bogclient.MaxBatch {
				return errors.New("NDJSON line exceeds 4 MiB")
			}
			line = append(line, part...)
			if !errors.Is(err, bufio.ErrBufferFull) {
				break
			}
		}
		if err != nil && !errors.Is(err, io.EOF) {
			return err
		}
		if len(batch)+len(line) > bogclient.MaxBatch {
			if e := send(); e != nil {
				return e
			}
		}
		batch = append(batch, line...)
		if errors.Is(err, io.EOF) {
			return send()
		}
	}
}
