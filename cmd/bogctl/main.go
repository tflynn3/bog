package main

import (
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
	if err := run(os.Args[1:], os.Stdin, os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, err)
		var unknown *bogclient.UnknownOutcomeError
		if errors.As(err, &unknown) {
			os.Exit(2)
		}
		os.Exit(1)
	}
}
func run(args []string, in io.Reader, out io.Writer) error {
	fs := flag.NewFlagSet("bogctl", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	server := fs.String("server", "http://127.0.0.1:8080", "Bog process URL")
	timeout := fs.Duration("timeout", 30*time.Second, "overall timeout")
	if err := fs.Parse(args); err != nil {
		return err
	}
	args = fs.Args()
	if len(args) == 0 {
		return errors.New("usage: bogctl [-server URL] [-timeout DURATION] health|ingest|query")
	}
	c, err := bogclient.New(*server, nil)
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	ctx, cancel := context.WithTimeout(ctx, *timeout)
	defer cancel()
	switch args[0] {
	case "health":
		if len(args) != 1 {
			return errors.New("health takes no arguments")
		}
		if err := c.Health(ctx); err != nil {
			return err
		}
		_, err = fmt.Fprintln(out, "healthy")
		return err
	case "ingest":
		if len(args) != 1 {
			return errors.New("ingest reads NDJSON from stdin")
		}
		return c.IngestStream(ctx, in)
	case "query":
		qf := flag.NewFlagSet("query", flag.ContinueOnError)
		qf.SetOutput(io.Discard)
		start := qf.String("start", "", "inclusive RFC3339 timestamp")
		end := qf.String("end", "", "exclusive RFC3339 timestamp")
		contains := qf.String("contains", "", "literal substring")
		limit := qf.Int("limit", 500, "page size")
		if err := qf.Parse(args[1:]); err != nil {
			return err
		}
		if qf.NArg() != 0 {
			return errors.New("unexpected query arguments")
		}
		s, err := time.Parse(time.RFC3339Nano, *start)
		if err != nil {
			return fmt.Errorf("start: %w", err)
		}
		e, err := time.Parse(time.RFC3339Nano, *end)
		if err != nil {
			return fmt.Errorf("end: %w", err)
		}
		return c.QueryStream(ctx, bogclient.Query{Start: s, End: e, Contains: *contains, Limit: *limit}, out)
	default:
		return fmt.Errorf("unknown command %q", args[0])
	}
}
