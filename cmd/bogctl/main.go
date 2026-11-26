package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
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
	fs := flag.NewFlagSet("bogctl", flag.ContinueOnError)
	address := fs.String("addr", "http://127.0.0.1:8080", "process-specific Bog URL")
	start := fs.String("start", "", "inclusive RFC3339 timestamp")
	end := fs.String("end", "", "exclusive RFC3339 timestamp")
	contains := fs.String("contains", "", "literal filter")
	limit := fs.Int("limit", 500, "page size")
	timeout := fs.Duration("timeout", 0, "overall deadline (0 disables)")
	if err := fs.Parse(os.Args[1:]); err != nil {
		return err
	}
	if fs.NArg() != 1 {
		return fmt.Errorf("usage: bogctl [flags] ingest|query")
	}
	c, err := bogclient.New(*address, nil)
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if *timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, *timeout)
		defer cancel()
	}
	switch fs.Arg(0) {
	case "ingest":
		return c.IngestStream(ctx, os.Stdin)
	case "query":
		s, err := time.Parse(time.RFC3339Nano, *start)
		if err != nil {
			return fmt.Errorf("start: %w", err)
		}
		e, err := time.Parse(time.RFC3339Nano, *end)
		if err != nil {
			return fmt.Errorf("end: %w", err)
		}
		enc := json.NewEncoder(os.Stdout)
		return c.Query(ctx, bogclient.Query{Start: s, End: e, Contains: *contains, Limit: *limit}, func(r bogclient.Row) error { return enc.Encode(r) })
	default:
		return fmt.Errorf("unknown command %q", fs.Arg(0))
	}
}
