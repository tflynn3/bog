package main

import (
	"bog/internal/bogclient"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func run() error {
	fs := flag.NewFlagSet("bogctl", flag.ContinueOnError)
	base := fs.String("url", "http://127.0.0.1:8080", "Bog base URL")
	timeout := fs.Duration("timeout", 0, "overall timeout (0 disables)")
	start := fs.String("start", "", "inclusive RFC3339 timestamp")
	end := fs.String("end", "", "exclusive RFC3339 timestamp")
	contains := fs.String("contains", "", "literal filter")
	limit := fs.Int("limit", 500, "page size (1-1000)")
	if err := fs.Parse(os.Args[1:]); err != nil {
		return err
	}
	args := fs.Args()
	if len(args) != 1 {
		return fmt.Errorf("usage: bogctl [flags] health|ingest|query")
	}
	c, err := bogclient.New(*base, nil)
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
	switch args[0] {
	case "health":
		return c.Health(ctx)
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
		return c.Query(ctx, bogclient.Query{Start: s, End: e, Contains: *contains, Limit: *limit}, func(row json.RawMessage) error { _, err := fmt.Fprintln(os.Stdout, string(row)); return err })
	default:
		return fmt.Errorf("unknown command %q", args[0])
	}
}
func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
