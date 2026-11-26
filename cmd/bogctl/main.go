package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"bog/internal/bogclient"
)

func run() error {
	if len(os.Args) < 2 {
		return fmt.Errorf("usage: bogctl ingest|query [options]")
	}
	cmd := os.Args[1]
	f := flag.NewFlagSet(cmd, flag.ContinueOnError)
	server := f.String("server", "http://127.0.0.1:8080", "Bog URL (use a direct process address for paginated queries)")
	timeout := f.Duration("timeout", 0, "overall timeout (0 disables)")
	start := f.String("start", "", "inclusive RFC3339 timestamp")
	end := f.String("end", "", "exclusive RFC3339 timestamp")
	contains := f.String("contains", "", "literal filter")
	limit := f.Int("limit", 500, "rows per page (1–1000)")
	if err := f.Parse(os.Args[2:]); err != nil {
		return err
	}
	if f.NArg() != 0 {
		return fmt.Errorf("unexpected arguments")
	}
	c, err := bogclient.New(*server, nil)
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
	switch cmd {
	case "ingest":
		return c.IngestStream(ctx, os.Stdin)
	case "query":
		if *start == "" || *end == "" {
			return fmt.Errorf("query requires -start and -end")
		}
		return c.Query(ctx, bogclient.Query{Start: *start, End: *end, Contains: *contains, Limit: *limit}, os.Stdout)
	default:
		return fmt.Errorf("unknown command %q", cmd)
	}
}
func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
