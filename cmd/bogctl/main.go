package main

import (
	"bog/internal/bogclient"
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"time"
)

func run() error {
	if len(os.Args) < 2 {
		return fmt.Errorf("usage: bogctl health|ingest|query [options]")
	}
	cmd := os.Args[1]
	f := flag.NewFlagSet(cmd, flag.ContinueOnError)
	addr := f.String("addr", "http://127.0.0.1:8080", "direct Bog process URL")
	retry503 := f.Bool("retry-503", false, "retry 503 only when connected directly to Bog")
	timeout := f.Duration("timeout", 0, "overall timeout (0 disables)")
	start := f.String("start", "", "inclusive RFC3339 timestamp")
	end := f.String("end", "", "exclusive RFC3339 timestamp")
	contains := f.String("contains", "", "literal filter")
	limit := f.Int("limit", 500, "rows per page")
	if err := f.Parse(os.Args[2:]); err != nil {
		return err
	}
	if f.NArg() != 0 {
		return fmt.Errorf("unexpected arguments")
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	if *timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, *timeout)
		defer cancel()
	}
	c := bogclient.New(*addr)
	c.Retry503 = *retry503
	switch cmd {
	case "health":
		return c.Health(ctx)
	case "ingest":
		return c.IngestNDJSON(ctx, os.Stdin)
	case "query":
		s, e := time.Parse(time.RFC3339Nano, *start)
		if e != nil {
			return fmt.Errorf("start: %w", e)
		}
		t, e := time.Parse(time.RFC3339Nano, *end)
		if e != nil {
			return fmt.Errorf("end: %w", e)
		}
		return c.Query(ctx, bogclient.Query{Start: s, End: t, Contains: *contains, Limit: *limit}, os.Stdout)
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
