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
		return fmt.Errorf("usage: bogctl health|ingest|query [flags]")
	}
	cmd := os.Args[1]
	fs := flag.NewFlagSet(cmd, flag.ContinueOnError)
	endpoint := fs.String("endpoint", "http://127.0.0.1:8080", "direct URL of one Bog process")
	timeout := fs.Duration("timeout", 0, "overall timeout (0 disables)")
	start := fs.String("start", "", "inclusive RFC3339 timestamp")
	end := fs.String("end", "", "exclusive RFC3339 timestamp")
	contains := fs.String("contains", "", "literal filter")
	limit := fs.Int("limit", 500, "rows per page (1-1000)")
	if err := fs.Parse(os.Args[2:]); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return fmt.Errorf("unexpected arguments")
	}
	c, err := bogclient.New(*endpoint, nil)
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	if *timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, *timeout)
		defer cancel()
	}
	switch cmd {
	case "health":
		return c.Health(ctx)
	case "ingest":
		return c.Ingest(ctx, os.Stdin)
	case "query":
		s, e := time.Parse(time.RFC3339Nano, *start)
		if e != nil {
			return e
		}
		t, e := time.Parse(time.RFC3339Nano, *end)
		if e != nil {
			return e
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
