package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"
	"time"

	"bog/internal/bogclient"
)

func run(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: bogctl health|ingest|query [options]")
	}
	f := flag.NewFlagSet(args[0], flag.ContinueOnError)
	endpoint := f.String("url", "http://127.0.0.1:8080", "Bog process URL")
	timeout := f.Duration("timeout", 30*time.Second, "request deadline (0 disables)")
	start := f.String("start", "", "inclusive RFC3339 timestamp")
	end := f.String("end", "", "exclusive RFC3339 timestamp")
	contains := f.String("contains", "", "literal filter")
	limit := f.Int("limit", 500, "rows per page (1–1000)")
	if err := f.Parse(args[1:]); err != nil {
		return err
	}
	if f.NArg() != 0 {
		return fmt.Errorf("unexpected arguments")
	}
	c, err := bogclient.New(*endpoint, nil)
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
		b, err := io.ReadAll(io.LimitReader(os.Stdin, bogclient.MaxBatch+1))
		if err != nil {
			return err
		}
		return c.Ingest(ctx, b)
	case "query":
		if *start == "" || *end == "" {
			return fmt.Errorf("query requires -start and -end")
		}
		return c.Query(ctx, bogclient.Query{Start: *start, End: *end, Contains: *contains, Limit: *limit}, os.Stdout)
	default:
		return fmt.Errorf("unknown command %q", args[0])
	}
}
func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
