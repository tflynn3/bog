package main

import (
	"bog/internal/bogclient"
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
func run() error {
	flags := flag.NewFlagSet("bogctl", flag.ContinueOnError)
	server := flags.String("server", "http://127.0.0.1:8080", "URL of a single Bog process")
	timeout := flags.Duration("timeout", 30*time.Second, "deadline for the entire operation")
	start := flags.String("start", "", "inclusive RFC3339 query start")
	end := flags.String("end", "", "exclusive RFC3339 query end")
	contains := flags.String("contains", "", "literal query filter")
	limit := flags.Int("limit", 500, "query page size (1–1000)")
	if err := flags.Parse(os.Args[1:]); err != nil {
		return err
	}
	if flags.NArg() != 1 {
		return fmt.Errorf("usage: bogctl [flags] health|ingest|query")
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithTimeout(ctx, *timeout)
	defer cancel()
	client := bogclient.New(*server)
	switch flags.Arg(0) {
	case "health":
		if err := client.Health(ctx); err != nil {
			return err
		}
		fmt.Println("healthy")
		return nil
	case "ingest":
		return client.Ingest(ctx, os.Stdin)
	case "query":
		if *start == "" || *end == "" || *limit < 1 || *limit > 1000 {
			return fmt.Errorf("query requires -start, -end and a page size between 1 and 1000")
		}
		return client.Query(ctx, *start, *end, *contains, *limit, os.Stdout)
	default:
		return fmt.Errorf("unknown command %q", flags.Arg(0))
	}
}
