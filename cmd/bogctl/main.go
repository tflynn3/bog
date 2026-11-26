package main

import (
	"context"
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
	if len(os.Args) < 2 {
		return fmt.Errorf("usage: bogctl ingest|query [flags]")
	}
	fs := flag.NewFlagSet(os.Args[1], flag.ContinueOnError)
	addr := fs.String("addr", "http://127.0.0.1:8080", "Bog endpoint")
	timeout := fs.Duration("timeout", 30*time.Second, "request timeout")
	start := fs.String("start", "", "inclusive RFC3339 start")
	end := fs.String("end", "", "exclusive RFC3339 end")
	contains := fs.String("contains", "", "literal filter")
	limit := fs.Int("limit", 500, "page size")
	cursor := fs.String("cursor", "", "continuation cursor")
	all := fs.Bool("all", false, "stream all pages")
	if err := fs.Parse(os.Args[2:]); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return fmt.Errorf("unexpected arguments")
	}
	c, err := bogclient.New(*addr, nil)
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	ctx, cancel := context.WithTimeout(ctx, *timeout)
	defer cancel()
	switch os.Args[1] {
	case "ingest":
		b, err := io.ReadAll(io.LimitReader(os.Stdin, bogclient.MaxBatch+1))
		if err != nil {
			return err
		}
		return c.Ingest(ctx, b)
	case "query":
		q := bogclient.Query{Start: *start, End: *end, Contains: *contains, Limit: *limit, Cursor: *cursor, Page: *all && *cursor == ""}
		for {
			next, err := c.Query(ctx, q, os.Stdout)
			if err != nil {
				return err
			}
			if !*all {
				if next != "" {
					fmt.Fprintln(os.Stderr, "Bog-Next-Cursor:", next)
				}
				return nil
			}
			if next == "" {
				return nil
			}
			q.Cursor = next
			q.Page = false
		}
	default:
		return fmt.Errorf("unknown command %q", os.Args[1])
	}
}
