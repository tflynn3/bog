package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"time"

	"bog/internal/bogclient"
)

func run(args []string, in io.Reader, out io.Writer) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: bogctl health|ingest|query [flags]")
	}
	fs := flag.NewFlagSet(args[0], flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	addr := fs.String("addr", "http://127.0.0.1:8080", "Bog process URL")
	timeout := fs.Duration("timeout", 30*time.Second, "request deadline")
	start := fs.String("start", "", "inclusive RFC3339 timestamp")
	end := fs.String("end", "", "exclusive RFC3339 timestamp")
	contains := fs.String("contains", "", "literal filter")
	limit := fs.Int("limit", 500, "page size")
	if err := fs.Parse(args[1:]); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return fmt.Errorf("unexpected arguments")
	}
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	c := bogclient.New(*addr)
	switch args[0] {
	case "health":
		if err := c.Health(ctx); err != nil {
			return err
		}
		_, err := fmt.Fprintln(out, "healthy")
		return err
	case "ingest":
		return c.IngestStream(ctx, in)
	case "query":
		if *start == "" || *end == "" {
			return fmt.Errorf("query requires -start and -end")
		}
		return c.Query(ctx, bogclient.Query{Start: *start, End: *end, Contains: *contains, Limit: *limit}, out)
	default:
		return fmt.Errorf("unknown command %q", args[0])
	}
}
func main() {
	if err := run(os.Args[1:], os.Stdin, os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
