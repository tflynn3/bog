package main

import (
	"bog/internal/bogclient"
	"context"
	"flag"
	"fmt"
	"os"
	"time"
)

func run() error {
	fs := flag.NewFlagSet("bogctl", flag.ContinueOnError)
	server := fs.String("server", "http://127.0.0.1:8080", "Bog process URL")
	timeout := fs.Duration("timeout", 30*time.Second, "operation deadline")
	if err := fs.Parse(os.Args[1:]); err != nil {
		return err
	}
	args := fs.Args()
	if len(args) == 0 {
		return fmt.Errorf("usage: bogctl [--server URL] [--timeout DURATION] health|ingest|query")
	}
	c, err := bogclient.New(*server, nil)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	switch args[0] {
	case "health":
		if len(args) != 1 {
			return fmt.Errorf("health takes no arguments")
		}
		if err = c.Health(ctx); err == nil {
			fmt.Println("healthy")
		}
		return err
	case "ingest":
		if len(args) != 1 {
			return fmt.Errorf("ingest reads NDJSON from stdin")
		}
		return c.IngestStream(ctx, os.Stdin)
	case "query":
		f := flag.NewFlagSet("query", flag.ContinueOnError)
		start := f.String("start", "", "inclusive RFC3339 timestamp")
		end := f.String("end", "", "exclusive RFC3339 timestamp")
		contains := f.String("contains", "", "literal substring")
		limit := f.Int("limit", 500, "rows per page (1-1000)")
		if err = f.Parse(args[1:]); err != nil {
			return err
		}
		if *start == "" || *end == "" || f.NArg() != 0 {
			return fmt.Errorf("query requires --start and --end")
		}
		return c.Query(ctx, bogclient.Query{Start: *start, End: *end, Contains: *contains, Limit: *limit}, os.Stdout)
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
