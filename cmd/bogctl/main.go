package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"bog/internal/bogclient"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
func run(args []string) error {
	f := flag.NewFlagSet("bogctl", flag.ContinueOnError)
	addr := f.String("addr", "http://127.0.0.1:8080", "Bog process URL")
	timeout := f.Duration("timeout", 30*time.Second, "operation deadline")
	if err := f.Parse(args); err != nil {
		return err
	}
	args = f.Args()
	if len(args) == 0 {
		return fmt.Errorf("usage: bogctl [global flags] health|ingest|query")
	}
	c, err := bogclient.New(*addr, nil)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	switch args[0] {
	case "health":
		return c.Health(ctx)
	case "ingest":
		if len(args) != 1 {
			return fmt.Errorf("ingest reads NDJSON from stdin")
		}
		return c.IngestStream(ctx, os.Stdin)
	case "query":
		qf := flag.NewFlagSet("query", flag.ContinueOnError)
		start := qf.String("start", "", "inclusive RFC3339 timestamp")
		end := qf.String("end", "", "exclusive RFC3339 timestamp")
		contains := qf.String("contains", "", "literal filter")
		limit := qf.Int("limit", 500, "rows per page")
		if err := qf.Parse(args[1:]); err != nil {
			return err
		}
		if *start == "" || *end == "" || qf.NArg() != 0 {
			return fmt.Errorf("query requires -start and -end")
		}
		return c.Query(ctx, bogclient.Query{Start: *start, End: *end, Contains: *contains, Limit: *limit}, os.Stdout)
	default:
		return fmt.Errorf("unknown command %q", args[0])
	}
}
