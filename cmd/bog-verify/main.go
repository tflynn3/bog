package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/url"
	"os"
	"os/signal"
	"syscall"
	"time"

	"bog/internal/backup"
	"bog/internal/client"
	"bog/internal/store"
)

const usage = `Usage:
  bog-verify verify -backup DIR [-timeout 5m]
  bog-verify compare -backup DIR -url URL -start TIME -end TIME
                     [-contains TEXT] [-limit 100] [-max-pages 100000] [-timeout 5m]

The reference backup must remain immutable throughout the command. Use a
separate copy as the restored server's data directory. Query times are
RFC3339 and the range is half-open. The filter is a case-sensitive substring.

Exit statuses:
  0  verified backup or complete query match
  2  invalid command or options
  3  backup verification failed
  4  comparison incomplete or result could not be reported
  5  results differ
`

type options struct {
	bundle   string
	baseURL  string
	timeout  time.Duration
	maxPages int
	query    client.Query
}

func parseOptions(mode string, args []string) (options, error) {
	var opts options
	flags := flag.NewFlagSet(mode, flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	flags.StringVar(&opts.bundle, "backup", "", "path to immutable reference backup")
	flags.DurationVar(&opts.timeout, "timeout", 5*time.Minute, "overall operation deadline")
	var start, end string
	if mode == "compare" {
		flags.StringVar(&opts.baseURL, "url", "", "base URL of the restored Bog server")
		flags.StringVar(&start, "start", "", "inclusive RFC3339 timestamp")
		flags.StringVar(&end, "end", "", "exclusive RFC3339 timestamp")
		flags.StringVar(&opts.query.Contains, "contains", "", "case-sensitive substring filter")
		flags.IntVar(&opts.query.Limit, "limit", 100, "maximum rows per page")
		flags.IntVar(&opts.maxPages, "max-pages", 100000, "maximum pages per stream, including the terminal page")
	}
	if err := flags.Parse(args); err != nil {
		return options{}, err
	}
	if flags.NArg() != 0 {
		return options{}, fmt.Errorf("unexpected argument %q", flags.Arg(0))
	}
	if opts.bundle == "" {
		return options{}, errors.New("-backup is required")
	}
	if opts.timeout <= 0 {
		return options{}, errors.New("-timeout must be positive")
	}
	if mode == "verify" {
		return opts, nil
	}
	u, err := url.Parse(opts.baseURL)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") ||
		u.Hostname() == "" || u.Opaque != "" || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" {
		return options{}, errors.New("-url must be an HTTP or HTTPS base URL without a query or fragment")
	}
	opts.query.Start, err = store.ParseTime(start)
	if err != nil {
		return options{}, fmt.Errorf("invalid -start: %w", err)
	}
	opts.query.End, err = store.ParseTime(end)
	if err != nil {
		return options{}, fmt.Errorf("invalid -end: %w", err)
	}
	if opts.query.End.Before(opts.query.Start) {
		return options{}, errors.New("-end precedes -start")
	}
	if opts.query.Limit < 1 || opts.query.Limit > backup.MaxPageSize {
		return options{}, fmt.Errorf("-limit must be between 1 and %d", backup.MaxPageSize)
	}
	if opts.maxPages < 1 {
		return options{}, errors.New("-max-pages must be positive")
	}
	return opts, nil
}

func run(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	status, err := execute(ctx, args, stdout)
	if err != nil {
		if _, writeErr := fmt.Fprintf(stderr, "bog-verify: %v\n", err); writeErr != nil {
			return exitIncomplete
		}
	}
	return status
}

func execute(ctx context.Context, args []string, out io.Writer) (int, error) {
	if len(args) == 0 {
		if _, err := io.WriteString(out, usage); err != nil {
			return exitUsage, fmt.Errorf("write usage: %w", err)
		}
		return exitUsage, nil
	}
	mode := args[0]
	switch mode {
	case "help", "-h", "--help":
		return showHelp(out)
	case "verify", "compare":
	default:
		return exitUsage, fmt.Errorf("unknown command %q (see bog-verify -h)", mode)
	}
	opts, err := parseOptions(mode, args[1:])
	if errors.Is(err, flag.ErrHelp) {
		return showHelp(out)
	}
	if err != nil {
		return exitUsage, err
	}

	ctx, cancel := context.WithTimeout(ctx, opts.timeout)
	defer cancel()
	if mode == "verify" {
		point, err := backup.Verify(ctx, opts.bundle)
		if err != nil {
			return exitVerification, verificationError(opts.bundle, err)
		}
		if err := ctx.Err(); err != nil {
			return exitVerification, verificationError(opts.bundle, err)
		}
		return report(out, fmt.Sprintf("backup verified: %s\n", formatPoint(point)))
	}
	return compareBackup(ctx, opts, out)
}

func showHelp(out io.Writer) (int, error) {
	if _, err := io.WriteString(out, usage); err != nil {
		return exitIncomplete, fmt.Errorf("write usage: %w", err)
	}
	return exitOK, nil
}

func report(out io.Writer, message string) (int, error) {
	if _, err := io.WriteString(out, message); err != nil {
		return exitIncomplete, fmt.Errorf("write result: %w", err)
	}
	return exitOK, nil
}

func verificationError(bundle string, err error) error {
	return fmt.Errorf("backup verification failed for %q: %w", bundle, err)
}

func formatPoint(p backup.Point) string {
	return fmt.Sprintf("generation=%x covered_offset=%d next_ordinal=%d committed_end=%d",
		p.Generation, p.CoveredOffset, p.NextOrdinal, p.CommittedEnd)
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	status := run(ctx, os.Args[1:], os.Stdout, os.Stderr)
	stop()
	os.Exit(status)
}
