package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"net/netip"
	"net/url"
	"strconv"
	"strings"
	"time"

	"bog/internal/client"
)

// Match the /query protocol limit without importing the server.
const maxQueryLimit = 1000

const usage = `Usage:
  bog-export -url URL -start TIME -end TIME
             [-contains TEXT] [-limit 100] [-max-pages 100000]
             [-max-page-bytes 33554432] [-max-retries 3]
             [-retry-delay 100ms] [-timeout 5m]

Export query rows as NDJSON to stdout. Times are RFC3339 and the range is
half-open. The filter is a case-sensitive substring. The page budget includes
the terminal page. The timeout covers the walk, retries, and output.
An incomplete export can leave a partial final record. Earlier newline-
terminated records remain valid and in query order.

Exit statuses:
  0  complete export or help
  1  incomplete export or output failure
  2  invalid options
`

type options struct {
	baseURL      string
	query        client.Query
	maxPages     int
	maxPageBytes int64
	retry        client.PageRetry
	timeout      time.Duration
}

func parseOptions(args []string) (options, error) {
	var opts options
	var start, end string
	flags := flag.NewFlagSet("bog-export", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	flags.StringVar(&opts.baseURL, "url", "", "base URL of the Bog server")
	flags.StringVar(&start, "start", "", "inclusive RFC3339 timestamp")
	flags.StringVar(&end, "end", "", "exclusive RFC3339 timestamp")
	flags.StringVar(&opts.query.Contains, "contains", "", "case-sensitive substring filter")
	flags.IntVar(&opts.query.Limit, "limit", 100, "maximum rows per page")
	flags.IntVar(&opts.maxPages, "max-pages", 100000, "maximum pages, including the terminal page")
	flags.Int64Var(&opts.maxPageBytes, "max-page-bytes", client.DefaultMaxPage, "maximum encoded bytes per query page")
	flags.IntVar(&opts.retry.MaxRetries, "max-retries", 3, "maximum retries per page")
	flags.DurationVar(&opts.retry.Delay, "retry-delay", 100*time.Millisecond, "wait between page attempts")
	flags.DurationVar(&opts.timeout, "timeout", 5*time.Minute, "overall export deadline")
	if err := flags.Parse(args); err != nil {
		return options{}, err
	}
	if flags.NArg() != 0 {
		return options{}, fmt.Errorf("unexpected argument %q", flags.Arg(0))
	}
	u, err := url.Parse(opts.baseURL)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") ||
		u.Hostname() == "" || u.Opaque != "" || u.RawQuery != "" || u.ForceQuery || strings.Contains(opts.baseURL, "#") {
		return options{}, errors.New("-url must be an HTTP or HTTPS base URL without a query or fragment")
	}
	if host := u.Hostname(); strings.Contains(host, ":") || strings.HasPrefix(u.Host, "[") {
		addr, err := netip.ParseAddr(host)
		if err != nil || !addr.Is6() || !strings.HasPrefix(u.Host, "[") {
			return options{}, errors.New("-url must use a valid bracketed IPv6 address for an IPv6 host")
		}
	}
	if port := u.Port(); port != "" {
		if n, err := strconv.ParseUint(port, 10, 16); err != nil || n == 0 {
			return options{}, errors.New("-url port must be between 1 and 65535")
		}
	}
	opts.query.Start, err = client.ParseQueryTime(start)
	if err != nil {
		return options{}, fmt.Errorf("invalid -start (expected RFC3339): %w", err)
	}
	opts.query.End, err = client.ParseQueryTime(end)
	if err != nil {
		return options{}, fmt.Errorf("invalid -end (expected RFC3339): %w", err)
	}
	if opts.query.End.Before(opts.query.Start) {
		return options{}, errors.New("-end precedes -start")
	}
	if opts.query.Limit < 1 || opts.query.Limit > maxQueryLimit {
		return options{}, fmt.Errorf("-limit must be between 1 and %d", maxQueryLimit)
	}
	if opts.maxPages < 1 {
		return options{}, errors.New("-max-pages must be positive")
	}
	if opts.maxPageBytes < 1 || opts.maxPageBytes == math.MaxInt64 {
		return options{}, fmt.Errorf("-max-page-bytes must be between 1 and %d", int64(math.MaxInt64-1))
	}
	if opts.retry.MaxRetries < 0 {
		return options{}, errors.New("-max-retries must be nonnegative")
	}
	if opts.retry.Delay < 0 {
		return options{}, errors.New("-retry-delay must be nonnegative")
	}
	if opts.timeout <= 0 {
		return options{}, errors.New("-timeout must be positive")
	}
	return opts, nil
}
