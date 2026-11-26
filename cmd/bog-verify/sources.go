package main

import (
	"context"
	"errors"
	"fmt"
	"io"

	"bog/internal/backup"
	"bog/internal/client"
)

func compareBackup(ctx context.Context, opts options, out io.Writer) (int, error) {
	server, err := client.New(opts.baseURL, nil)
	if err != nil {
		return exitUsage, fmt.Errorf("configure server client: %w", err)
	}
	defer server.CloseIdleConnections()
	walk, err := server.NewBoundedWalk(opts.query, opts.maxPages)
	if err != nil {
		return exitUsage, err
	}

	// OpenRestored verifies the whole bundle and pins the same directory for
	// all reads. The operator keeps this reference separate from the server.
	reader, err := backup.OpenRestored(ctx, opts.bundle)
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return exitIncomplete, fmt.Errorf("comparison interrupted while verifying backup %q: %w", opts.bundle, err)
		}
		return exitVerification, verificationError(opts.bundle, err)
	}
	point := reader.Point()
	source := referenceStream(reader, opts.query, opts.maxPages)
	live := func(ctx context.Context) (queryPage, error) {
		p, err := walk.NextPage(ctx)
		if err != nil {
			return queryPage{}, err
		}
		return queryPage{rows: p.Rows, terminal: p.NextCursor == ""}, nil
	}
	result, status, err := compare(ctx, source, live)
	if closeErr := reader.Close(); closeErr != nil {
		err = errors.Join(err, fmt.Errorf("close reference backup %q: %w", opts.bundle, closeErr))
		if status == exitOK {
			status = exitIncomplete
		}
	}
	if err != nil {
		return status, err
	}
	return report(out, fmt.Sprintf("query matched: rows=%d reference_pages=%d server_pages=%d %s\n",
		result.matched, result.referencePages, result.livePages, formatPoint(point)))
}

func referenceStream(reader *backup.Reader, q client.Query, maxPages int) pageStream {
	query := backup.Query{Start: q.Start, End: q.End, Contains: q.Contains}
	pages := 0
	return func(ctx context.Context) (queryPage, error) {
		if pages == maxPages {
			return queryPage{}, fmt.Errorf("after %d pages: %w", pages, client.ErrPageBudget)
		}
		rows, next, err := reader.QueryPage(ctx, query, q.Limit)
		if err != nil {
			return queryPage{}, err
		}
		query.After = next
		pages++
		page := queryPage{terminal: next == nil, rows: make([]client.Row, len(rows))}
		for i, r := range rows {
			page.rows[i] = r.Row
		}
		return page, nil
	}
}
