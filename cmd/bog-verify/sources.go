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
	server.MaxPageBytes = opts.maxPageBytes
	walk, err := server.NewBoundedWalk(opts.query, opts.maxPages)
	if err != nil {
		return exitUsage, err
	}

	query := backup.Query{Start: opts.query.Start, End: opts.query.End, Contains: opts.query.Contains}
	reference, err := backup.OpenReferenceWalk(ctx, opts.bundle, query, opts.query.Limit)
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return exitIncomplete, fmt.Errorf("comparison interrupted while opening reference backup %q: %w", opts.bundle, err)
		}
		if errors.Is(err, backup.ErrReferenceWalk) {
			return exitIncomplete, fmt.Errorf("reference backup %q: %w", opts.bundle, err)
		}
		return exitVerification, verificationError(opts.bundle, err)
	}
	point := reference.Point()
	source := referenceStream(reference, opts.maxPages)
	live := func(ctx context.Context) (queryPage, error) {
		p, err := walk.NextPage(ctx)
		if errors.Is(err, client.ErrPageBytes) {
			return queryPage{}, fmt.Errorf("live page exceeds -max-page-bytes=%d; increase -max-page-bytes or reduce -limit: %w", opts.maxPageBytes, err)
		}
		if err != nil {
			return queryPage{}, err
		}
		return queryPage{rows: p.Rows, terminal: p.NextCursor == ""}, nil
	}
	result, status, err := compare(ctx, source, live)
	if closeErr := reference.Close(); closeErr != nil {
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

func referenceStream(walk *backup.ReferenceWalk, maxPages int) pageStream {
	pages := 0
	return func(ctx context.Context) (queryPage, error) {
		if pages == maxPages {
			return queryPage{}, fmt.Errorf("after %d pages: %w", pages, client.ErrPageBudget)
		}
		rows, terminal, err := walk.NextPage(ctx)
		if err != nil {
			return queryPage{}, err
		}
		pages++
		page := queryPage{terminal: terminal, rows: make([]client.Row, len(rows))}
		for i, r := range rows {
			page.rows[i] = r.Row
		}
		return page, nil
	}
}
