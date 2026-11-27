package main

import (
	"context"
	"errors"
	"fmt"
	"io"

	"bog/internal/backup"
	"bog/internal/client"
)

func compareBackup(ctx context.Context, opts options, server *client.Client, out io.Writer) (int, error) {
	result, point, status, err := compareRestore(ctx, opts, server, nil, nil)
	if err != nil {
		return status, err
	}
	return report(out, fmt.Sprintf("query matched: %s %s\n", comparisonCounts(*result), formatPoint(*point)))
}

func compareRestore(ctx context.Context, opts options, server *client.Client, retry *client.PageRetry, expected *backup.Point) (
	result *comparison, point *backup.Point, status int, err error,
) {
	walk, err := server.NewBoundedWalk(opts.query, opts.maxPages)
	if err != nil {
		return result, point, exitUsage, err
	}
	query := backup.Query{Start: opts.query.Start, End: opts.query.End, Contains: opts.query.Contains}
	reference, err := backup.OpenReferenceWalk(ctx, opts.bundle, query, opts.query.Limit)
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return result, point, exitIncomplete, fmt.Errorf("comparison interrupted while opening reference backup %q: %w", opts.bundle, err)
		}
		if errors.Is(err, backup.ErrReferenceWalk) {
			return result, point, exitIncomplete, fmt.Errorf("reference backup %q: %w", opts.bundle, err)
		}
		return result, point, exitVerification, verificationError(opts.bundle, err)
	}
	defer func() {
		if closeErr := reference.Close(); closeErr != nil {
			err = errors.Join(err, fmt.Errorf("close reference backup %q: %w", opts.bundle, closeErr))
			if status == exitOK {
				status = exitIncomplete
			}
		}
	}()
	verified := reference.Point()
	point = &verified
	if expected != nil && verified != *expected {
		return result, point, exitVerification, verificationError(opts.bundle,
			fmt.Errorf("reference recovery point changed: expected %s, found %s", formatPoint(*expected), formatPoint(verified)))
	}
	fetch := walk.NextPage
	if retry != nil {
		fetch = func(ctx context.Context) (client.Page, error) {
			return walk.NextPageWithRetry(ctx, *retry)
		}
	}
	live := func(ctx context.Context) (queryPage, error) {
		p, err := fetch(ctx)
		if errors.Is(err, client.ErrPageBytes) {
			return queryPage{}, fmt.Errorf("live page exceeds -max-page-bytes=%d; increase -max-page-bytes or reduce -limit: %w", opts.maxPageBytes, err)
		}
		if err != nil {
			return queryPage{}, err
		}
		return queryPage{rows: p.Rows, terminal: p.NextCursor == ""}, nil
	}
	counts, status, err := compare(ctx, referenceStream(reference, opts.maxPages), live)
	return &counts, point, status, err
}

func comparisonCounts(result comparison) string {
	return fmt.Sprintf("rows=%d reference_pages=%d server_pages=%d",
		result.matched, result.referencePages, result.livePages)
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
