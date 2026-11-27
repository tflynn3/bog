package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"bog/internal/checkpoint"
	"bog/internal/client"
)

func (s liveService) checkPublishedQueries(ctx context.Context, rows []checkpoint.BlockRow) error {
	c, err := client.New(s.base, nil)
	if err != nil {
		return err
	}
	defer c.CloseIdleConnections()
	// Keep the HTTP parity probes within the shipped server's fixed query
	// work budget at every benchmark scale. Disk parity covers all ordinals.
	start, end := epoch, epoch.Add(37*time.Second)
	for _, literal := range []string{sparseLiteral, "x", "é", "€"} {
		want := reference(rows, query{start: start, end: end, literal: literal})
		if err := checkLiveQuery(ctx, c, client.Query{Start: start, End: end, Contains: literal, Limit: 1000}, want); err != nil {
			return fmt.Errorf("live query %q: %w", literal, err)
		}
	}
	return nil
}

func checkLiveQuery(ctx context.Context, c *client.Client, q client.Query, want []checkpoint.BlockRow) error {
	walk, err := c.NewBoundedWalk(q, len(want)+1)
	if err != nil {
		return err
	}
	i := 0
	for {
		page, err := walk.NextPage(ctx)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}
		for _, row := range page.Rows {
			if i == len(want) || !row.TS.Equal(want[i].Row.TS) || row.Line != want[i].Row.Line {
				return fmt.Errorf("HTTP ordered row %d disagrees with oracle", i)
			}
			i++
		}
	}
	if i != len(want) {
		return fmt.Errorf("HTTP returned %d rows, oracle wants %d", i, len(want))
	}
	return nil
}
