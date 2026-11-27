package client

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

const exportDestinationStagePrefix = ".export-output-stage-"

// ExportFile writes a bounded snapshot as NDJSON to a private sibling file.
// After accepting the terminal page, it syncs and closes the file and
// atomically replaces path. Errors before replacement leave the destination
// intact and remove the stage. The parent must be an existing, trusted
// directory on a local filesystem with atomic rename. This provides atomic
// visibility. Crash durability of the directory entry is outside this contract.
// Set a deadline on ctx to bound the walk and its retries.
func (c *Client) ExportFile(ctx context.Context, q Query, maxPages int, retry PageRetry, path string) error {
	return c.exportFile(ctx, q, maxPages, retry, path, (*os.Root).Rename)
}

func (c *Client) exportFile(ctx context.Context, q Query, maxPages int, retry PageRetry, path string,
	rename func(*os.Root, string, string) error,
) (resultErr error) {
	walk, err := c.NewBoundedWalkWithContext(ctx, q, maxPages)
	if err != nil {
		return err
	}
	if err := withQueryCancellation(ctx, nil); err != nil {
		return err
	}
	dir, name := filepath.Split(path)
	if name == "" || name == "." || name == ".." || strings.HasPrefix(strings.ToLower(name), exportDestinationStagePrefix) {
		return errors.New("export destination must name a file outside the reserved staging prefix")
	}
	if dir == "" {
		dir = "."
	}
	root, err := os.OpenRoot(dir)
	if err != nil {
		return fmt.Errorf("open export destination directory: %w", err)
	}
	defer func() {
		resultErr = exportIOError(resultErr, "close export destination directory", root.Close())
	}()

	stage := exportDestinationStagePrefix + rand.Text()
	file, err := root.OpenFile(stage, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return fmt.Errorf("stage export output: %w", err)
	}
	published := false
	defer func() {
		if !published {
			resultErr = exportIOError(resultErr, "remove staged export output", root.Remove(stage))
		}
	}()

	err = writeExportRows(ctx, file, walk, retry)
	if err == nil {
		err = exportIOError(nil, "sync staged export output", file.Sync())
	}
	err = exportIOError(err, "close staged export output", file.Close())
	if err := withQueryCancellation(ctx, err); err != nil {
		return err
	}
	// Rename is the commit point. A cancellation racing a successful rename
	// leaves the completed export published and reports success.
	if err := rename(root, stage, name); err != nil {
		return withQueryCancellation(ctx, fmt.Errorf("publish export output: %w", err))
	}
	published = true
	return nil
}

func writeExportRows(ctx context.Context, out io.Writer, walk *ContextBoundedWalk, retry PageRetry) error {
	encoder := json.NewEncoder(out)
	for {
		page, err := walk.NextPageWithRetry(retry)
		if err != nil {
			return err
		}
		for _, row := range page.Rows {
			if err := withQueryCancellation(ctx, nil); err != nil {
				return err
			}
			if err := encoder.Encode(row); err != nil {
				return fmt.Errorf("write staged export row: %w", err)
			}
		}
		if page.NextCursor == "" {
			return withQueryCancellation(ctx, nil)
		}
	}
}
