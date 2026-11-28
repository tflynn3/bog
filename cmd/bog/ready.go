package main

import (
	"context"
	"net/http"
	"os"

	"bog/internal/httpapi"
	"bog/internal/readiness"
	"bog/internal/store"
	"bog/internal/wal"
)

func newReadyHandler(ctx context.Context, c config, s *store.Store, w *wal.Log, dir *os.File) http.Handler {
	return readiness.NewHandlerForDirectory(ctx, dir, w, func(appender readiness.Appender) http.Handler {
		return httpapi.NewWithQueryLimits(s, appender, c.maxResponseBytes, c.queryConcurrency, c.queryWorkBudget)
	})
}
