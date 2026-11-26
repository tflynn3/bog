package main

import (
	"context"
	"net/http"

	"bog/internal/httpapi"
	"bog/internal/readiness"
	"bog/internal/store"
	"bog/internal/wal"
)

func newReadyHandler(ctx context.Context, c config, s *store.Store, w *wal.Log) http.Handler {
	return readiness.NewHandler(ctx, c.dir, w, func(appender readiness.Appender) http.Handler {
		return httpapi.NewWithQueryLimits(s, appender, c.maxResponseBytes, c.queryConcurrency, c.queryWorkBudget)
	})
}
