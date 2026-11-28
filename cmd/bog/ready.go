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
	h, _ := newReadyHandlerWithAPI(ctx, c, s, w)
	return h
}

// Keep the issuing API available to the lifecycle owner so it can retire
// snapshot captures after the HTTP server has drained.
func newReadyHandlerWithAPI(ctx context.Context, c config, s *store.Store, w *wal.Log) (http.Handler, *httpapi.API) {
	var api *httpapi.API
	h := readiness.NewHandler(ctx, c.dir, w, func(appender readiness.Appender) http.Handler {
		api = httpapi.NewWithQueryLimits(s, appender, c.maxResponseBytes, c.queryConcurrency, c.queryWorkBudget).(*httpapi.API)
		return api
	})
	return h, api
}
