package main

import (
	"context"
	"net/http"
	"os"

	"bog/internal/httpapi"
	"bog/internal/readiness"
)

func newReadyHandler(ctx context.Context, c config, s *retained, probe *os.File) (http.Handler, *httpapi.API) {
	source := &retainedPages{storage: s, responseBytes: c.maxResponseBytes}
	var api *httpapi.API
	handler := readiness.NewHandlerForDirectory(ctx, probe, s, func(appender readiness.Appender) http.Handler {
		api = httpapi.NewWithPageSourceAndQueryResources(source, appender, c.maxResponseBytes, c.queryConcurrency, c.queryWorkBudget,
			httpapi.QueryResourceLimits{ResponseBytes: 32 << 20, ScanBytes: 64 << 20, Captures: maxLiveCaptures})
		api.SetIngestMemoryLimit(32 << 20)
		return api
	})
	return handler, api
}
