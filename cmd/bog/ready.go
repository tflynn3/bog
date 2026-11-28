package main

import (
	"context"
	"net"
	"net/http"
	"os"

	"bog/internal/httpapi"
	"bog/internal/readiness"
)

const maxServingResponse = 32 << 20

func newReadyHandler(ctx context.Context, c config, s *retained, probe *os.File, addr net.Addr) (http.Handler, *httpapi.API) {
	var api *httpapi.API
	responseBytes := min(c.maxResponseBytes, maxServingResponse)
	pages := &retainedPages{storage: s, responseBytes: responseBytes}
	handler := readiness.NewHandlerForDirectory(ctx, probe, s, func(appender readiness.Appender) http.Handler {
		api = httpapi.NewWithPageSourceAndQueryResources(pages, appender, responseBytes, min(c.queryConcurrency, 64),
			c.queryWorkBudget, httpapi.QueryResourceLimits{
				ResponseBytes: maxServingResponse,
				ScanBytes:     56 << 20,
				Captures:      maxCaptures,
			})
		api.SetIngestByteLimit(12 << 20)
		tcp, ok := addr.(*net.TCPAddr)
		if !ok || !tcp.IP.IsLoopback() || os.Getenv("BOG_CAPACITY_CONTROL") != "1" {
			return api
		}
		mux := http.NewServeMux()
		mux.HandleFunc("/_capacity/compaction", s.compactionHandler(api))
		mux.Handle("/", api)
		return mux
	})
	return handler, api
}
