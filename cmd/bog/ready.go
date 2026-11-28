package main

import (
	"context"
	"net"
	"net/http"
	"os"

	"bog/internal/httpapi"
	"bog/internal/readiness"
)

func capacityEnabled(listener net.Listener) bool {
	addr, ok := listener.Addr().(*net.TCPAddr)
	return os.Getenv("BOG_CAPACITY_CONTROL") == "1" && ok && addr.IP.To4() != nil && addr.IP.IsLoopback()
}

func newReadyHandler(ctx context.Context, c config, s *retained, probe *os.File, capacity bool) (http.Handler, *httpapi.API) {
	var api *httpapi.API
	handler := readiness.NewHandlerForDirectory(ctx, probe, s, func(appender readiness.Appender) http.Handler {
		api = httpapi.NewWithPageSourceAndQueryResources(s, appender, c.maxResponseBytes, c.queryConcurrency, c.queryWorkBudget,
			httpapi.QueryResourceLimits{ResponseBytes: 32 << 20, ScanBytes: aggregateScanBytes, Captures: 64})
		if !capacity {
			return api
		}
		mux := http.NewServeMux()
		mux.HandleFunc("/_capacity/compaction", s.compactionHandler(api))
		mux.Handle("/", api)
		return mux
	})
	return handler, api
}
