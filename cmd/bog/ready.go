package main

import (
	"context"
	"net"
	"net/http"
	"os"

	"bog/internal/httpapi"
	"bog/internal/readiness"
)

func newReadyHandler(ctx context.Context, c config, rt *retainedRuntime, dir *os.File, addr net.Addr) (http.Handler, *httpapi.API) {
	var api *httpapi.API
	handler := readiness.NewHandlerForDirectory(ctx, dir, rt, func(appender readiness.Appender) http.Handler {
		api = httpapi.NewWithPageSourceAndQueryResources(rt, appender, c.maxResponseBytes, c.queryConcurrency, c.queryWorkBudget,
			httpapi.QueryResourceLimits{ResponseBytes: 40 << 20, ScanBytes: 56 << 20, Captures: 64})
		api.LimitIngestMemory(56 << 20)
		listen, ok := addr.(*net.TCPAddr)
		if os.Getenv("BOG_CAPACITY_CONTROL") != "1" || !ok || listen.IP.To4() == nil || !listen.IP.IsLoopback() {
			return api
		}
		mux := http.NewServeMux()
		mux.Handle("/", api)
		mux.HandleFunc("/_capacity/compaction", rt.capacityControl(api))
		return mux
	})
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if rt.stopped.Load() {
			http.Error(w, "server draining", http.StatusServiceUnavailable)
			return
		}
		handler.ServeHTTP(w, req)
	}), api
}
