package main

import (
	"context"
	"net/http"
	"os"

	"bog/internal/httpapi"
	"bog/internal/readiness"
	"bog/internal/wal"
)

func newReadyHandler(ctx context.Context, dir *os.File, w *wal.Log, api *httpapi.API) http.Handler {
	return readiness.NewHandlerForDirectory(ctx, dir, w, func(readiness.Appender) http.Handler {
		return api
	})
}
