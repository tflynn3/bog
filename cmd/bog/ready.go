package main

import (
	"context"
	"errors"
	"net/http"
	"os"

	"bog/internal/httpapi"
	"bog/internal/readiness"
)

func openProbe(s *storageOwner) (_ *os.File, err error) {
	probe, err := s.root.Open(".")
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			err = errors.Join(err, probe.Close())
		}
	}()
	want, err := s.dir.Stat()
	if err != nil {
		return nil, err
	}
	got, err := probe.Stat()
	if err != nil {
		return nil, err
	}
	if !os.SameFile(want, got) {
		return nil, errors.New("readiness directory differs from WAL directory")
	}
	return probe, nil
}

func newReadyHandler(ctx context.Context, c config, s *storageOwner, probe *os.File) (http.Handler, *httpapi.API) {
	var api *httpapi.API
	handler := readiness.NewHandlerForDirectory(ctx, probe, s, func(appender readiness.Appender) http.Handler {
		api = httpapi.NewWithPageSourceAndQueryResources(&pageSource{owner: s}, appender, c.maxResponseBytes,
			c.queryConcurrency, c.queryWorkBudget, httpapi.QueryResourceLimits{
				ResponseBytes: uint64(min(c.maxResponseBytes, httpapi.DefaultQueryMaxResponseBytes)),
				ScanBytes:     52 << 20,
				Captures:      64,
			})
		return api
	})
	return handler, api
}
