package main

import (
	"context"
	"errors"
	"net/http"
	"os"
	"time"

	"bog/internal/httpapi"
	"bog/internal/readiness"
)

const (
	queryScanLimit     = 8 * scanReservationBytes
	queryResponseLimit = 32 << 20
	ingestBodyLimit    = 8 << 20
	queryAdmissionWait = 20 * time.Millisecond
)

func (s *retainedStorage) readyDirectory() (*os.File, error) {
	d, err := s.owner.OpenDirectory()
	if err != nil {
		return nil, err
	}
	owner, ownerErr := s.dir.Stat()
	opened, openedErr := d.Stat()
	err = errors.Join(ownerErr, openedErr)
	if err == nil && !os.SameFile(owner, opened) {
		err = errors.New("readiness directory differs from the WAL owner")
	}
	if err != nil {
		return nil, errors.Join(err, d.Close())
	}
	return d, nil
}

func newReadyHandler(ctx context.Context, c config, s *retainedStorage, dir *os.File) (http.Handler, *httpapi.API) {
	api := httpapi.NewWithPageSourceAndQueryResources(s, s, c.maxResponseBytes, c.queryConcurrency, c.queryWorkBudget, httpapi.QueryResourceLimits{
		ResponseBytes: queryResponseLimit, ScanBytes: queryScanLimit, Captures: captureLimit,
		AdmissionWait: queryAdmissionWait,
	})
	api.LimitIngestBytes(ingestBodyLimit)
	handler := readiness.NewHandlerForDirectory(ctx, dir, s, func(readiness.Appender) http.Handler { return api })
	return s.capacityHandler(api, handler), api
}
