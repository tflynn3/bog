package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"
)

const controlPath = "/_capacity/compaction"

type liveService struct {
	client    *http.Client
	base      string
	pid, port int
}

func (s liveService) request(ctx context.Context, method, path string, body []byte) (status int, data []byte, err error) {
	req, err := http.NewRequestWithContext(ctx, method, s.base+path, bytes.NewReader(body))
	if err != nil {
		return 0, nil, err
	}
	if len(body) > 0 {
		req.Header.Set("Content-Type", "application/x-ndjson")
	}
	resp, err := s.client.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer func() { err = errors.Join(err, resp.Body.Close()) }()
	const limit = 64 << 10
	data, err = io.ReadAll(io.LimitReader(resp.Body, limit+1))
	if len(data) > limit {
		return resp.StatusCode, nil, errors.New("candidate reply exceeds 64 KiB")
	}
	return resp.StatusCode, data, err
}

type controlReply struct {
	State         string  `json:"state"`
	Generation    *uint64 `json:"generation"`
	CoveredRows   *int    `json:"covered_rows"`
	CommittedRows *int    `json:"committed_rows"`
}

func (s liveService) control(ctx context.Context, method, action string, cutoff int) (controlReply, int, error) {
	status, data, err := s.request(ctx, method, controlPath+"?action="+action+"&cutoff="+strconv.Itoa(cutoff), nil)
	var reply controlReply
	if err != nil || status != http.StatusOK {
		return reply, status, err
	}
	if err := json.Unmarshal(data, &reply); err != nil {
		return reply, status, err
	}
	if reply.Generation == nil || reply.CoveredRows == nil || reply.CommittedRows == nil ||
		*reply.CoveredRows < 0 || *reply.CommittedRows < *reply.CoveredRows ||
		(reply.State != "idle" && reply.State != "running" && reply.State != "complete") {
		return reply, status, errors.New("invalid live compaction state or counters")
	}
	return reply, status, nil
}

func (s liveService) advance(ctx context.Context, count int, previous uint64) error {
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	method, action := http.MethodPost, "start"
	for {
		reply, status, err := s.control(ctx, method, action, count)
		if err != nil {
			return err
		}
		if status != http.StatusOK {
			return fmt.Errorf("compaction %s returned HTTP %d", action, status)
		}
		if *reply.CommittedRows != count || reply.State == "idle" {
			return errors.New("compaction status disagrees with acknowledged workload")
		}
		if reply.State == "complete" {
			if *reply.Generation <= previous || *reply.CoveredRows != count {
				return errors.New("compaction failed to publish requested cutoff in a newer generation")
			}
			return nil
		}
		method, action = http.MethodGet, "status"
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}
