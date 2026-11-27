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

	"bog/verify/capacityevidence"
)

const controlPath = "/_capacity/compaction"

type liveService struct {
	client    *http.Client
	base      string
	pid, port int
	strict    bool
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

func (r controlReply) atCutoff(state string, count int) bool {
	return r.State == state && r.Generation != nil &&
		r.CoveredRows != nil && *r.CoveredRows == count &&
		r.CommittedRows != nil && *r.CommittedRows == count
}

func (s liveService) control(ctx context.Context, method, action string, cutoff int) (controlReply, int, error) {
	status, data, err := s.request(ctx, method, controlPath+"?action="+action+"&cutoff="+strconv.Itoa(cutoff), nil)
	var reply controlReply
	if err != nil || status != http.StatusOK {
		return reply, status, err
	}
	if s.strict {
		observation, _, err := capacityevidence.DecodeCompactionStatus(data)
		if err != nil {
			return reply, status, err
		}
		return controlReply{observation.State, &observation.Generation, &observation.CoveredRows, &observation.CommittedRows}, status, nil
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

func (s liveService) advance(ctx context.Context, count int, previous controlReply) (controlReply, error) {
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	method, action := http.MethodPost, "start"
	for {
		reply, status, err := s.control(ctx, method, action, count)
		if err != nil {
			return controlReply{}, err
		}
		if status != http.StatusOK {
			return controlReply{}, fmt.Errorf("compaction %s returned HTTP %d", action, status)
		}
		if *reply.CommittedRows != count || reply.State == "idle" {
			return controlReply{}, errors.New("compaction status disagrees with acknowledged workload")
		}
		if reply.State == "complete" {
			if *reply.Generation <= *previous.Generation || *reply.CoveredRows != count {
				return controlReply{}, errors.New("compaction failed to publish requested cutoff in a newer generation")
			}
			return reply, nil
		}
		if s.strict && (*reply.Generation != *previous.Generation || *reply.CoveredRows != *previous.CoveredRows) {
			return controlReply{}, errors.New("running compaction disagrees with its predecessor")
		}
		method, action = http.MethodGet, "status"
		select {
		case <-ctx.Done():
			return controlReply{}, ctx.Err()
		case <-ticker.C:
		}
	}
}
