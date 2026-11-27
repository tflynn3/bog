package querymodel_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"bog/verify/capacityevidence"
)

const (
	capacityControl         = "/_capacity/compaction"
	pinnedCompactionTimeout = 5 * time.Second
)

type pinnedCompaction struct {
	raw  *http.Client
	base string
}

type pinnedCompactionReply struct {
	capacityevidence.CompactionObservation
	metrics json.RawMessage
}

type pinnedCompactionHTTPError struct {
	action string
	cutoff int
	status int
}

func (e *pinnedCompactionHTTPError) Error() string {
	return fmt.Sprintf("compaction %s at cutoff %d returned HTTP %d", e.action, e.cutoff, e.status)
}

func (c pinnedCompaction) read(ctx context.Context, method, action string, cutoff int) (pinnedCompactionReply, error) {
	path := c.base + capacityControl + "?action=" + action + "&cutoff=" + strconv.Itoa(cutoff)
	status, body, err := pinnedRequest(ctx, c.raw, method, path)
	if err != nil {
		return pinnedCompactionReply{}, fmt.Errorf("compaction %s at cutoff %d: %w", action, cutoff, err)
	}
	if status != http.StatusOK {
		return pinnedCompactionReply{}, &pinnedCompactionHTTPError{action: action, cutoff: cutoff, status: status}
	}
	observation, metrics, err := capacityevidence.DecodeCompactionStatus(body)
	if err != nil {
		return pinnedCompactionReply{}, fmt.Errorf("decode compaction %s at cutoff %d: %w", action, cutoff, err)
	}
	return pinnedCompactionReply{CompactionObservation: observation, metrics: metrics}, nil
}

// publish confirms that the requested cutoff is selected. The caller supplies
// the deadline for publication and any pre-start status request.
func (c pinnedCompaction) publish(ctx context.Context, previous capacityevidence.CompactionObservation, cutoff, committed int) (capacityevidence.CompactionObservation, error) {
	if previous.State != "complete" || cutoff <= previous.CoveredRows || cutoff > committed || previous.CommittedRows > committed {
		return capacityevidence.CompactionObservation{}, fmt.Errorf("invalid predecessor at cutoff %d with %d expected committed rows: %+v", cutoff, committed, previous)
	}
	observe := func(method, action string) (capacityevidence.CompactionObservation, error) {
		if err := ctx.Err(); err != nil {
			return capacityevidence.CompactionObservation{}, err
		}
		reply, err := c.read(ctx, method, action, cutoff)
		if err != nil {
			return capacityevidence.CompactionObservation{}, err
		}
		current := reply.CompactionObservation
		if current.CommittedRows == committed &&
			(current.State == "running" && current.Generation == previous.Generation && current.CoveredRows == previous.CoveredRows ||
				current.State == "complete" && current.Generation > previous.Generation && current.CoveredRows == cutoff) {
			return current, nil
		}
		return current, fmt.Errorf("unexpected compaction %s at cutoff %d with %d expected committed rows: %+v (predecessor %+v)", action, cutoff, committed, current, previous)
	}
	started, err := observe(http.MethodPost, "start")
	if err != nil {
		return started, err
	}
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		current, err := observe(http.MethodGet, "status")
		if err != nil {
			return current, err
		}
		if started.State == "complete" && current != started {
			return current, fmt.Errorf("completed compaction changed before status observation at cutoff %d: start=%+v status=%+v", cutoff, started, current)
		}
		if current.State == "complete" {
			return current, ctx.Err()
		}
		select {
		case <-ctx.Done():
			return current, ctx.Err()
		case <-ticker.C:
		}
	}
}

func pinnedRequest(ctx context.Context, raw *http.Client, method, path string) (status int, body []byte, err error) {
	req, err := http.NewRequestWithContext(ctx, method, path, nil)
	if err != nil {
		return 0, nil, err
	}
	req.Header.Set("Accept-Encoding", "identity")
	res, err := raw.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer func() { err = errors.Join(err, res.Body.Close()) }()
	const maxBody = 8192
	body, err = io.ReadAll(io.LimitReader(res.Body, maxBody+1))
	if err == nil && (len(body) > maxBody || res.ContentLength >= 0 && res.ContentLength != int64(len(body))) {
		err = errors.New("invalid control response body length")
	}
	return res.StatusCode, body, err
}

func TestPinnedCompactionPublication(t *testing.T) {
	t.Parallel()
	const (
		running       = `{"state":"running","generation":1,"covered_rows":2,"committed_rows":7}`
		full          = `{"state":"complete","generation":2,"covered_rows":5,"committed_rows":5}`
		partial       = `{"state":"complete","generation":2,"covered_rows":5,"committed_rows":7}`
		changed       = `{"state":"complete","generation":3,"covered_rows":5,"committed_rows":7}`
		partialMetric = `{"state":"complete","generation":2,"covered_rows":5,"committed_rows":7,"metrics":{"retired_generation_pins":1}}`
	)
	tests := []struct {
		name      string
		committed int
		replies   []string
		wantErr   string
	}{
		{"running to complete with suffix", 7, []string{running, running, partial}, ""},
		{"immediate fully covered", 5, []string{full, full}, ""},
		{"immediate completion with changing metrics", 7, []string{partial, partialMetric}, ""},
		{"changed completed status", 7, []string{partial, changed}, "completed compaction changed before status observation"},
		{"running after completed start", 7, []string{partial, running}, "completed compaction changed before status observation"},
		{"running generation changed", 7, []string{strings.Replace(running, `"generation":1`, `"generation":2`, 1)}, "unexpected compaction start"},
		{"running coverage changed", 7, []string{strings.Replace(running, `"covered_rows":2`, `"covered_rows":3`, 1)}, "unexpected compaction start"},
		{"completion at wrong cutoff", 7, []string{running, strings.Replace(partial, `"covered_rows":5`, `"covered_rows":6`, 1)}, "unexpected compaction status"},
		{"completion did not advance", 7, []string{strings.Replace(partial, `"generation":2`, `"generation":1`, 1)}, "unexpected compaction start"},
		{"lost committed suffix", 7, []string{running, full}, "unexpected compaction status"},
		{"extra committed rows", 5, []string{partial}, "unexpected compaction start"},
		{"duplicate field", 7, []string{strings.Replace(running, `"generation":1`, `"generation":1,"generation":1`, 1)}, `duplicate compaction status member "generation"`},
		{"mis-cased field", 7, []string{running, strings.Replace(partial, `"covered_rows"`, `"Covered_Rows"`, 1)}, `missing or null "covered_rows"`},
		{"oversized successful reply", 7, []string{partial + strings.Repeat(" ", 8192)}, "invalid control response body length"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			const cutoff = 5
			var requests atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				i := int(requests.Add(1)) - 1
				method, action := http.MethodGet, "status"
				if i == 0 {
					method, action = http.MethodPost, "start"
				}
				if i >= len(tc.replies) || r.Method != method || r.URL.Path != capacityControl ||
					r.URL.RawQuery != "action="+action+"&cutoff=5" || r.Header.Get("Accept-Encoding") != "identity" {
					t.Errorf("unexpected compaction request %d: %s %s, Accept-Encoding=%q", i, r.Method, r.URL, r.Header.Get("Accept-Encoding"))
					w.WriteHeader(http.StatusBadRequest)
					return
				}
				if _, err := io.WriteString(w, tc.replies[i]); err != nil {
					t.Errorf("write compaction reply %d: %v", i, err)
				}
			}))
			defer server.Close()
			ctx, cancel := context.WithTimeout(t.Context(), pinnedCompactionTimeout)
			defer cancel()
			previous := capacityevidence.CompactionObservation{State: "complete", Generation: 1, CoveredRows: 2, CommittedRows: 2}
			if tc.committed > cutoff {
				previous.CommittedRows = tc.committed
			}
			control := pinnedCompaction{raw: server.Client(), base: server.URL}
			got, err := control.publish(ctx, previous, cutoff, tc.committed)
			if requests.Load() != int32(len(tc.replies)) {
				t.Errorf("got %d requests, want %d", requests.Load(), len(tc.replies))
			}
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("publication error = %v, want %q", err, tc.wantErr)
				}
				return
			}
			want := capacityevidence.CompactionObservation{State: "complete", Generation: 2, CoveredRows: cutoff, CommittedRows: tc.committed}
			if err != nil || got != want {
				t.Fatalf("publication = %+v, %v, want %+v", got, err, want)
			}
		})
	}
}

func TestPinnedCompactionPublicationCancellation(t *testing.T) {
	t.Parallel()
	deadline, stop := context.WithTimeout(t.Context(), pinnedCompactionTimeout)
	defer stop()
	ctx, cancel := context.WithCancel(deadline)
	defer cancel()
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch n := requests.Add(1); {
		case n == 1 && r.Method == http.MethodPost:
			if _, err := io.WriteString(w, `{"state":"running","generation":0,"covered_rows":0,"committed_rows":5}`); err != nil {
				t.Errorf("write running reply: %v", err)
			}
		case n == 2 && r.Method == http.MethodGet:
			cancel()
			select {
			case <-r.Context().Done():
			case <-deadline.Done():
				t.Error("canceling publication did not cancel the in-flight poll")
			}
		default:
			t.Errorf("unexpected request %d: %s %s", n, r.Method, r.URL)
			w.WriteHeader(http.StatusBadRequest)
		}
	}))
	defer server.Close()
	control := pinnedCompaction{raw: server.Client(), base: server.URL}
	previous := capacityevidence.CompactionObservation{State: "complete"}
	if _, err := control.publish(ctx, previous, 5, 5); !errors.Is(err, context.Canceled) {
		t.Errorf("publication error = %v, want context cancellation", err)
	}
	if got := requests.Load(); got != 2 {
		t.Errorf("got %d requests, want one start and one status poll", got)
	}
}
