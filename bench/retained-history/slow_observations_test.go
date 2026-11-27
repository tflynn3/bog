package main

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestReadSocketPressure(t *testing.T) {
	const server, client = "0100007F:2030", "0100007F:3040"
	for _, tc := range []struct {
		name, fields, problem string
		queue                 uint64
		ready                 bool
	}{
		{name: "zero window", fields: "01 0000A100:00000000 04:00000001", queue: 0xa100, ready: true},
		{name: "retransmission", fields: "01 0000A100:00000000 01:00000001", queue: 0xa100},
		{name: "empty queue", fields: "01 00000000:00000000 04:00000001"},
		{name: "finished", fields: "05 0000A100:00000000 04:00000001", problem: "no longer established"},
		{name: "missing timer", fields: "01 0000A100:00000000", problem: "truncated"},
		{name: "malformed queue", fields: "01 nope:00000000 04:00000001", problem: "invalid TCP queue"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			table := "sl local_address rem_address st tx_queue:rx_queue tr:tm->when\n" +
				"0: " + client + " " + server + " 01 0000FFFF:00000000 04:00000001\n" +
				"1: " + server + " " + client + " " + tc.fields
			got, ready, err := readSocketPressure(table, server, client)
			if tc.problem != "" {
				if err == nil || !strings.Contains(err.Error(), tc.problem) {
					t.Errorf("got error %v, want %q", err, tc.problem)
				}
			} else if err != nil || ready != tc.ready || got.SendQueue != tc.queue {
				t.Errorf("got %+v, %v, %v, want %d bytes and ready=%v", got, ready, err, tc.queue, tc.ready)
			}
		})
	}
	if _, _, err := readSocketPressure("", server, client); err == nil {
		t.Fatal("a missing connection supplied evidence")
	}
}

func TestPhaseMemorySamplesInsideWorkloadAndJoinsOnFailure(t *testing.T) {
	e := newSlowResponseEvidence()
	e.origin = time.Now()
	workErr := errors.New("work failed")
	sampleErr := errors.New("memory read failed")
	joined := make(chan struct{})
	var reads atomic.Int32
	s := service{readMemoryCurrent: func(context.Context) (uint64, error) {
		switch reads.Add(1) {
		case 1:
			return 32, nil
		case 2:
			return 96, nil
		default:
			close(joined)
			return 0, sampleErr
		}
	}}
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	err := e.measurePhase(ctx, s, "test", func() error {
		select {
		case <-joined:
			return workErr
		case <-ctx.Done():
			return ctx.Err()
		}
	})
	if !errors.Is(err, workErr) || !errors.Is(err, sampleErr) {
		t.Fatalf("phase lost work or measurement error: %v", err)
	}
	m := e.Memory["test"]
	if m.Peak != 96 || m.Samples != 2 || m.Error != sampleErr.Error() || reads.Load() != 3 {
		t.Fatalf("unexpected phase memory %+v after %d calls", m, reads.Load())
	}
}
