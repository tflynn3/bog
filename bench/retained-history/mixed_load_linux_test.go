package main

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestMixedLoadLiveContinuationBackpressure(t *testing.T) {
	f := mixedLoadLiveFixture(fixtures("live")[0])
	w := f.newWalk(f.SeedRows+f.BatchRows, false)
	w.pageLimit, w.cursor = mixedLoadPageLimit(f), "held"
	body, err := f.batch(1, w.pageLimit+1)
	if err != nil {
		t.Fatal(err)
	}
	bound, err := mixedLoadTCPBufferBound()
	if err != nil {
		t.Fatal(err)
	}
	if len(body) < 7<<20 || len(body) > 8<<20 {
		t.Fatalf("fixture body of %d bytes is outside the 7–8 MiB range", len(body))
	}
	if int64(len(body)) <= bound {
		t.Skipf("host TCP buffer bound of %d bytes exceeds fixture body of %d bytes", bound, len(body))
	}
	finished := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(out http.ResponseWriter, _ *http.Request) {
		defer close(finished)
		out.Header().Set("Content-Type", "application/x-ndjson")
		out.Header().Set("Content-Length", fmt.Sprint(len(body)))
		_, _ = out.Write(body)
	}))
	defer server.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 6*time.Second)
	defer cancel()
	run := mixedLoadRun{lifetime: ctx, report: &mixedLoadReport{Queries: make(map[string]*queryPhase)}}
	if err := run.useService(newService(server.URL)); err != nil {
		t.Fatal(err)
	}
	slow, err := run.startSlow(ctx, w, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer slow.close()
	observer := linuxMixedLoad{tcpBufferBound: bound, process: &child{
		port: server.Listener.Addr().(*net.TCPAddr).Port, done: make(chan struct{}),
	}}
	if err := mixedLoadPoll(ctx, func() (bool, error) { return observer.Stalled(ctx, slow) }); err != nil {
		t.Fatal(err)
	}
	select {
	case <-finished:
		t.Fatal("handler returned while the TCP probe reported a stalled response")
	default:
	}
	if err := slow.close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-finished:
	case <-ctx.Done():
		t.Fatal("canceling the stalled response did not unblock the handler")
	}
}
