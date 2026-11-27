package main

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestExportDeadlineOnLaterPage(t *testing.T) {
	t.Parallel()
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if requests.Add(1) == 1 {
			servePage(t, w, cursorFor(1), rowA)
			return
		}
		<-r.Context().Done()
	}))
	defer server.Close()
	var stdout, stderr bytes.Buffer
	status := run(t.Context(), argsFor(server.URL, "-timeout", "750ms"), &stdout, &stderr)
	assertResult(t, status, stdout.String(), stderr.String(), exitIncomplete, rowA, "context deadline exceeded")
	if got := requests.Load(); got != 2 {
		t.Errorf("requests = %d, want 2", got)
	}
}

// First-page output consumes part of the export deadline. The second page's
// retry delay fits within a fresh deadline but overruns the remaining time.
func TestExportSharesDeadlineAcrossOutputAndRetries(t *testing.T) {
	t.Parallel()
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		switch requests.Add(1) {
		case 1:
			servePage(t, w, cursorFor(1), rowA)
		case 2:
			http.Error(w, "busy", http.StatusServiceUnavailable)
		default:
			servePage(t, w, "", rowB)
		}
	}))
	defer server.Close()
	var stdout slowOutput
	var stderr bytes.Buffer
	status := run(t.Context(), argsFor(server.URL, "-timeout", "1s", "-retry-delay", "700ms"), &stdout, &stderr)
	assertResult(t, status, stdout.String(), stderr.String(), exitIncomplete, rowA, "context deadline exceeded")
	if got := requests.Load(); got != 2 {
		t.Errorf("requests = %d, want 2", got)
	}
	if !stdout.deadline.IsZero() {
		t.Errorf("stdout deadline remained set to %v", stdout.deadline)
	}
}

type slowOutput struct {
	bytes.Buffer
	mu       sync.Mutex
	deadline time.Time
}

func (w *slowOutput) SetWriteDeadline(deadline time.Time) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.deadline = deadline
	return nil
}

func (w *slowOutput) Write(p []byte) (int, error) {
	w.mu.Lock()
	deadline := w.deadline
	w.mu.Unlock()
	time.Sleep(time.Until(deadline.Add(-450 * time.Millisecond)))
	return w.Buffer.Write(p)
}
