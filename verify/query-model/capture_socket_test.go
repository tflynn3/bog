package querymodel_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"mime"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"bog/internal/client"
	"bog/internal/httpapi"
	model "bog/verify/query-model"
)

const (
	faultHeader    = "X-Query-Model-Fault"
	socketDeadline = 3 * time.Second
)

func querySocketRequest(ctx context.Context, base string, q model.WireRequest) (*http.Request, error) {
	params := url.Values{
		"start":    {q.Filter.Start.UTC().Format(time.RFC3339Nano)},
		"end":      {q.Filter.End.UTC().Format(time.RFC3339Nano)},
		"contains": {q.Filter.Contains},
		"limit":    {strconv.Itoa(q.Limit)},
	}
	if q.Cursor == "" {
		params.Set("page", "1")
	} else {
		params.Set("cursor", q.Cursor)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, base+"/query?"+params.Encode(), nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept-Encoding", "identity")
	return req, nil
}

func observeQueryRefusal(ctx context.Context, raw *http.Client, base string, q model.WireRequest, status int, want string) (err error) {
	req, err := querySocketRequest(ctx, base, q)
	if err != nil {
		return err
	}
	res, err := raw.Do(req)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, res.Body.Close()) }()
	body, err := io.ReadAll(io.LimitReader(res.Body, int64(len(want))+1))
	if err != nil {
		return err
	}
	media, _, mediaErr := mime.ParseMediaType(res.Header.Get("Content-Type"))
	_, headerCursor := res.Header[cursorHeader]
	_, trailerCursor := res.Trailer[cursorHeader]
	for _, declaration := range res.Header.Values("Trailer") {
		for name := range strings.SplitSeq(declaration, ",") {
			trailerCursor = trailerCursor || strings.EqualFold(strings.TrimSpace(name), cursorHeader)
		}
	}
	framed := res.ContentLength == int64(len(body)) ||
		(res.ContentLength < 0 && len(res.TransferEncoding) == 1 && res.TransferEncoding[0] == "chunked")
	if res.StatusCode != status || string(body) != want || mediaErr != nil || media != "text/plain" ||
		!framed || headerCursor || trailerCursor || res.Uncompressed || res.Header.Get("Content-Encoding") != "" {
		// A buggy success may have returned cursor bytes. Report metadata
		// only for a response that is not a clean plain-text refusal.
		return fmt.Errorf("expected HTTP %d %q without a cursor, got HTTP %d type=%q read_bytes=%d declared=%d header_cursor=%t trailer_cursor=%t",
			status, want, res.StatusCode, media, len(body), res.ContentLength, headerCursor, trailerCursor)
	}
	return nil
}

type socketFault struct {
	mode model.OpeningFault
	done chan error
}

type resumeGate struct {
	entered chan struct{}
	release chan struct{}
	done    chan struct{}
}

type querySocketFixture struct {
	server  *httptest.Server
	raw     *http.Client
	client  *client.Client
	fetch   model.FetchWirePage
	next    atomic.Uint64
	actions sync.Map // string -> *socketFault or *resumeGate
}

type gateKey struct{}

type gatedQueryTransport struct{ http.RoundTripper }

func (t gatedQueryTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if id, ok := req.Context().Value(gateKey{}).(string); ok {
		req = req.Clone(req.Context())
		req.Header.Set(faultHeader, id)
	}
	return t.RoundTripper.RoundTrip(req)
}

func newQuerySocketFixture(t *testing.T, handler http.Handler) *querySocketFixture {
	t.Helper()
	s := new(querySocketFixture)
	s.server = httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		action, ok := s.actions.LoadAndDelete(r.Header.Get(faultHeader))
		if !ok {
			handler.ServeHTTP(w, r)
			return
		}
		switch a := action.(type) {
		case *socketFault:
			fault := &queryFaultWriter{ResponseWriter: w, mode: a.mode}
			handler.ServeHTTP(fault, r)
			a.done <- fault.observed
		case *resumeGate:
			defer close(a.done)
			handler.ServeHTTP(&heldResumeWriter{ResponseWriter: w, ctx: r.Context(), gate: a}, r)
		}
	}))
	s.server.Config.ReadHeaderTimeout = socketDeadline
	s.server.Config.ReadTimeout = socketDeadline
	s.server.Config.WriteTimeout = 2 * socketDeadline
	s.server.Start()
	t.Cleanup(s.server.Close)
	transport := s.server.Client().Transport.(*http.Transport).Clone()
	transport.DisableKeepAlives = true
	transport.MaxResponseHeaderBytes = 16 << 10
	t.Cleanup(transport.CloseIdleConnections)
	s.raw = &http.Client{
		Transport: transport, Timeout: socketDeadline,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
	c, fetch, err := newWitnessClient(s.server.URL, gatedQueryTransport{transport}, 256)
	if err != nil {
		t.Fatal(err)
	}
	s.client, s.fetch = c, fetch
	return s
}

func (s *querySocketFixture) register(action any) string {
	id := strconv.FormatUint(s.next.Add(1), 10)
	s.actions.Store(id, action)
	return id
}

func (s *querySocketFixture) refuse(ctx context.Context, q model.WireRequest, status int, body string) error {
	return observeQueryRefusal(ctx, s.raw, s.server.URL, q, status, body)
}

func (s *querySocketFixture) interrupt(ctx context.Context, q model.WireRequest, mode model.OpeningFault) (model.InterruptedOpening, error) {
	var observed model.InterruptedOpening
	fault := &socketFault{mode: mode, done: make(chan error, 1)}
	id := s.register(fault)
	defer s.actions.Delete(id)
	ctx, cancel := context.WithTimeout(ctx, socketDeadline)
	defer cancel()
	req, err := querySocketRequest(ctx, s.server.URL, q)
	if err != nil {
		return observed, err
	}
	req.Close = true
	req.Header.Set(faultHeader, id)
	switch mode {
	case model.BeforeHeaders:
		err = readNoResponse(ctx, req)
	case model.DuringBody:
		observed, err = s.readInterruptedBody(req, q.MaxPageBytes)
	default:
		return observed, errors.New("unknown opening fault")
	}
	// Handler completion, including deferred capture accounting, must
	// precede the next admission probe.
	select {
	case serverErr := <-fault.done:
		if !errors.Is(serverErr, os.ErrDeadlineExceeded) {
			err = errors.Join(err, fmt.Errorf("expected a server socket write timeout, got %v", serverErr))
		}
	case <-ctx.Done():
		err = errors.Join(err, fmt.Errorf("waiting for failed opening handler: %w", ctx.Err()))
	}
	return observed, err
}

func readNoResponse(ctx context.Context, req *http.Request) (err error) {
	conn, err := (&net.Dialer{}).DialContext(ctx, "tcp4", req.URL.Host)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, conn.Close()) }()
	deadline, _ := ctx.Deadline()
	if err := conn.SetDeadline(deadline); err != nil {
		return err
	}
	if err := req.Write(conn); err != nil {
		return err
	}
	n, err := io.Copy(io.Discard, io.LimitReader(conn, 1))
	if err != nil || n != 0 {
		return fmt.Errorf("pre-header exchange received %d response bytes (read error %v)", n, err)
	}
	return nil
}

func (s *querySocketFixture) readInterruptedBody(req *http.Request, maxBytes int64) (result model.InterruptedOpening, err error) {
	res, err := s.raw.Do(req)
	if err != nil {
		return result, err
	}
	defer func() { err = errors.Join(err, res.Body.Close()) }()
	tokens := res.Header.Values(cursorHeader)
	media, _, mediaErr := mime.ParseMediaType(res.Header.Get("Content-Type"))
	if res.StatusCode != http.StatusOK || res.ContentLength <= 1 || res.ContentLength > maxBytes ||
		len(tokens) != 1 || tokens[0] == "" || mediaErr != nil || media != "application/x-ndjson" ||
		res.Uncompressed || res.Header.Get("Content-Encoding") != "" {
		return result, fmt.Errorf("interrupted opening HTTP %d type=%q length=%d cursor_headers=%d",
			res.StatusCode, media, res.ContentLength, len(tokens))
	}
	body, err := io.ReadAll(io.LimitReader(res.Body, maxBytes+1))
	if !errors.Is(err, io.ErrUnexpectedEOF) || len(body) == 0 || int64(len(body)) >= res.ContentLength {
		return result, fmt.Errorf("expected truncated body, read %d of %d bytes: %v", len(body), res.ContentLength, err)
	}
	return model.InterruptedOpening{Cursor: tokens[0], DeclaredBytes: res.ContentLength, Prefix: body}, nil
}

type queryFaultWriter struct {
	http.ResponseWriter
	mode     model.OpeningFault
	observed error
	failed   bool
}

func (w *queryFaultWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

func (w *queryFaultWriter) Flush() {
	if err := w.FlushError(); err != nil && w.observed == nil {
		w.observed = err
	}
}

func (w *queryFaultWriter) FlushError() error {
	if w.mode == model.BeforeHeaders && w.Header().Get(cursorHeader) != "" {
		// The underlying writer has only staged the status and headers.
		// The expired deadline covers its first attempt to write to TCP.
		err := w.failWrite(nil)
		if errors.Is(err, os.ErrDeadlineExceeded) {
			return errors.Join(httpapi.ErrQueryHeadersUnsent, err)
		}
		return err
	}
	return http.NewResponseController(w.ResponseWriter).Flush()
}

func (w *queryFaultWriter) Write(p []byte) (int, error) {
	if w.Header().Get(cursorHeader) == "" {
		return w.ResponseWriter.Write(p)
	}
	if w.mode == model.BeforeHeaders || w.failed {
		return 0, w.failWrite(nil)
	}
	// Flush real headers and a nonempty prefix before arming the socket
	// deadline. The advertised length stays unchanged.
	prefix := len(p) / 2
	if prefix == 0 {
		return 0, io.ErrShortWrite
	}
	n, err := w.ResponseWriter.Write(p[:prefix])
	if err != nil {
		return n, err
	}
	if n != prefix {
		return n, io.ErrShortWrite
	}
	if err := http.NewResponseController(w.ResponseWriter).Flush(); err != nil {
		return n, err
	}
	return n, w.failWrite(p[prefix:])
}

func (w *queryFaultWriter) failWrite(pending []byte) error {
	if !w.failed {
		w.failed = true
		controller := http.NewResponseController(w.ResponseWriter)
		if err := controller.SetWriteDeadline(time.Now().Add(-time.Second)); err != nil {
			w.observed = fmt.Errorf("arm server write deadline: %v", err)
		} else {
			if len(pending) != 0 {
				n, err := w.ResponseWriter.Write(pending)
				w.observed = err
				if err == nil && n != len(pending) {
					w.observed = io.ErrShortWrite
				}
			}
			if w.observed == nil {
				// Force buffered bytes to cross the real server TCP socket.
				w.observed = controller.Flush()
			}
		}
		if w.observed == nil {
			w.observed = errors.New("armed socket write did not fail")
		}
	}
	return w.observed
}

type heldResumeWriter struct {
	http.ResponseWriter
	ctx     context.Context
	gate    *resumeGate
	entered bool
}

func (w *heldResumeWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

func (w *heldResumeWriter) WriteHeader(status int) {
	if status == http.StatusOK && !w.entered {
		w.entered = true
		close(w.gate.entered)
		select {
		case <-w.gate.release:
		case <-w.ctx.Done():
		}
	}
	w.ResponseWriter.WriteHeader(status)
}

type resumeResult struct {
	page model.WirePage
	err  error
}

func (s *querySocketFixture) holdResume(ctx context.Context, q model.WireRequest, whileHeld func() error) (model.WirePage, error) {
	gate := &resumeGate{entered: make(chan struct{}), release: make(chan struct{}), done: make(chan struct{})}
	id := s.register(gate)
	defer s.actions.Delete(id)
	ctx, cancel := context.WithTimeout(ctx, socketDeadline)
	defer cancel()
	result := make(chan resumeResult, 1)
	go func() {
		page, err := s.fetch(context.WithValue(ctx, gateKey{}, id), q)
		result <- resumeResult{page, err}
	}()

	var heldErr error
	var completed resumeResult
	var received bool
	select {
	case <-gate.entered:
		heldErr = whileHeld()
	case completed = <-result:
		heldErr = errors.New("resume completed before reaching the delivery gate")
		received = true
	case <-ctx.Done():
		heldErr = ctx.Err()
	}
	close(gate.release)
	// The client can read a complete body before the final capture release.
	// Join both sides before allowing a capacity probe to run.
	join, stop := context.WithTimeout(context.WithoutCancel(ctx), socketDeadline)
	defer stop()
	if !received {
		select {
		case completed = <-result:
		case <-join.Done():
			return model.WirePage{}, errors.Join(heldErr, fmt.Errorf("join resume client: %w", join.Err()))
		}
	}
	if _, unclaimed := s.actions.LoadAndDelete(id); !unclaimed {
		select {
		case <-gate.done:
		case <-join.Done():
			return model.WirePage{}, errors.Join(heldErr, completed.err, fmt.Errorf("join resume handler: %w", join.Err()))
		}
	}
	return completed.page, errors.Join(heldErr, completed.err)
}
