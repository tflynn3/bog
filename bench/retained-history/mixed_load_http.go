package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptrace"
	"net/url"
	"strconv"
	"strings"
	"syscall"
	"time"
)

var errMixedLoadAdmission = errors.New("query admission refused")

type mixedLoadTransport func(*http.Request) (*http.Response, error)

func (f mixedLoadTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func (r *mixedLoadRun) useService(s service) error {
	base, err := url.Parse(s.base)
	if err != nil || base.Scheme != "http" || base.Hostname() != "127.0.0.1" ||
		base.Port() == "" || base.Path != "" || base.RawQuery != "" || base.Fragment != "" ||
		base.User != nil || s.client == nil {
		return errors.New("mixed load requires an IPv4 loopback HTTP service")
	}
	client := *s.client
	transport := client.Transport
	if transport == nil {
		transport = http.DefaultTransport
	}
	// Keep idle client connections out of candidate descriptor comparisons.
	client.Transport = mixedLoadTransport(func(req *http.Request) (*http.Response, error) {
		copy := req.Clone(req.Context())
		copy.Close = true
		return transport.RoundTrip(copy)
	})
	s.client, s.queryPhases, s.acknowledged = &client, r.report.Queries, &r.ledger
	r.service = s
	return nil
}

func (r *mixedLoadRun) open(ctx context.Context, w *walk) (err error) {
	var attempt queryAttempt
	defer func() { recordQuery(r.report.Queries, w.phase, attempt, err) }()
	resp, data, err := r.requestBounded(ctx, http.MethodGet, mixedLoadQuery(w.filter, "", 1), nil, &attempt, 128<<10)
	if resp != nil && resp.StatusCode == http.StatusOK {
		// Keep the token even if framing or ledger validation later fails.
		if token := resp.Header.Get("Bog-Next-Cursor"); token != "" {
			r.cursors[w] = token
		}
	}
	if err != nil {
		return err
	}
	attempt.Phase = "validation"
	mixedLoadRejection(&attempt, resp, data)
	if resp.StatusCode != http.StatusOK {
		err := fmt.Errorf("query HTTP %d (%s)", resp.StatusCode, attempt.Admission)
		if mixedLoadAdmissionRefusal(attempt.Admission) {
			return errors.Join(errMixedLoadAdmission, err)
		}
		return err
	}
	s := r.service
	client := *s.client
	client.Transport = mixedLoadTransport(func(*http.Request) (*http.Response, error) {
		resp.Body = io.NopCloser(bytes.NewReader(data))
		return resp, nil
	})
	s.client, s.queryPhases = &client, nil
	w.pageLimit = 1
	_, err = w.page(ctx, s, r.fixture)
	w.pageLimit = min(1000, (8<<20)/(r.LineBytes+128))
	if err == nil && w.cursor == "" {
		err = errors.New("opening page did not issue a capture cursor")
	}
	return err
}

func (r *mixedLoadRun) retire(ctx context.Context, w *walk) error {
	if err := r.retireCapture(ctx, r.control, r.cursors[w]); err != nil {
		return err
	}
	delete(r.cursors, w)
	return nil
}

func (r *mixedLoadRun) verifyLedger(ctx context.Context, phase string, matched *int) error {
	w := r.newWalk(r.ledger.rows(), false)
	w.phase = phase
	if len(w.want) != r.ledger.rows() {
		return errors.New("full query does not cover the acknowledged ledger")
	}
	err := r.open(ctx, w)
	if err == nil {
		_, err = w.exhaust(ctx, r.service, r.fixture)
	}
	*matched = w.offset
	if err != nil {
		return err
	}
	return r.retire(ctx, w)
}

func mixedLoadQuery(q filter, cursor string, limit int) string {
	params := url.Values{
		"start": {q.start.Format(time.RFC3339Nano)}, "end": {q.end.Format(time.RFC3339Nano)},
		"contains": {q.literal}, "limit": {strconv.Itoa(limit)},
	}
	if cursor == "" {
		params.Set("page", "1")
	} else {
		params.Set("cursor", cursor)
	}
	return "/query?" + params.Encode()
}

func mixedLoadRejection(attempt *queryAttempt, resp *http.Response, body []byte) {
	if resp.StatusCode != http.StatusServiceUnavailable {
		return
	}
	if attempt.HasCursor || len(resp.Trailer.Values("Bog-Next-Cursor")) != 0 {
		attempt.Admission = "malformed_rejection"
		return
	}
	attempt.observeRejection(body)
	switch strings.TrimSpace(string(body)) {
	case "query response capacity exhausted":
		attempt.Admission = "response_capacity_rejection"
	case "query scan capacity exhausted":
		attempt.Admission = "scan_capacity_rejection"
	case "query capture capacity exhausted":
		attempt.Admission = "capture_capacity_rejection"
	}
}

func mixedLoadAdmissionRefusal(reason string) bool {
	switch reason {
	case "capacity_rejection", "work_budget_rejection", "response_capacity_rejection",
		"scan_capacity_rejection", "capture_capacity_rejection":
		return true
	}
	return false
}

func (r *mixedLoadRun) requireRetired(ctx context.Context, q filter, token string) (err error) {
	var attempt queryAttempt
	defer func() { recordQuery(r.report.Queries, "retired_resume", attempt, err) }()
	res, body, err := r.requestBounded(ctx, http.MethodGet, mixedLoadQuery(q, token, 1), nil, &attempt, 64<<10)
	if err != nil {
		return err
	}
	if res.StatusCode != http.StatusGone || attempt.HasCursor || len(res.Trailer.Values("Bog-Next-Cursor")) != 0 ||
		strings.TrimSpace(string(body)) != "query capture expired" {
		return fmt.Errorf("retired continuation returned HTTP %d without the expected capture-expired rejection", res.StatusCode)
	}
	attempt.Admission = "capture_expired"
	return nil
}

type mixedLoadResponse struct {
	conn   *net.TCPConn
	res    *http.Response
	cancel context.CancelFunc
}

func (s *mixedLoadResponse) close() error {
	if s.cancel == nil {
		return nil
	}
	s.cancel()
	s.cancel = nil
	return s.res.Body.Close()
}

func (r *mixedLoadRun) startSlow(ctx context.Context, w *walk, index int) (_ *mixedLoadResponse, err error) {
	const startup = 4 * time.Second
	start := time.Now()
	attempt := queryAttempt{Admission: "unknown", Phase: "headers", Budget: startup}
	defer func() {
		attempt.Elapsed = time.Since(start)
		recordQuery(r.report.Queries, fmt.Sprintf("slow_resume_%d", index+1), attempt, err)
	}()
	// Use a separate HTTP/1 connection and constrain its window before
	// connecting. As in the release saturation probe, receiving headers
	// alone is insufficient evidence that the server is still writing.
	dialer := net.Dialer{Timeout: startup, Control: func(_, _ string, socket syscall.RawConn) error {
		var optionErr error
		err := socket.Control(func(fd uintptr) {
			optionErr = syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_RCVBUF, 4096)
		})
		return errors.Join(err, optionErr)
	}}
	transport := &http.Transport{
		DialContext: dialer.DialContext, DisableKeepAlives: true, DisableCompression: true,
		ResponseHeaderTimeout: startup, MaxResponseHeaderBytes: 64 << 10,
	}
	slowCtx, cancel := context.WithCancel(r.lifetime)
	stopStartup := context.AfterFunc(ctx, cancel)
	defer stopStartup()
	slow := &mixedLoadResponse{cancel: cancel}
	defer func() {
		if err != nil {
			cancel()
			if slow.res != nil {
				err = errors.Join(err, slow.res.Body.Close())
			}
		}
	}()
	trace := &httptrace.ClientTrace{GotConn: func(info httptrace.GotConnInfo) {
		slow.conn, _ = info.Conn.(*net.TCPConn)
	}}
	req, err := http.NewRequestWithContext(httptrace.WithClientTrace(slowCtx, trace), http.MethodGet,
		r.base+mixedLoadQuery(w.filter, w.cursor, w.pageLimit), nil)
	if err != nil {
		return nil, err
	}
	slow.res, err = transport.RoundTrip(req)
	if err != nil {
		return nil, err
	}
	attempt.observeHeaders(slow.res, time.Since(start))
	if attempt.Admission != "ndjson_response" {
		read, stop := context.WithTimeout(ctx, startup)
		defer stop()
		stopRead := context.AfterFunc(read, cancel)
		defer stopRead()
		data, readErr := io.ReadAll(io.LimitReader(slow.res.Body, 64<<10+1))
		if len(data) <= 64<<10 && readErr == nil {
			mixedLoadRejection(&attempt, slow.res, data)
		}
		attempt.BodyBytes = len(data)
		return nil, errors.Join(fmt.Errorf("slow continuation HTTP %d (%s)", slow.res.StatusCode, attempt.Admission), readErr)
	}
	if slow.conn == nil || slow.res.ContentLength == 0 {
		return nil, errors.New("slow continuation did not start a nonempty TCP response")
	}
	return slow, nil
}

func (r *mixedLoadRun) requireStalls(ctx context.Context, first int) error {
	for i := first; i < len(r.slow); i++ {
		stalled, err := r.observer.Stalled(ctx, r.slow[i].conn)
		if err != nil {
			return fmt.Errorf("observe backpressure on slow response %d: %w", i+1, err)
		}
		if !stalled {
			return fmt.Errorf("slow response %d lost backpressure before release", i+1)
		}
	}
	return ctx.Err()
}

func (r *mixedLoadRun) drainResponse(ctx context.Context, w *walk) error {
	slow := r.slow[0]
	if err := slow.conn.SetReadBuffer(1 << 20); err != nil {
		return err
	}
	s := r.service
	client := *s.client
	// Feed the original on-wire response through the regular walk's body,
	// framing and ledger validation. This sends no second HTTP request.
	used := false
	client.Transport = mixedLoadTransport(func(*http.Request) (*http.Response, error) {
		used = true
		return slow.res, nil
	})
	s.client = &client
	w.phase = "slow_response_body"
	drain, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	stop := context.AfterFunc(drain, slow.cancel)
	defer stop()
	_, err := w.page(drain, s, r.fixture)
	if !used {
		return errors.Join(err, slow.close(), drain.Err())
	}
	// readPage closed the body. Release the request context without
	// closing the body a second time.
	slow.cancel()
	slow.cancel = nil
	return errors.Join(err, drain.Err())
}

func (r *mixedLoadRun) cleanup(ctx context.Context) error {
	var errs []error
	for _, slow := range r.slow {
		errs = append(errs, slow.close())
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	for w := range r.cursors {
		if err := r.retire(ctx, w); err != nil {
			errs = append(errs, fmt.Errorf("retire outstanding capture: %w", err))
		}
	}
	return errors.Join(errs...)
}

func mixedLoadZeroWindow(ctx context.Context, table, server, peer string) (bool, error) {
	for line := range strings.SplitSeq(table, "\n") {
		fields := strings.Fields(line)
		if len(fields) < 3 || fields[1] != server || fields[2] != peer {
			continue
		}
		if len(fields) < 6 {
			return false, errors.New("incomplete candidate TCP socket entry")
		}
		queue, _, queueOK := strings.Cut(fields[4], ":")
		timer, _, timerOK := strings.Cut(fields[5], ":")
		tx, err := strconv.ParseUint(queue, 16, 32)
		if err != nil || !queueOK || !timerOK {
			return false, errors.New("invalid candidate TCP queue or timer")
		}
		return fields[3] == "01" && timer == "04" && tx > 0, ctx.Err()
	}
	return false, ctx.Err()
}
