package client

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"strings"
)

// queryReadBlock caps the read and staging size independently of both the
// advertised length and the caller's byte ceiling. Most records can be decoded
// directly from this block. A record spanning blocks uses separate scratch
// space that grows on demand.
const queryReadBlock = 16 * 1024

// queryFrame consumes the encoded payload exposed by net/http. For HTTP/1
// chunked delivery and HTTP/2, the transport owns termination and trailer
// decoding. In all modes, a transport EOF is required even after the expected
// bytes have arrived. In particular, running out of byte budget cannot stand
// in for an EOF from the body.
//
// The frame tracks bytes and the transport error independently. A single Read
// may report both the overflow probe byte and an error. Keeping both lets the
// caller reject the page as oversized and still inspect the underlying error.
// The response owner closes the body after decoding, including on early exit.
type queryFrame struct {
	body     io.Reader
	declared int64
	limit    int64
	received int64

	// end is set by a terminal read, cancellation, or a read that proves
	// overflow. A terminal read can also return a nonempty payload prefix.
	end     bool
	readErr error
}

func newQueryFrame(resp *http.Response, limit int64) (queryFrame, error) {
	if limit < 0 || limit == math.MaxInt64 {
		return queryFrame{}, errors.New("invalid response size limit")
	}
	if !framed(resp) {
		return queryFrame{}, fmt.Errorf("%w: query response has unverifiable framing", ErrMalformedPage)
	}
	if err := checkQueryEncoding(resp); err != nil {
		return queryFrame{}, err
	}
	if resp.ContentLength > limit {
		return queryFrame{}, pageSizeError{}
	}
	return queryFrame{
		body:     resp.Body,
		declared: resp.ContentLength,
		limit:    limit,
	}, nil
}

func checkQueryEncoding(resp *http.Response) error {
	// Payload bytes are both the unit of the limit and the input to the
	// record parser. Transparent decompression makes the former unverifiable.
	values := resp.Header.Values("Content-Encoding")
	if resp.Uncompressed || len(values) > 1 || (len(values) == 1 && !strings.EqualFold(values[0], "identity")) {
		return fmt.Errorf("%w: unsupported query content encoding", ErrMalformedPage)
	}
	return nil
}

func (f *queryFrame) blockSize() int {
	// Most Bog responses have a known length. Give small pages a smaller
	// buffer while leaving space to read efficiently if the body runs past
	// its declaration. A valid page can require the extra overflow probe.
	size := min(int64(queryReadBlock), f.limit+1)
	if f.declared >= 0 {
		size = min(size, max(int64(512), f.declared+1))
	}
	return int(size)
}

// read fills part of a nonempty block and returns the bytes eligible for record
// decoding. The slice is only valid until the next read. An empty slice means
// reading is finished. Check result after the loop even if the final block
// ended on a record boundary. Bytes returned with a failing terminal read
// still count toward the limit, but cannot contribute to a validated page.
func (f *queryFrame) read(ctx context.Context, block []byte) []byte {
	if f.end {
		return nil
	}
	remaining := f.limit - f.received
	// The budget is validated before construction and cannot wrap here.
	// Probe with exactly one byte when remaining == 0.
	window := block[:min(int64(len(block)), remaining+1)]
	n, err := readQueryBlock(ctx, f.body, window)
	f.received += int64(n)

	f.end = err != nil || f.received > f.limit
	if err != io.EOF {
		f.readErr = err
	}
	if f.received > f.limit || f.readErr != nil {
		return nil
	}
	return window[:n]
}

// readQueryBlock checks cancellation between reads without starting a reader
// goroutine. A blocked transport read is governed by the HTTP request context.
// An occasional (0, nil) does not establish termination, but a reader that
// repeatedly makes no progress must fail instead of spinning indefinitely.
func readQueryBlock(ctx context.Context, body io.Reader, dst []byte) (int, error) {
	const maxEmptyReads = 100 // Same consecutive empty-read bound as bufio.Reader.
	for range maxEmptyReads {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		n, err := body.Read(dst)
		if n < 0 || n > len(dst) {
			return 0, joinQueryErrors(fmt.Errorf("invalid query body read count %d for %d bytes", n, len(dst)), err)
		}
		if n > 0 || err != nil {
			return n, err
		}
	}
	return 0, io.ErrNoProgress
}

// result is called only after a terminal read. Check overflow first, then
// delivery, then the declared length. This is the order used when the entire
// body was validated before decoding. A short, otherwise clean EOF on a
// fixed-length body is a truncation even at an NDJSON record boundary.
func (f *queryFrame) result(ctx context.Context) error {
	var deliveryErr error
	if f.readErr != nil {
		deliveryErr = interruptedPage(ctx, f.readErr)
	}
	if f.received > f.limit {
		if deliveryErr != nil {
			return errors.Join(pageSizeError{}, deliveryErr)
		}
		return pageSizeError{}
	}
	if deliveryErr != nil {
		return deliveryErr
	}
	if f.declared < 0 || f.received == f.declared {
		return nil
	}
	if f.received < f.declared {
		return fmt.Errorf("%w: received %d of %d declared bytes: %w",
			ErrPageInterrupted, f.received, f.declared, io.ErrUnexpectedEOF)
	}
	return fmt.Errorf("%w: received %d bytes with Content-Length %d",
		ErrMalformedPage, f.received, f.declared)
}
