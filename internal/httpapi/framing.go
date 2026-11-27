package httpapi

import (
	"io"
	"net/http"
	"strconv"
)

// writeQueryPage commits the headers and the finalized NDJSON body together.
// A reader can use the declared length to detect an interrupted write even
// when the connection closes immediately after a complete record.
func writeQueryPage(w http.ResponseWriter, body []byte, nextCursor string) error {
	setQueryPageHeaders(w.Header(), len(body), nextCursor)
	n, err := w.Write(body)
	if err == nil && n != len(body) {
		return io.ErrShortWrite
	}
	return err
}

func setQueryPageHeaders(header http.Header, size int, nextCursor string) {
	header.Set("Content-Type", "application/x-ndjson")
	header.Set("Content-Length", strconv.Itoa(size))
	if nextCursor != "" {
		header.Set("Bog-Next-Cursor", nextCursor)
	}
}

// Budget rejection is a complete framed response even when a middleware
// flushes the headers before the body write.
func writeQueryBudgetError(w http.ResponseWriter) {
	const body = "query work budget exhausted\n"
	header := w.Header()
	header.Set("Content-Type", "text/plain; charset=utf-8")
	header.Set("X-Content-Type-Options", "nosniff")
	header.Set("Content-Length", strconv.Itoa(len(body)))
	w.WriteHeader(http.StatusServiceUnavailable)
	if n, err := io.WriteString(w, body); err != nil || n != len(body) {
		return // Content-Length lets the client detect a partial write.
	}
}
