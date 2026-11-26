package httpapi

import (
	"io"
	"net/http"
	"strconv"
)

func writeQueryRejection(w http.ResponseWriter, message string, status int) {
	body := message + "\n"
	header := w.Header()
	header.Set("Content-Type", "text/plain; charset=utf-8")
	header.Set("Content-Length", strconv.Itoa(len(body)))
	header.Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(status)
	if n, err := w.Write([]byte(body)); err != nil || n != len(body) {
		// A disconnected client can detect an incomplete error body by length.
		return
	}
}

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
