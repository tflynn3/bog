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
