package httpapi

import (
	"bytes"
	"io"
	"net/http"
	"strconv"
)

// writeQueryBody commits a finalized NDJSON body. The explicit length lets an
// HTTP/1.0 client distinguish EOF at the end of a page from a disconnected peer.
func writeQueryBody(w http.ResponseWriter, body *bytes.Buffer) error {
	size := body.Len()
	w.Header().Set("Content-Length", strconv.Itoa(size))
	n, err := w.Write(body.Bytes())
	if err == nil && n != size {
		return io.ErrShortWrite
	}
	return err
}
