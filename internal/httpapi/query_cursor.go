package httpapi

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"strings"
	"time"
	"unicode/utf8"

	"bog/internal/store"
)

const (
	cursorVersion     = 1
	cursorHeaderSize  = 1 + 8 + 8 + 4 + 2
	maxCursorPosition = 1024
	maxCursorLength   = (4*(cursorHeaderSize+maxCursorPosition+sha256.Size) + 2) / 3
)

type cursorData struct {
	capture  uint64
	deadline time.Time
	window   uint32
	offset   uint16
	after    PagePosition
}

func (a *API) filterDigest(filter PageFilter) [sha256.Size]byte {
	// UTC RFC3339Nano timestamps cannot contain the delimiter.
	digest := sha256.New()
	digest.Write(filter.Start.AppendFormat(nil, time.RFC3339Nano))
	digest.Write([]byte{0})
	digest.Write(filter.End.AppendFormat(nil, time.RFC3339Nano))
	digest.Write([]byte{0})
	digest.Write([]byte(filter.Contains))
	var out [sha256.Size]byte
	digest.Sum(out[:0])
	return out
}

func (a *API) cursorMAC(payload []byte, filter [sha256.Size]byte) []byte {
	mac := hmac.New(sha256.New, a.cursorKey[:])
	mac.Write([]byte("bog-query-cursor-v1\x00"))
	mac.Write(filter[:])
	mac.Write(payload)
	return mac.Sum(nil)
}

func validPagePosition(p PagePosition) bool {
	return p != "" && len(p) <= maxCursorPosition && utf8.ValidString(string(p))
}

func cursorUnits(positionBytes int) uint64 {
	return cursorTokenUnits(base64.RawURLEncoding.EncodedLen(cursorHeaderSize + positionBytes + sha256.Size))
}

// Cursor framing and authentication process the encoded token in bounded
// blocks. The independently sized filter literal is metered separately.
func cursorTokenUnits(tokenBytes int) uint64 {
	return 1 + uint64((tokenBytes+sha256.BlockSize-1)/sha256.BlockSize)
}

func (a *API) encodeCursor(c cursorData, filter [sha256.Size]byte) string {
	payload := make([]byte, cursorHeaderSize, cursorHeaderSize+len(c.after)+sha256.Size)
	payload[0] = cursorVersion
	binary.BigEndian.PutUint64(payload[1:9], c.capture)
	binary.BigEndian.PutUint64(payload[9:17], uint64(c.deadline.UnixNano()))
	binary.BigEndian.PutUint32(payload[17:21], c.window)
	binary.BigEndian.PutUint16(payload[21:23], c.offset)
	payload = append(payload, c.after...)
	return base64.RawURLEncoding.EncodeToString(append(payload, a.cursorMAC(payload, filter)...))
}

func (a *API) decodeCursor(token string, filter [sha256.Size]byte) (cursorData, error) {
	if len(token) > maxCursorLength || strings.ContainsAny(token, "\r\n") {
		return cursorData{}, store.ErrInvalidQueryCursor
	}
	raw, err := base64.RawURLEncoding.Strict().DecodeString(token)
	if err != nil || len(raw) <= cursorHeaderSize+sha256.Size {
		return cursorData{}, store.ErrInvalidQueryCursor
	}
	payload, sig := raw[:len(raw)-sha256.Size], raw[len(raw)-sha256.Size:]
	after := PagePosition(payload[cursorHeaderSize:])
	if payload[0] != cursorVersion || !hmac.Equal(sig, a.cursorMAC(payload, filter)) || !validPagePosition(after) {
		return cursorData{}, store.ErrInvalidQueryCursor
	}
	return cursorData{
		capture:  binary.BigEndian.Uint64(payload[1:9]),
		deadline: time.Unix(0, int64(binary.BigEndian.Uint64(payload[9:17]))),
		window:   binary.BigEndian.Uint32(payload[17:21]),
		offset:   binary.BigEndian.Uint16(payload[21:23]),
		after:    after,
	}, nil
}
