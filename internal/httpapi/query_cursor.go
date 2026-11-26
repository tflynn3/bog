package httpapi

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"strings"
	"time"

	"bog/internal/store"
)

const (
	cursorPayloadBytes  = 1 + 8 + 4 + 2 + sha256.Size
	maxCursorTokenBytes = ((cursorPayloadBytes+sha256.Size)*8 + 5) / 6
)

type cursorData struct {
	capture  uint64
	block    uint32
	offset   uint16 // index of the last delivered row within block
	position [sha256.Size]byte
}

func (a *API) filterDigest(filter PageFilter) [sha256.Size]byte {
	mac := hmac.New(sha256.New, a.cursorKey[:])
	mac.Write([]byte("bog-filter-v1\x00"))
	for _, stamp := range [...]time.Time{filter.Start, filter.End} {
		mac.Write(stamp.UTC().AppendFormat(nil, time.RFC3339Nano))
		mac.Write([]byte{0})
	}
	mac.Write([]byte(filter.Contains))
	var digest [sha256.Size]byte
	mac.Sum(digest[:0])
	return digest
}

func (a *API) cursorMAC(payload []byte, filter [sha256.Size]byte) []byte {
	mac := hmac.New(sha256.New, a.cursorKey[:])
	mac.Write([]byte("bog-cursor-v2\x00"))
	mac.Write(filter[:])
	mac.Write(payload)
	return mac.Sum(nil)
}

func (a *API) encodeCursor(c cursorData, filter [sha256.Size]byte) string {
	payload := make([]byte, 0, cursorPayloadBytes+sha256.Size)
	payload = append(payload, 2)
	payload = binary.BigEndian.AppendUint64(payload, c.capture)
	payload = binary.BigEndian.AppendUint32(payload, c.block)
	payload = binary.BigEndian.AppendUint16(payload, c.offset)
	payload = append(payload, c.position[:]...)
	return base64.RawURLEncoding.EncodeToString(append(payload, a.cursorMAC(payload, filter)...))
}

func (a *API) decodeCursor(token string, filter [sha256.Size]byte) (cursorData, error) {
	var c cursorData
	if len(token) != maxCursorTokenBytes || strings.ContainsAny(token, "\r\n") {
		return c, store.ErrInvalidQueryCursor
	}
	b, err := base64.RawURLEncoding.Strict().DecodeString(token)
	if err != nil || len(b) != cursorPayloadBytes+sha256.Size || b[0] != 2 {
		return c, store.ErrInvalidQueryCursor
	}
	payload, signature := b[:cursorPayloadBytes], b[cursorPayloadBytes:]
	if !hmac.Equal(signature, a.cursorMAC(payload, filter)) {
		return c, store.ErrInvalidQueryCursor
	}
	c.capture = binary.BigEndian.Uint64(payload[1:9])
	c.block = binary.BigEndian.Uint32(payload[9:13])
	c.offset = binary.BigEndian.Uint16(payload[13:15])
	copy(c.position[:], payload[15:])
	return c, nil
}

func chargeCursorValidation(work *store.QueryWork, literalBytes int) error {
	return work.Charge(1, cursorTokenWork, uint64(literalBytes))
}

// Cursor framing and hashing process bounded blocks. The filter literal is
// charged separately because its length is independent of the token length.
const cursorTokenWork = (maxCursorTokenBytes + sha256.BlockSize - 1) / sha256.BlockSize

func cursorPositionWork(position PagePosition) uint64 {
	return (uint64(len(position)) + sha256.BlockSize - 1) / sha256.BlockSize
}
