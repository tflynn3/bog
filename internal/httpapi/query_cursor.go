package httpapi

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"

	"bog/internal/store"
)

const (
	cursorVersion     = 1
	cursorPayloadSize = 1 + 8 + sha256.Size + 2 + 2 + sha256.Size
	cursorSize        = cursorPayloadSize + sha256.Size
)

var cursorTokenSize = base64.RawURLEncoding.EncodedLen(cursorSize)

// Cursor encoding and authentication process bounded tokens and positions in
// blocks. The base token charge covers fixed setup, including HMAC padding.
var cursorTokenWork = 1 + cursorBlockWork(cursorTokenSize)

func cursorBlockWork(size int) uint64 {
	return (uint64(size) + sha256.BlockSize - 1) / sha256.BlockSize
}

type cursorData struct {
	Capture uint64
	Filter  [sha256.Size]byte
	Scan    uint16
	Offset  uint16
	After   [sha256.Size]byte
}

func positionDigest(position PagePosition) [sha256.Size]byte {
	return sha256.Sum256([]byte(position))
}

func (a *API) filterDigest(filter PageFilter) [sha256.Size]byte {
	// Timestamp seconds and nanoseconds distinguish the full accepted date
	// range and are independent of the client's time zone spelling.
	var bounds [24]byte
	binary.BigEndian.PutUint64(bounds[:8], uint64(filter.Start.Unix()))
	binary.BigEndian.PutUint32(bounds[8:12], uint32(filter.Start.Nanosecond()))
	binary.BigEndian.PutUint64(bounds[12:20], uint64(filter.End.Unix()))
	binary.BigEndian.PutUint32(bounds[20:], uint32(filter.End.Nanosecond()))
	mac := hmac.New(sha256.New, a.cursorKey[:])
	mac.Write([]byte("bog-filter-v1\x00"))
	mac.Write(bounds[:])
	mac.Write([]byte(filter.Contains))
	var digest [sha256.Size]byte
	copy(digest[:], mac.Sum(nil))
	return digest
}

func (a *API) cursorMAC(payload []byte) []byte {
	mac := hmac.New(sha256.New, a.cursorKey[:])
	mac.Write([]byte("bog-cursor-v1\x00"))
	mac.Write(payload)
	return mac.Sum(nil)
}

func (a *API) encodeCursor(c cursorData) string {
	var b [cursorSize]byte
	b[0] = cursorVersion
	binary.BigEndian.PutUint64(b[1:9], c.Capture)
	copy(b[9:41], c.Filter[:])
	binary.BigEndian.PutUint16(b[41:43], c.Scan)
	binary.BigEndian.PutUint16(b[43:45], c.Offset)
	copy(b[45:cursorPayloadSize], c.After[:])
	copy(b[cursorPayloadSize:], a.cursorMAC(b[:cursorPayloadSize]))
	return base64.RawURLEncoding.EncodeToString(b[:])
}

func (a *API) decodeCursor(token string) (cursorData, error) {
	var c cursorData
	if len(token) != cursorTokenSize {
		return c, store.ErrInvalidQueryCursor
	}
	var b [cursorSize]byte
	n, err := base64.RawURLEncoding.Strict().Decode(b[:], []byte(token))
	if err != nil || n != len(b) || b[0] != cursorVersion ||
		!hmac.Equal(b[cursorPayloadSize:], a.cursorMAC(b[:cursorPayloadSize])) {
		return c, store.ErrInvalidQueryCursor
	}
	c.Capture = binary.BigEndian.Uint64(b[1:9])
	copy(c.Filter[:], b[9:41])
	c.Scan = binary.BigEndian.Uint16(b[41:43])
	c.Offset = binary.BigEndian.Uint16(b[43:45])
	copy(c.After[:], b[45:cursorPayloadSize])
	return c, nil
}
