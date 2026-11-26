package httpapi

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"time"

	"bog/internal/store"
)

const maxCursorTokenBytes = 2048

type cursorData struct {
	Version   int          `json:"v"`
	Start     time.Time    `json:"start"`
	End       time.Time    `json:"end"`
	Contains  string       `json:"contains"`
	Capture   string       `json:"capture"`
	After     PagePosition `json:"after"`
	ExpiresAt int64        `json:"expires"`
}

func (a *API) containsDigest(value string) string {
	mac := hmac.New(sha256.New, a.cursorKey[:])
	mac.Write([]byte("bog-filter-v1\x00"))
	mac.Write([]byte(value))
	return base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}

func (a *API) encodeCursor(c cursorData) (string, error) {
	b, err := json.Marshal(c)
	if err != nil {
		return "", err
	}
	if base64.RawURLEncoding.EncodedLen(len(b)+sha256.Size) > maxCursorTokenBytes {
		return "", errors.New("cursor payload too large")
	}
	mac := hmac.New(sha256.New, a.cursorKey[:])
	mac.Write([]byte("bog-cursor-v1\x00"))
	mac.Write(b)
	return base64.RawURLEncoding.EncodeToString(append(b, mac.Sum(nil)...)), nil
}
func (a *API) decodeCursor(token string) (cursorData, error) {
	var c cursorData
	if len(token) > maxCursorTokenBytes {
		return c, store.ErrInvalidQueryCursor
	}
	b, err := base64.RawURLEncoding.Strict().DecodeString(token)
	if err != nil || len(b) <= sha256.Size {
		return c, store.ErrInvalidQueryCursor
	}
	payload, sig := b[:len(b)-sha256.Size], b[len(b)-sha256.Size:]
	mac := hmac.New(sha256.New, a.cursorKey[:])
	mac.Write([]byte("bog-cursor-v1\x00"))
	mac.Write(payload)
	if !hmac.Equal(sig, mac.Sum(nil)) || json.Unmarshal(payload, &c) != nil || c.Version != 1 {
		return c, store.ErrInvalidQueryCursor
	}
	return c, nil
}

func (c cursorData) expired() bool {
	return time.Now().UnixNano() >= c.ExpiresAt
}

func chargeCursorValidation(work *store.QueryWork, tokenBytes, literalBytes int) error {
	// A cursor costs one validation unit plus its encoded and literal bytes.
	return work.Charge(1, uint64(tokenBytes), uint64(literalBytes))
}

func (a *API) authenticateCursor(token string, filter PageFilter, work *store.QueryWork) (cursorData, error) {
	if err := chargeCursorValidation(work, len(token), len(filter.Contains)); err != nil {
		return cursorData{}, err
	}
	cursor, err := a.decodeCursor(token)
	if err != nil {
		return cursorData{}, err
	}
	if !cursor.Start.Equal(filter.Start) || !cursor.End.Equal(filter.End) ||
		cursor.Contains != a.containsDigest(filter.Contains) || cursor.Capture == "" || cursor.After == "" {
		return cursorData{}, store.ErrInvalidQueryCursor
	}
	if cursor.expired() {
		return cursorData{}, ErrCaptureExpired
	}
	return cursor, nil
}
