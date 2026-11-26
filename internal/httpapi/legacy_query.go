package httpapi

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"time"

	"bog/internal/store"
)

const legacyCursorReservation = 2048

type legacyCursorData struct {
	Version  int               `json:"v"`
	Start    time.Time         `json:"start"`
	End      time.Time         `json:"end"`
	Contains [sha256.Size]byte `json:"contains"`
	Count    int               `json:"count"`
	After    store.Position    `json:"after"`
}

func (a *API) legacyContainsDigest(value string) [sha256.Size]byte {
	mac := hmac.New(sha256.New, a.cursorKey[:])
	mac.Write([]byte("bog-filter-v1\x00"))
	mac.Write([]byte(value))
	var digest [sha256.Size]byte
	copy(digest[:], mac.Sum(nil))
	return digest
}

func (a *API) encodeLegacyCursor(c legacyCursorData) (string, error) {
	b, err := json.Marshal(c)
	if err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(append(b, a.cursorMAC(b)...)), nil
}

func (a *API) decodeLegacyCursor(token string) (legacyCursorData, error) {
	var c legacyCursorData
	if len(token) > base64.RawURLEncoding.EncodedLen(2048) {
		return c, store.ErrInvalidQueryCursor
	}
	b, err := base64.RawURLEncoding.Strict().DecodeString(token)
	if err != nil || len(b) <= sha256.Size || len(b) > 2048 {
		return c, store.ErrInvalidQueryCursor
	}
	payload, sig := b[:len(b)-sha256.Size], b[len(b)-sha256.Size:]
	if !hmac.Equal(sig, a.cursorMAC(payload)) || json.Unmarshal(payload, &c) != nil || c.Version != 1 {
		return c, store.ErrInvalidQueryCursor
	}
	return c, nil
}

// legacySnapshotQuery preserves the in-memory server's per-page work contract
// until the retained runtime installs its PageSource.
func (a *API) legacySnapshotQuery(w http.ResponseWriter, params queryParams, work *store.QueryWork) {
	start, end, contains := params.filter.Start, params.filter.End, params.filter.Contains
	page, limit := params.page, params.limit
	var rows []store.Row
	var captured int
	var anchor, next *store.Position
	var digest [sha256.Size]byte
	var err error
	var count *int
	if params.cursor != "" {
		// Charge one unit for token validation and a unit per input byte
		// for decoding and authenticating the token and its literal.
		if err := work.Charge(1, uint64(len(params.cursor)), uint64(len(contains))); err != nil {
			writeQueryError(w, err)
			return
		}
		c, err := a.decodeLegacyCursor(params.cursor)
		if err != nil {
			writeQueryError(w, err)
			return
		}
		digest = a.legacyContainsDigest(contains)
		if !c.Start.Equal(start) || !c.End.Equal(end) || c.Contains != digest {
			writeQueryError(w, store.ErrInvalidQueryCursor)
			return
		}
		count, anchor = &c.Count, &c.After
	}
	if page {
		// The one-row call captures both the snapshot and a usable anchor.
		rows, captured, next, err = a.legacyQueryStore.QueryPageWithWork(start, end, contains, 1, nil, nil, work)
		if err == nil && next != nil && limit > 1 {
			anchor = next
			var rest []store.Row
			rest, _, next, err = a.legacyQueryStore.QueryPageWithWork(start, end, contains, limit-1, &captured, anchor, work)
			rows = append(rows, rest...)
		}
	} else {
		rows, captured, next, err = a.legacyQueryStore.QueryPageWithWork(start, end, contains, limit, count, anchor, work)
	}
	if err != nil {
		writeQueryError(w, err)
		return
	}
	response := queryResponse{maxBytes: a.maxResponseBytes}
	emitted := 0
	for _, row := range rows {
		fits, required, err := response.appendRow(row, work)
		if err != nil {
			writeQueryError(w, err)
			return
		}
		if !fits {
			if emitted == 0 {
				writeQueryError(w, &oversizedQueryRecord{a.maxResponseBytes, required})
				return
			}
			break
		}
		emitted++
	}
	if emitted < len(rows) {
		// QueryPage only exposes a position when another matching row exists.
		// Since a fetched row was omitted, the shorter call has that lookahead.
		shortLimit := emitted
		if page {
			shortLimit--
		}
		if shortLimit == 0 {
			next = anchor
		} else {
			_, _, next, err = a.legacyQueryStore.QueryPageWithWork(start, end, contains, shortLimit, &captured, anchor, work)
			if err != nil {
				writeQueryError(w, err)
				return
			}
		}
	}
	var token string
	if next != nil {
		if page {
			if err := work.Charge(uint64(len(contains))); err != nil {
				writeQueryError(w, err)
				return
			}
			digest = a.legacyContainsDigest(contains)
		}
		token, err = a.encodeLegacyCursor(legacyCursorData{1, start, end, digest, captured, *next})
		if err != nil {
			writeQueryError(w, err)
			return
		}
	}
	if err := response.reserveDelivery(work); err != nil {
		writeQueryError(w, err)
		return
	}
	response.write(w, token)
}
