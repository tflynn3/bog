package httpapi

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"math"
	"net/http"
	"strconv"
	"time"

	"bog/internal/store"
)

const (
	snapshotTagSize = 16 // HMAC-SHA256 truncated to 128 bits.
	snapshotTagMin  = snapshotTagSize + 2
	snapshotTagMax  = snapshotTagSize + 2*binary.MaxVarintLen64
	snapshotBounds  = 2 * (8 + 4) // Unix seconds and nanoseconds for each bound.
)

var snapshotEncoding = base64.RawURLEncoding.Strict()

type snapshotQuery struct {
	start, end time.Time
	contains   string
}

// snapshotKey derives a key for the semantic query. Equivalent timestamps
// bind to the same key, and the literal occupies the remainder of the input.
func (a *API) snapshotKey(query snapshotQuery, work *store.QueryWork) ([sha256.Size]byte, error) {
	var key [sha256.Size]byte
	if err := work.Charge(snapshotBounds, uint64(len(query.contains))); err != nil {
		return key, err
	}
	var bounds [snapshotBounds]byte
	binary.BigEndian.PutUint64(bounds[:8], uint64(query.start.Unix()))
	binary.BigEndian.PutUint32(bounds[8:12], uint32(query.start.Nanosecond()))
	binary.BigEndian.PutUint64(bounds[12:20], uint64(query.end.Unix()))
	binary.BigEndian.PutUint32(bounds[20:], uint32(query.end.Nanosecond()))
	mac := hmac.New(sha256.New, a.cursorKey[:])
	mac.Write([]byte("bog-snapshot-query-v2\x00"))
	mac.Write(bounds[:])
	mac.Write([]byte(query.contains))
	copy(key[:], mac.Sum(nil))
	return key, nil
}

func snapshotTag(key [sha256.Size]byte, payload []byte) []byte {
	mac := hmac.New(sha256.New, key[:])
	mac.Write([]byte("bog-snapshot-cursor-v2\x00"))
	mac.Write(payload)
	return mac.Sum(nil)[:snapshotTagSize]
}

func encodeSnapshotCursor(key [sha256.Size]byte, cursor store.SnapshotCursor) string {
	var buf [snapshotTagMax]byte
	payload := binary.AppendUvarint(buf[:0], uint64(cursor.Count))
	payload = binary.AppendUvarint(payload, uint64(cursor.Ordinal))
	return snapshotEncoding.EncodeToString(append(payload, snapshotTag(key, payload)...))
}

func decodeSnapshotCursor(key [sha256.Size]byte, token string, work *store.QueryWork) (store.SnapshotCursor, error) {
	var cursor store.SnapshotCursor
	if len(token) < snapshotEncoding.EncodedLen(snapshotTagMin) ||
		len(token) > snapshotEncoding.EncodedLen(snapshotTagMax) {
		return cursor, store.ErrInvalidQueryCursor
	}
	// Reserve decoding and authentication before inspecting the token.
	if err := work.Charge(1, uint64(len(token)), uint64(snapshotEncoding.DecodedLen(len(token)))); err != nil {
		return cursor, err
	}
	var buf [snapshotTagMax]byte
	n, err := snapshotEncoding.Decode(buf[:], []byte(token))
	if err != nil || n < snapshotTagMin || snapshotEncoding.EncodedLen(n) != len(token) {
		return cursor, store.ErrInvalidQueryCursor
	}
	payload, tag := buf[:n-snapshotTagSize], buf[n-snapshotTagSize:n]
	if !hmac.Equal(tag, snapshotTag(key, payload)) {
		return cursor, store.ErrInvalidQueryCursor
	}
	count, first := binary.Uvarint(payload)
	if first <= 0 || first != uvarintSize(count) || count > math.MaxInt {
		return cursor, store.ErrInvalidQueryCursor
	}
	ordinal, second := binary.Uvarint(payload[first:])
	if second <= 0 || second != uvarintSize(ordinal) || first+second != len(payload) || ordinal >= count {
		return cursor, store.ErrInvalidQueryCursor
	}
	return store.SnapshotCursor{Count: int(count), Ordinal: int(ordinal)}, nil
}

func uvarintSize(value uint64) int {
	var buf [binary.MaxVarintLen64]byte
	return binary.PutUvarint(buf[:], value)
}

// executeParsedQuery handles valid query parameters using the request's work
// budget through response commit.
func (a *API) executeParsedQuery(w http.ResponseWriter, query snapshotQuery, limit int, page bool, token string, work *store.QueryWork) {
	var (
		rows     []store.Row
		snapshot store.SnapshotPage
		key      [sha256.Size]byte
		err      error
	)
	paginated := page || token != ""
	if paginated {
		var cursor *store.SnapshotCursor
		if token != "" {
			key, err = a.snapshotKey(query, work)
			if err != nil {
				writeQueryError(w, err)
				return
			}
			decoded, err := decodeSnapshotCursor(key, token, work)
			if err != nil {
				writeQueryError(w, err)
				return
			}
			cursor = &decoded
		}
		snapshot, err = a.store.QuerySnapshotPage(query.start, query.end, query.contains, limit, cursor, work)
		rows = snapshot.Rows
	} else {
		rows, err = a.store.QueryWithWork(query.start, query.end, query.contains, limit, work)
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
				message := fmt.Sprintf("query record exceeds response byte limit (cap %d bytes, requires %d bytes)", a.maxResponseBytes, required)
				http.Error(w, message, http.StatusUnprocessableEntity)
				return
			}
			break
		}
		emitted++
	}

	var next string
	if paginated && (snapshot.More || emitted < len(rows)) {
		if page {
			key, err = a.snapshotKey(query, work)
			if err != nil {
				writeQueryError(w, err)
				return
			}
		}
		next = encodeSnapshotCursor(key, store.SnapshotCursor{Count: snapshot.Count, Ordinal: snapshot.Ordinals[emitted-1]})
	}
	delivery := (uint64(response.size) + queryDeliveryBytesPerUnit - 1) / queryDeliveryBytesPerUnit
	if err := work.Charge(delivery); err != nil {
		writeQueryError(w, err)
		return
	}
	if next != "" {
		w.Header().Set("Bog-Next-Cursor", next)
	}
	w.Header().Set("Content-Type", "application/x-ndjson")
	w.Header().Set("Content-Length", strconv.Itoa(response.size))
	w.WriteHeader(http.StatusOK)
	for _, record := range response.rows {
		if n, err := w.Write(record); err != nil || n != len(record) {
			// Headers are committed. A short body is detectable by the client.
			return
		}
	}
}
