// Package streampolicy checks the proposed shared-deployment stream policy
// against explicit observations from a disposable fixture target.
package streampolicy

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"slices"
	"strconv"
)

type Scope struct{ Tenant, Stream string }

type Target struct {
	Principal string
	Selector  string
}

type Row struct {
	Timestamp string `json:"ts"`
	Line      string `json:"line"`
}

type Record struct {
	Scope Scope
	Row   Row
}

// Response is a fully read observation. Body and Headers preserve the observed
// application response, including errors, with canonical header names and
// transport-only metadata (such as Date) excluded. Complete attests that the
// adapter checked framing and inspected the entire body and application headers.
type Response struct {
	Complete bool
	Status   int
	Body     []byte
	Headers  map[string][]string
}

type QueryRequest struct {
	Target
	Start, End string
	Contains   string
	Limit      int
	Cursor     string
}

type Page struct {
	Response
	Rows       []Row
	NextCursor string
}

type IngestRequest struct {
	Target
	Key string
	// Body is the exact validated-format input to submit, including JSON escapes.
	Body string
}

// Committed contains the complete committed byte stream and all decoded rows
// obtained by independently replaying it in append order. Never substitute a
// query result or a re-encoding of the rows for the observed committed bytes.
type Committed struct {
	Complete bool
	Bytes    []byte
	Records  []Record
}

type ExportStart struct {
	Response
	ArtifactID string
}

type Manifest struct {
	Scope          Scope
	Version        string
	PolicyVersions []string
}

type Artifact struct {
	Response
	// Complete response framing alone does not establish artifact integrity.
	// Verified attests that the complete artifact and authenticated manifest
	// were validated. Bytes are the original artifact, including metadata.
	Verified bool
	Bytes    []byte
	Manifest Manifest
	Records  []Record
}

// QueryObserver performs one request and observes rows and the opaque cursor
// independently of status. Rejected requests must expose any returned data.
type QueryObserver interface {
	Query(context.Context, QueryRequest) (Page, error)
}

// RetryObserver submits one request without transport-level retries. Capture the
// whole observable response whether the request committed, replayed, or failed.
type RetryObserver interface {
	Ingest(context.Context, IngestRequest) (Response, error)
}

// ReplayObserver inspects the entire committed fixture and reopens the target
// from it. Subsequent requests must reach the reopened target.
type ReplayObserver interface {
	RestartAndReplay(context.Context) (Committed, error)
}

// ExportObserver waits for the final create outcome and fetches an artifact as
// the supplied principal and stream. Both calls capture application failures
// as observations. Fetch reports any data exposed on failure.
type ExportObserver interface {
	CreateExport(context.Context, Target) (ExportStart, error)
	FetchExport(context.Context, Target, string) (Artifact, error)
}

// ExportState is the complete inventory of published fixture artifacts and the
// number of in-flight or staged exports. Each value is the SHA-256 of the
// original artifact bytes, including its authenticated manifest.
type ExportState struct {
	Complete  bool
	Artifacts map[string][sha256.Size]byte
	Pending   int
}

// ExportStateObserver independently inventories the target's export state.
// The Export adapter must also implement this interface. The runner calls it
// only when no requests are in flight.
type ExportStateObserver interface {
	ObserveExportState(context.Context) (ExportState, error)
}

// Adapters must share one fresh, exclusively owned ReferenceFixture target.
// Query, Ingest and FetchExport or CreateExport may run concurrently on that
// target, including concurrent calls to the same observer. Restarts and export
// inventories run after requests drain. Adapters honor context through I/O and
// cleanup, return
// errors for unsupported observations, and bound observations before reading
// them into memory. Returned observations must remain stable through Run.
// The caller owns the target lifecycle.
type Adapters struct {
	Query  QueryObserver
	Retry  RetryObserver
	Replay ReplayObserver
	Export ExportObserver
}

var (
	ErrIncomplete = errors.New("stream policy evidence incomplete")
	ErrViolation  = errors.New("stream policy contract violated")
)

// MaxEvidenceBytes caps each captured body or durable byte surface.
const MaxEvidenceBytes = 1 << 20

const anySuccessStatus = 0

func requireResponse(response Response, status int) error {
	if !response.Complete || response.Status < 100 || response.Status > 599 || len(response.Body) > MaxEvidenceBytes {
		return ErrIncomplete
	}
	if status == anySuccessStatus && response.Status/100 != 2 {
		return fmt.Errorf("%w: status %d, want a successful export", ErrViolation, response.Status)
	}
	if status != anySuccessStatus && response.Status != status {
		return fmt.Errorf("%w: status %d, want %d", ErrViolation, response.Status, status)
	}
	if response.Status == http.StatusNoContent && len(response.Body) != 0 {
		return fmt.Errorf("%w: 204 response carried a body", ErrViolation)
	}
	return nil
}

func sameResponse(got, want Response) bool {
	return got.Status == want.Status && bytes.Equal(got.Body, want.Body) &&
		maps.EqualFunc(got.Headers, want.Headers, slices.Equal)
}

func compareRows(got, want []Row) error {
	if !slices.Equal(got, want) {
		return fmt.Errorf("%w: rows differ from fixture (got %d, want %d)", ErrViolation, len(got), len(want))
	}
	return nil
}

// jsonUnescaped normalizes JSON escapes anywhere within a byte surface, including
// a JSON string carried inside binary framing. The original bytes are checked
// too. Adapters also check framing and supply independently decoded records.
func jsonUnescaped(data []byte) []byte {
	if !bytes.ContainsRune(data, '\\') {
		return data
	}
	var escape [14]byte
	escape[0] = '"'
	decoded := make([]byte, 0, len(data))
	for i := 0; i < len(data); {
		n := 0
		if data[i] == '\\' && i+1 < len(data) {
			n = 2
			if data[i+1] == 'u' {
				n = 6
				if i+12 <= len(data) && bytes.HasPrefix(data[i+6:], []byte("\\u")) {
					first, err := strconv.ParseUint(string(data[i+2:i+6]), 16, 16)
					if err == nil && 0xd800 <= first && first <= 0xdbff {
						n = 12
					}
				}
			}
		}
		if n > len(data)-i {
			n = 0
		}
		consumed := false
		for n > 0 {
			copy(escape[1:], data[i:i+n])
			escape[n+1] = '"'
			var value string
			if err := json.Unmarshal(escape[:n+2], &value); err == nil {
				decoded = append(decoded, value...)
				i += n
				consumed = true
				break
			}
			if n == 12 {
				n = 6
			} else {
				n = 0
			}
		}
		if !consumed {
			decoded = append(decoded, data[i])
			i++
		}
	}
	return decoded
}

func containsMarker(original, decoded []byte, marker string) bool {
	return bytes.Contains(original, []byte(marker)) || bytes.Contains(decoded, []byte(marker))
}

func checkMarkers(data []byte, markers []string) error {
	decoded := jsonUnescaped(data)
	for i, marker := range markers {
		if containsMarker(data, decoded, marker) {
			return fmt.Errorf("%w: unexpected fixture marker %d", ErrViolation, i+1)
		}
	}
	return nil
}

func inspectResponse(response Response, markers []string) error {
	if err := checkMarkers(response.Body, markers); err != nil {
		return err
	}
	size := len(response.Body)
	for key, values := range response.Headers {
		size += len(key)
		if size > MaxEvidenceBytes {
			return ErrIncomplete
		}
		if err := checkMarkers([]byte(key), markers); err != nil {
			return err
		}
		for _, value := range values {
			size += len(value)
			if size > MaxEvidenceBytes {
				return ErrIncomplete
			}
			if err := checkMarkers([]byte(value), markers); err != nil {
				return err
			}
		}
	}
	return nil
}

func inspectRecords(raw []byte, got, want []Record, forbidden []string) error {
	if len(raw) == 0 || len(raw) > MaxEvidenceBytes {
		return ErrIncomplete
	}
	decoded := jsonUnescaped(raw)
	for i, marker := range forbidden {
		if containsMarker(raw, decoded, marker) {
			return fmt.Errorf("%w: durable bytes contain unexpected fixture marker %d", ErrViolation, i+1)
		}
	}
	if !slices.Equal(got, want) {
		return fmt.Errorf("%w: durable records differ from fixture (got %d, want %d)", ErrViolation, len(got), len(want))
	}
	for i, record := range want {
		if !containsMarker(raw, decoded, record.Row.Line) {
			return fmt.Errorf("%w: durable bytes lack retained-field witness for record %d", ErrIncomplete, i+1)
		}
	}
	return nil
}
