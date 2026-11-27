package fuzz

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"maps"
	"path/filepath"
	"slices"
	"time"
	"unicode/utf8"

	"bog/internal/store"
	querymodel "bog/verify/query-model"
)

const (
	replayMaxBatches    = 8
	replayMaxRows       = 64
	replayMaxInputBytes = 16 << 10
	replayMaxLineBytes  = 512
	replayMaxFiles      = 8
	replayMaxFileBytes  = 32 << 10
	replayMaxImageBytes = 64 << 10
	replayDeadline      = 5 * time.Second
	replayPageSize      = 2

	metadataFile = "bog.backup"
	manifestFile = "bog.checkpoint"
	tailFile     = "bog.wal.tail"
	walMagic     = "BOGWAL01"
	frameHeader  = 16
)

var (
	le                 = binary.LittleEndian
	crcTable           = crc32.MakeTable(crc32.Castagnoli)
	errReplayLimit     = errors.New("replay oracle limit exceeded")
	errReplayUncertain = errors.New("replay oracle cannot record an uncertain batch")
	replayFrom         = time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC)
	replayUntil        = time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC)
)

type files map[string][]byte

type replayOutcome uint8

const (
	replayCommitted replayOutcome = iota + 1
	replayRolledBack
	replayUncertain
)

type replayBatch struct {
	rows    []store.Row
	payload []byte
}

func prepareReplayBatch(rows []store.Row) (replayBatch, error) {
	if len(rows) == 0 || len(rows) > replayMaxRows {
		return replayBatch{}, fmt.Errorf("%w: batch has %d rows", errReplayLimit, len(rows))
	}
	var payload bytes.Buffer
	enc := json.NewEncoder(&payload)
	for _, row := range rows {
		if len(row.Line) > replayMaxLineBytes {
			return replayBatch{}, fmt.Errorf("%w: input line has %d bytes", errReplayLimit, len(row.Line))
		}
		if !utf8.ValidString(row.Line) || row.TS.Before(replayFrom) || !row.TS.Before(replayUntil) {
			return replayBatch{}, errors.New("invalid replay fixture row")
		}
		if err := enc.Encode(row); err != nil {
			return replayBatch{}, fmt.Errorf("encode fixture row: %w", err)
		}
		if payload.Len() > replayMaxInputBytes-frameHeader-len(walMagic) {
			return replayBatch{}, fmt.Errorf("%w: encoded batch", errReplayLimit)
		}
	}
	return replayBatch{slices.Clone(rows), payload.Bytes()}, nil
}

func replayFrame(payload []byte) []byte {
	header := make([]byte, frameHeader, frameHeader+len(payload))
	n := uint32(len(payload))
	le.PutUint32(header[0:4], n)
	le.PutUint32(header[4:8], ^n)
	le.PutUint32(header[8:12], crc32.Checksum(payload, crcTable))
	le.PutUint32(header[12:16], crc32.Checksum(header[:12], crcTable))
	return append(header, payload...)
}

type replayCut struct {
	end  int
	rows int
}

// committedBatchLedger is populated by the fixture driver. A cut addresses a
// complete acknowledged batch, with cut zero denoting the empty WAL. The driver
// serializes recording and hands observers detached snapshots.
type committedBatchLedger struct {
	log      querymodel.Log
	wal      []byte
	cuts     []replayCut
	attempts int
	rows     int
	bytes    int
	fenced   bool
}

func (l *committedBatchLedger) checkBatch(batch replayBatch) error {
	size := frameHeader + len(batch.payload)
	if l.fenced {
		return errReplayUncertain
	}
	if len(batch.rows) == 0 || len(batch.payload) == 0 {
		return errors.New("unprepared replay batch")
	}
	if l.attempts >= replayMaxBatches || size > replayMaxInputBytes-l.bytes-len(walMagic) ||
		len(batch.rows) > replayMaxRows-l.rows {
		return fmt.Errorf("%w: cumulative fixture input", errReplayLimit)
	}
	return nil
}

func (l *committedBatchLedger) record(batch replayBatch, outcome replayOutcome) error {
	if err := l.checkBatch(batch); err != nil {
		return err
	}
	if outcome == replayUncertain {
		l.fenced = true
		return errReplayUncertain
	}
	if outcome != replayCommitted && outcome != replayRolledBack {
		return fmt.Errorf("unknown replay batch outcome %d", outcome)
	}
	l.attempts++
	l.rows += len(batch.rows)
	l.bytes += frameHeader + len(batch.payload)
	if outcome == replayRolledBack {
		return nil
	}
	if len(l.wal) == 0 {
		l.wal = []byte(walMagic)
	}
	l.wal = append(l.wal, replayFrame(batch.payload)...)
	rows := make([]querymodel.Row, len(batch.rows))
	for i, row := range batch.rows {
		rows[i] = querymodel.Row{Time: row.TS, Line: row.Line}
	}
	l.cuts = append(l.cuts, replayCut{len(l.wal), l.log.Append(rows)})
	return nil
}

func (l *committedBatchLedger) cut(batch int) (replayCut, error) {
	if l.fenced {
		return replayCut{}, errReplayUncertain
	}
	if batch < 0 || batch > len(l.cuts) {
		return replayCut{}, fmt.Errorf("committed batch cut %d outside [0,%d]", batch, len(l.cuts))
	}
	if batch == 0 {
		return replayCut{end: len(walMagic)}, nil
	}
	return l.cuts[batch-1], nil
}

type replayView struct {
	covered replayCut
	wal     []byte
	rows    []querymodel.Entry
}

func (l *committedBatchLedger) view(coveredBatch, committedBatch int) (replayView, error) {
	covered, err := l.cut(coveredBatch)
	if err != nil {
		return replayView{}, err
	}
	committed, err := l.cut(committedBatch)
	if err != nil {
		return replayView{}, err
	}
	if covered.end > committed.end {
		return replayView{}, errors.New("checkpoint cut exceeds committed cut")
	}
	rows, err := l.log.Match(committed.rows, querymodel.Filter{Start: replayFrom, End: replayUntil})
	if err != nil {
		return replayView{}, err
	}
	wal := []byte(walMagic)
	if committedBatch != 0 {
		wal = slices.Clone(l.wal[:committed.end])
	}
	return replayView{covered, wal, rows}, nil
}

func (v replayView) prefix() []byte { return v.wal[:v.covered.end] }
func (v replayView) tail() []byte   { return v.wal[v.covered.end:] }

// replayHistory always includes a tie across batches, a duplicate split by a
// distinct tie, a backfill and lines whose JSON and decoded bytes differ.
func replayHistory(seed uint8) [][]store.Row {
	tie := time.Date(2026, 1, 1, 0, 0, 2, int(seed), time.UTC)
	dup := store.Row{TS: tie, Line: fmt.Sprintf("duplicate %02x\x00雪", seed)}
	return [][]store.Row{
		{dup},
		{
			{TS: tie, Line: "tie \"\\\"\n"},
			dup,
			{TS: tie.Add(-time.Second), Line: "backfill\r\n"},
		},
		{
			{TS: tie, Line: "after capture"},
			dup,
			{TS: tie.Add(-2 * time.Second), Line: "earliest"},
		},
	}
}

type replayBoundary struct {
	generation [sha256.Size]byte
	prefixHash [sha256.Size]byte
	covered    int64
	end        int64
	next       uint64
}

func (v replayView) boundary(generation [sha256.Size]byte) replayBoundary {
	return replayBoundary{
		generation: generation, covered: int64(v.covered.end),
		prefixHash: sha256.Sum256(v.prefix()), end: int64(len(v.wal)), next: uint64(v.covered.rows),
	}
}

type replayEvidence struct {
	boundary replayBoundary
	files    files
}

// artifactObservation admits an artifact through its public reader, emits
// rows in timestamp/ordinal order, and reports its boundary and exact bytes.
type artifactObservation func(context.Context, func(querymodel.Entry) error) (replayEvidence, error)

type replayExpectation struct {
	rows []querymodel.Entry
	replayEvidence
	reject error
}

func (v replayView) expect(generation [sha256.Size]byte, image files) replayExpectation {
	return replayExpectation{rows: v.rows, replayEvidence: replayEvidence{v.boundary(generation), image}}
}

func validateReplayFiles(image files) error {
	if len(image) > replayMaxFiles {
		return fmt.Errorf("%w: %d artifact files", errReplayLimit, len(image))
	}
	var total int
	for name, data := range image {
		if !filepath.IsLocal(name) || filepath.Base(name) != name || name == "." {
			return fmt.Errorf("invalid artifact name %q", name)
		}
		if len(data) > replayMaxFileBytes || len(data) > replayMaxImageBytes-total {
			return fmt.Errorf("%w: artifact bytes at %s", errReplayLimit, name)
		}
		total += len(data)
	}
	return nil
}

func compareReplayFiles(got, want files) error {
	if len(got) != len(want) {
		return fmt.Errorf("artifact file count %d, want %d", len(got), len(want))
	}
	for _, name := range slices.Sorted(maps.Keys(want)) {
		if b, ok := got[name]; !ok || !bytes.Equal(b, want[name]) {
			return fmt.Errorf("artifact bytes differ for %s", name)
		}
	}
	return nil
}

func checkReplay(ctx context.Context, observe artifactObservation, want replayExpectation) error {
	if len(want.rows) > replayMaxRows {
		return fmt.Errorf("%w: expected row count", errReplayLimit)
	}
	if err := validateReplayFiles(want.files); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, replayDeadline)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return err
	}
	count, lineBytes := 0, 0
	var rowErr error
	got, err := observe(ctx, func(row querymodel.Entry) error {
		if rowErr != nil {
			return rowErr
		}
		count++
		if err := ctx.Err(); err != nil {
			rowErr = err
		} else if len(row.Line) > replayMaxLineBytes || count > replayMaxRows ||
			len(row.Line) > replayMaxInputBytes-lineBytes {
			rowErr = fmt.Errorf("%w: observed row %d", errReplayLimit, count)
		} else if count > len(want.rows) {
			rowErr = fmt.Errorf("unexpected replay row at index %d", count-1)
		} else if expected := want.rows[count-1]; !row.Time.Equal(expected.Time) ||
			row.Ordinal != expected.Ordinal || row.Line != expected.Line {
			rowErr = fmt.Errorf("replay row %d = %+v, want %+v", count-1, row, expected)
		}
		lineBytes += min(len(row.Line), replayMaxLineBytes)
		return rowErr
	})
	if ctxErr := ctx.Err(); ctxErr != nil {
		return errors.Join(ctxErr, err)
	}
	if rowErr != nil {
		return errors.Join(rowErr, err)
	}
	if err := validateReplayFiles(got.files); err != nil {
		return err
	}
	if want.reject != nil {
		if !errors.Is(err, want.reject) || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) ||
			got.boundary != (replayBoundary{}) || got.files != nil {
			return fmt.Errorf("observe artifact: expected clean rejection matching %v, got %w", want.reject, err)
		}
		return nil
	}
	if err != nil {
		return fmt.Errorf("observe artifact: %w", err)
	}
	if count != len(want.rows) || got.boundary != want.boundary {
		return fmt.Errorf("replay boundary %+v and %d rows, want %+v and %d rows", got.boundary, count, want.boundary, len(want.rows))
	}
	return compareReplayFiles(got.files, want.files)
}
