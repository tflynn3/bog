//go:build linux || (darwin && cgo)

package fuzz

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"bog/internal/checkpoint"
	"bog/internal/store"
)

const (
	scanMemoryPageBytes     = 4 << 10
	scanMemoryDiskCap       = 10 << 20
	scanMemoryLock          = "bog.checkpoint.lock"
	scanMemoryManifestLimit = (256 << 10) + 1
	scanMemoryOutputLimit   = 1 << 20
	scanMemoryBlockHeader   = 44
	scanMemoryRowHeader     = 24
)

var scanMemoryStart = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

func scanMemoryAppendRuns(t *testing.T, yield func([]checkpoint.BlockRow)) {
	t.Helper()
	if checkpoint.MaxSegments != scanMemorySegments {
		t.Fatalf("v1 segment ceiling = %d, fixture targets %d", checkpoint.MaxSegments, scanMemorySegments)
	}
	// An unescaped line in a single-record batch without a final newline
	// uses every byte of the largest supported WAL payload.
	empty, err := json.Marshal(store.Row{TS: scanMemoryStart})
	requireRetainedOK(t, err)
	large := strings.Repeat("X", store.MaxBatch-len(empty))
	yield([]checkpoint.BlockRow{{Row: store.Row{TS: scanMemoryStart, Line: large}}})
	const tiedPrefix = "tied-"
	const tiedLineBytes = len(tiedPrefix) + 4
	lateLine := strings.Repeat("x", scanMemoryPageBytes-3*scanMemoryRowHeader-2*tiedLineBytes)
	for i := 1; i < scanMemorySegments; i++ {
		tied := store.Row{TS: scanMemoryStart, Line: fmt.Sprintf("%s%04d", tiedPrefix, i)}
		ordinal := 1 + 3*uint64(i-1)
		// The run contains a late append and two identical backfills.
		yield([]checkpoint.BlockRow{
			{Row: store.Row{TS: scanMemoryStart.Add(2 * time.Second), Line: lateLine}, Ordinal: ordinal},
			{Row: tied, Ordinal: ordinal + 1},
			{Row: tied, Ordinal: ordinal + 2},
		})
	}
}

func scanMemoryRows(t *testing.T) []checkpoint.BlockRow {
	t.Helper()
	var rows []checkpoint.BlockRow
	scanMemoryAppendRuns(t, func(run []checkpoint.BlockRow) { rows = append(rows, run...) })
	slices.SortStableFunc(rows, func(a, b checkpoint.BlockRow) int { return a.Row.TS.Compare(b.Row.TS) })
	return rows
}

func scanMemoryNames() []string {
	names := make([]string, 0, scanMemorySegments+2)
	for i := range scanMemorySegments {
		names = append(names, fmt.Sprintf("bog-segment-%032x", i))
	}
	return append(names, manifestFile, scanMemoryLock)
}

func seedScanMemory(t *testing.T, path string) scanMemoryFixture {
	t.Helper()
	names := scanMemoryNames()
	hash := sha256.New()
	_, err := hash.Write([]byte(walMagic))
	requireRetainedOK(t, err)
	offset := int64(len(walMagic))
	m := checkpoint.Manifest{Version: 1, Segments: make([]checkpoint.Segment, 0, scanMemorySegments)}
	fixture := scanMemoryFixture{Files: len(names) - 1, Segments: scanMemorySegments, PageBytes: scanMemoryPageBytes}
	scanMemoryAppendRuns(t, func(run []checkpoint.BlockRow) {
		var payload bytes.Buffer
		encoder := json.NewEncoder(&payload)
		for _, row := range run {
			requireRetainedOK(t, encoder.Encode(row.Row))
		}
		framed := replayFrame(payload.Bytes())
		blockSize := scanMemoryBlockHeader + scanMemoryPageBytes
		if len(m.Segments) == 0 {
			framed = maximumScanMemoryFrame(t, framed)
			fixture.MaxLineBytes = len(run[0].Row.Line)
			blockSize = scanMemoryBlockHeader + scanMemoryRowHeader + fixture.MaxLineBytes
		}
		_, err := hash.Write(framed)
		requireRetainedOK(t, err)
		offset += int64(len(framed))
		fixture.Rows += len(run)

		ordered := slices.Clone(run)
		slices.SortStableFunc(ordered, func(a, b checkpoint.BlockRow) int { return a.Row.TS.Compare(b.Row.TS) })
		block, err := checkpoint.EncodeBlock(ordered)
		requireRetainedOK(t, err)
		if len(block) != blockSize || fixture.Bytes+int64(len(block)) > scanMemoryDiskCap-scanMemoryOutputLimit {
			t.Fatalf("segment %d: got %d bytes, want %d within fixture cap", len(m.Segments), len(block), blockSize)
		}
		name := names[len(m.Segments)]
		requireRetainedOK(t, os.WriteFile(filepath.Join(path, name), block, 0600))
		fixture.Bytes += int64(len(block))
		m.Segments = append(m.Segments, checkpoint.Segment{Name: name, SHA256: retainedDigest(block), Size: int64(len(block))})
	})
	m.CoveredOffset, m.NextOrdinal, m.SourceSHA256 = offset, uint64(fixture.Rows), fmt.Sprintf("%x", hash.Sum(nil))
	envelope, generation, err := retainedEnvelope(m)
	requireRetainedOK(t, err)
	fixture.Bytes += int64(len(envelope))
	if len(envelope) >= scanMemoryManifestLimit || fixture.Bytes > scanMemoryDiskCap-scanMemoryOutputLimit {
		t.Fatalf("fixture is %d bytes with a %d-byte manifest", fixture.Bytes, len(envelope))
	}
	requireRetainedOK(t, os.WriteFile(filepath.Join(path, manifestFile), envelope, 0600))
	fixture.Generation = fmt.Sprintf("%x", generation)
	fixture.CoveredWALBytes = offset
	return fixture
}

func maximumScanMemoryFrame(t *testing.T, framed []byte) []byte {
	t.Helper()
	if len(framed) != frameHeader+store.MaxBatch+1 || !bytes.HasSuffix(framed, []byte{'\n'}) {
		t.Fatal("maximum line does not fill the expected single-row WAL payload")
	}
	framed = framed[:len(framed)-1]
	le.PutUint32(framed[:4], store.MaxBatch)
	le.PutUint32(framed[4:8], ^uint32(store.MaxBatch))
	le.PutUint32(framed[8:12], crc32.Checksum(framed[frameHeader:], crcTable))
	fixFrameHeader(framed)
	return framed
}

func scanMemoryFixtureBytes(t *testing.T, path string) int64 {
	t.Helper()
	names := scanMemoryNames()
	var total int64
	for _, name := range names[:len(names)-1] {
		info, err := os.Stat(filepath.Join(path, name))
		requireRetainedOK(t, err)
		if !info.Mode().IsRegular() || info.Size() > checkpoint.MaxBlockSize {
			t.Fatalf("invalid fixture file %s: %v", name, info)
		}
		total += info.Size()
	}
	if total > scanMemoryDiskCap-scanMemoryOutputLimit {
		t.Fatalf("fixture bytes %d exceed cap", total)
	}
	return total
}

func TestScanMemoryOverflowFixture(t *testing.T) {
	rows := scanMemoryRows(t)
	first := rows[0]
	if first.Ordinal != 0 || len(first.Row.Line) <= scanMemoryPageBytes {
		t.Fatalf("first row ordinal=%d line_bytes=%d does not force overflow", first.Ordinal, len(first.Row.Line))
	}
	encoded, err := json.Marshal(first.Row)
	requireRetainedOK(t, err)
	framed := maximumScanMemoryFrame(t, replayFrame(append(encoded, '\n')))
	payload := framed[frameHeader:]
	if len(payload) != store.MaxBatch {
		t.Fatalf("maximum WAL payload is %d bytes, want %d", len(payload), store.MaxBatch)
	}
	decoded, err := store.Decode(payload)
	requireRetainedOK(t, err)
	if len(decoded) != 1 || decoded[0] != first.Row {
		t.Fatal("maximum WAL payload failed exact row round trip")
	}
	if _, err := store.Decode(append(payload, ' ')); err == nil {
		t.Fatal("accepted a payload larger than the WAL batch limit")
	}
	firstLate := rows[1+2*(scanMemorySegments-1)]
	if rows[1].Row != rows[2].Row || rows[1].Ordinal+1 != rows[2].Ordinal ||
		rows[1].Ordinal <= firstLate.Ordinal || !rows[1].Row.TS.Before(firstLate.Row.TS) {
		t.Fatal("fixture does not exercise duplicate, tied and backfilled rows")
	}
}
