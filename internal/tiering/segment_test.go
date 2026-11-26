package tiering

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"testing"
)

func legacy(payloads ...[]byte) ([]byte, []byte) {
	b := []byte("BOGWAL01")
	for _, p := range payloads {
		var h [16]byte
		n := uint32(len(p))
		binary.LittleEndian.PutUint32(h[:], n)
		binary.LittleEndian.PutUint32(h[4:], ^n)
		binary.LittleEndian.PutUint32(h[8:], crc32.Checksum(p, crcTable))
		binary.LittleEndian.PutUint32(h[12:], crc32.Checksum(h[:12], crcTable))
		b = append(b, h[:]...)
		b = append(b, p...)
	}
	marker := make([]byte, 24)
	copy(marker, "BOGCMT01")
	binary.LittleEndian.PutUint64(marker[8:], uint64(len(b)))
	binary.LittleEndian.PutUint64(marker[16:], ^uint64(len(b)))
	return b, marker
}
func TestLegacySegmentRoundTrip(t *testing.T) {
	p := []byte("{\"ts\":\"0001-01-01T00:00:00Z\",\"line\":\"duplicate\"}\n{\"ts\":\"9999-12-31T23:59:59.999999998Z\",\"line\":\"duplicate\"}\n")
	wal, marker := legacy(p, p)
	wal = append(wal, []byte("uncommitted")...)
	var segment bytes.Buffer
	var generation [16]byte
	generation[0] = 42
	coverage, err := ImportLegacy(&segment, bytes.NewReader(wal), int64(len(wal)), marker, generation)
	if err != nil {
		t.Fatal(err)
	}
	var got [][]byte
	read, err := ReadSegment(bytes.NewReader(segment.Bytes()), int64(segment.Len()), func(p []byte) error { got = append(got, bytes.Clone(p)); return nil })
	if err != nil || read != coverage || len(got) != 2 || !bytes.Equal(got[0], p) || !bytes.Equal(got[1], p) {
		t.Fatalf("round trip: %v, %v, %q", read, err, got)
	}
}
func TestRejectCorruptionAndTruncation(t *testing.T) {
	wal, marker := legacy([]byte("{\"ts\":\"2020-01-01T00:00:00Z\",\"line\":\"x\"}\n"))
	var segment bytes.Buffer
	if _, err := ImportLegacy(&segment, bytes.NewReader(wal), int64(len(wal)), marker, [16]byte{}); err != nil {
		t.Fatal(err)
	}
	b := bytes.Clone(segment.Bytes())
	b[headerSize+16] ^= 1
	called := false
	if _, err := ReadSegment(bytes.NewReader(b), int64(len(b)), func([]byte) error { called = true; return nil }); err == nil || called {
		t.Fatal("corruption accepted or exposed")
	}
	if _, err := ReadSegment(bytes.NewReader(b), int64(len(b)-1), nil); err == nil {
		t.Fatal("truncation accepted")
	}
	wal[len(wal)-1] ^= 1
	if _, err := ImportLegacy(&bytes.Buffer{}, bytes.NewReader(wal), int64(len(wal)), marker, [16]byte{}); err == nil {
		t.Fatal("corrupt WAL accepted")
	}
}
