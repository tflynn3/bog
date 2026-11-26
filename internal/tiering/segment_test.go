package tiering

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"testing"
)

func TestSegmentRoundTrip(t *testing.T) {
	var data bytes.Buffer
	w, err := NewSegmentWriter(&data, 42)
	if err != nil {
		t.Fatal(err)
	}
	batches := [][]byte{
		[]byte("{\"ts\":\"9999-12-31T23:59:59.999999998Z\",\"line\":\"duplicate\"}\n{\"ts\":\"0001-01-01T00:00:00Z\",\"line\":\"duplicate\"}\n"),
		[]byte("{\"ts\":\"1970-01-01T00:00:00Z\",\"line\":\"literal.*\"}\n"),
	}
	for _, b := range batches {
		if err = w.Add(b); err != nil {
			t.Fatal(err)
		}
	}
	if err = w.Close(); err != nil {
		t.Fatal(err)
	}
	var got []Frame
	err = ReadSegment(bytes.NewReader(data.Bytes()), int64(data.Len()), func(f Frame) error { got = append(got, f); return nil })
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0].Sequence != 42 || got[1].Sequence != 43 || !bytes.Equal(got[0].Payload, batches[0]) || got[0].Min.Year() != 1 || got[0].Max.Year() != 9999 {
		t.Fatalf("bad frames: %+v", got)
	}
}

func TestSegmentRejectsCorruption(t *testing.T) {
	var data bytes.Buffer
	w, _ := NewSegmentWriter(&data, 1)
	if err := w.Add([]byte("{\"ts\":\"2020-01-01T00:00:00Z\",\"line\":\"x\"}\n")); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	original := data.Bytes()
	for _, offset := range []int{0, headerSize, headerSize + blockHeaderSize, len(original) - 1} {
		b := bytes.Clone(original)
		b[offset] ^= 1
		if err := ReadSegment(bytes.NewReader(b), int64(len(b)), nil); !errors.Is(err, ErrCorrupt) {
			t.Fatalf("offset %d: %v", offset, err)
		}
	}
	// Even a header with a valid checksum must not cause an oversized allocation.
	b := bytes.Clone(original)
	binary.LittleEndian.PutUint32(b[headerSize+4:], ^uint32(0))
	binary.LittleEndian.PutUint32(b[headerSize+52:], crc32.Checksum(b[headerSize:headerSize+52], castagnoli))
	if err := ReadSegment(bytes.NewReader(b), int64(len(b)), nil); !errors.Is(err, ErrCorrupt) {
		t.Fatal(err)
	}
	for i := 0; i < len(original); i++ {
		if err := ReadSegment(bytes.NewReader(original[:i]), int64(i), nil); err == nil {
			t.Fatalf("accepted truncation %d", i)
		}
	}
}
