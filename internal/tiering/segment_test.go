package tiering

import (
	"bytes"
	"io"
	"testing"
)

func TestSegmentRoundTripAndCorruption(t *testing.T) {
	batches := [][]byte{[]byte("{\"ts\":\"0001-01-01T00:00:00Z\",\"line\":\"same\"}\n{\"ts\":\"9999-12-31T23:59:59.999999998Z\",\"line\":\"same\"}"), []byte("{\"ts\":\"0001-01-01T00:00:00Z\",\"line\":\"same\"}")}
	end := uint64(8)
	for _, b := range batches {
		end += 16 + uint64(len(b))
	}
	h := SegmentHeader{7, 8, end}
	var out bytes.Buffer
	i := 0
	if err := WriteSegment(&out, h, func() ([]byte, error) {
		if i == len(batches) {
			return nil, io.EOF
		}
		b := batches[i]
		i++
		return b, nil
	}); err != nil {
		t.Fatal(err)
	}
	encoded := out.Bytes()
	var got [][]byte
	actual, err := ReadSegment(bytes.NewReader(encoded), int64(len(encoded)), func(b []byte) error { got = append(got, bytes.Clone(b)); return nil })
	if err != nil || actual != h || len(got) != len(batches) {
		t.Fatalf("header %v batches %d error %v", actual, len(got), err)
	}
	for i := range got {
		if !bytes.Equal(got[i], batches[i]) {
			t.Fatal("batch changed")
		}
	}
	for _, offset := range []int{8, 32, 36, 48, len(encoded) - 1} {
		damaged := bytes.Clone(encoded)
		damaged[offset] ^= 1
		if _, err := ReadSegment(bytes.NewReader(damaged), int64(len(damaged)), func([]byte) error { return nil }); err == nil {
			t.Fatalf("accepted corruption at %d", offset)
		}
	}
	for _, n := range []int{0, 35, len(encoded) - 1} {
		if _, err := ReadSegment(bytes.NewReader(encoded[:n]), int64(n), func([]byte) error { return nil }); err == nil {
			t.Fatalf("accepted truncation %d", n)
		}
	}
}
