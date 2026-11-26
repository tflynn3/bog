package tiering

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"testing"
)

func frame(p []byte) []byte {
	h := make([]byte, 16)
	binary.LittleEndian.PutUint32(h, uint32(len(p)))
	binary.LittleEndian.PutUint32(h[4:], ^uint32(len(p)))
	binary.LittleEndian.PutUint32(h[8:], crc32.Checksum(p, crcTable))
	binary.LittleEndian.PutUint32(h[12:], crc32.Checksum(h[:12], crcTable))
	return append(h, p...)
}
func TestCommittedConversion(t *testing.T) {
	p := []byte("{\"ts\":\"0001-01-01T00:00:00Z\",\"line\":\"same\"}\n{\"ts\":\"9999-12-31T23:59:59.999999998Z\",\"line\":\"same\"}\n")
	w := append([]byte(walMagic), frame(p)...)
	end := len(w)
	w = append(w, frame(p)...)
	var seg bytes.Buffer
	if err := Convert(bytes.NewReader(w), int64(len(w)), int64(end), &seg); err != nil {
		t.Fatal(err)
	}
	var got [][]byte
	if err := Read(bytes.NewReader(seg.Bytes()), int64(seg.Len()), func(p []byte) error { got = append(got, bytes.Clone(p)); return nil }); err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || !bytes.Equal(got[0], p) {
		t.Fatalf("unexpected frames: %q", got)
	}
	bad := bytes.Clone(seg.Bytes())
	bad[len(bad)-1] ^= 1
	if err := Read(bytes.NewReader(bad), int64(len(bad)), func([]byte) error { return nil }); err == nil {
		t.Fatal("accepted corruption")
	}
	for i := 0; i < len(seg.Bytes()); i++ {
		if err := Read(bytes.NewReader(seg.Bytes()[:i]), int64(i), func([]byte) error { return nil }); err == nil {
			t.Fatalf("accepted truncation at %d", i)
		}
	}
}
func TestBoundary(t *testing.T) {
	b := make([]byte, 24)
	copy(b, "BOGCMT01")
	binary.LittleEndian.PutUint64(b[8:], 8)
	binary.LittleEndian.PutUint64(b[16:], ^uint64(8))
	if n, err := Boundary(bytes.NewReader(b), 8); err != nil || n != 8 {
		t.Fatalf("%d %v", n, err)
	}
	b = append(b, 0)
	if _, err := Boundary(bytes.NewReader(b), 8); err == nil {
		t.Fatal("accepted trailing marker byte")
	}
}
