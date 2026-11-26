package tiering

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"hash/crc32"
	"testing"
)

func TestImportLegacyCommittedBoundary(t *testing.T) {
	payload := []byte("{\"ts\":\"0001-01-01T00:00:00Z\",\"line\":\"a\"}\n{\"ts\":\"0001-01-01T00:00:00Z\",\"line\":\"a\"}\n")
	var header [16]byte
	binary.LittleEndian.PutUint32(header[:4], uint32(len(payload)))
	binary.LittleEndian.PutUint32(header[4:8], ^uint32(len(payload)))
	binary.LittleEndian.PutUint32(header[8:12], crc32.Checksum(payload, castagnoli))
	binary.LittleEndian.PutUint32(header[12:], crc32.Checksum(header[:12], castagnoli))
	prefix := append(append([]byte("BOGWAL01"), header[:]...), payload...)
	input := append(bytes.Clone(prefix), []byte("uncommitted")...)
	var marker [24]byte
	copy(marker[:8], "BOGCMT01")
	binary.LittleEndian.PutUint64(marker[8:16], uint64(len(prefix)))
	binary.LittleEndian.PutUint64(marker[16:], ^uint64(len(prefix)))
	var output bytes.Buffer
	end, digest, err := ImportLegacy(bytes.NewReader(input), int64(len(input)), marker[:], &output, 17)
	if err != nil || end != int64(len(prefix)) || digest != sha256.Sum256(prefix) {
		t.Fatalf("import: %d %x %v", end, digest, err)
	}
	var frames []Frame
	if err := ReadSegment(bytes.NewReader(output.Bytes()), int64(output.Len()), func(f Frame) error { frames = append(frames, f); return nil }); err != nil {
		t.Fatal(err)
	}
	if len(frames) != 1 || frames[0].Sequence != 17 || !bytes.Equal(frames[0].Payload, payload) {
		t.Fatal(frames)
	}
	binary.LittleEndian.PutUint64(marker[8:16], uint64(len(prefix)-1))
	binary.LittleEndian.PutUint64(marker[16:], ^uint64(len(prefix)-1))
	if _, _, err := ImportLegacy(bytes.NewReader(input), int64(len(input)), marker[:], &bytes.Buffer{}, 1); err == nil {
		t.Fatal("accepted partial frame")
	}
	input[24] ^= 1
	binary.LittleEndian.PutUint64(marker[8:16], uint64(len(prefix)))
	binary.LittleEndian.PutUint64(marker[16:], ^uint64(len(prefix)))
	if _, _, err := ImportLegacy(bytes.NewReader(input), int64(len(input)), marker[:], &bytes.Buffer{}, 1); err == nil {
		t.Fatal("accepted corrupt committed frame")
	}
}
