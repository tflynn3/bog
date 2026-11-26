package tiering

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"testing"

	"bog/internal/store"
)

func legacyFixture(batches ...string) ([]byte, []byte) {
	wal := []byte("BOGWAL01")
	for _, batch := range batches {
		var h [16]byte
		n := uint32(len(batch))
		binary.LittleEndian.PutUint32(h[:], n)
		binary.LittleEndian.PutUint32(h[4:], ^n)
		binary.LittleEndian.PutUint32(h[8:], crc32.Checksum([]byte(batch), castagnoli))
		binary.LittleEndian.PutUint32(h[12:], crc32.Checksum(h[:12], castagnoli))
		wal = append(wal, h[:]...)
		wal = append(wal, batch...)
	}
	marker := make([]byte, 24)
	copy(marker, "BOGCMT01")
	binary.LittleEndian.PutUint64(marker[8:], uint64(len(wal)))
	binary.LittleEndian.PutUint64(marker[16:], ^uint64(len(wal)))
	return wal, marker
}

func TestReadLegacyCommittedPrefix(t *testing.T) {
	batch := `{"ts":"0001-01-01T00:00:00Z","line":"same"}` + "\n" + `{"ts":"9999-12-31T23:59:59.999999998Z","line":"same"}`
	wal, marker := legacyFixture(batch, batch)
	committed := len(wal)
	wal = append(wal, []byte("uncommitted junk")...)
	var offsets []int64
	var rows []store.Row
	end, err := ReadLegacy(bytes.NewReader(wal), int64(len(wal)), marker, func(offset int64, _ []byte, batch []store.Row) error {
		offsets = append(offsets, offset)
		rows = append(rows, batch...)
		return nil
	})
	if err != nil || end != int64(committed) || len(rows) != 4 || rows[0] != rows[2] || rows[1] != rows[3] || len(offsets) != 2 || offsets[1] != end {
		t.Fatalf("end=%d offsets=%v rows=%v err=%v", end, offsets, rows, err)
	}
}

func TestReadLegacyRejectsCorruption(t *testing.T) {
	wal, marker := legacyFixture(`{"ts":"2024-01-01T00:00:00Z","line":"x"}`)
	cases := []struct {
		name        string
		wal, marker []byte
	}{
		{"marker", wal, append([]byte(nil), marker[:23]...)},
		{"short frame", wal[:len(wal)-1], marker},
		{"header checksum", append([]byte(nil), wal...), marker},
		{"payload checksum", append([]byte(nil), wal...), marker},
	}
	cases[2].wal[8] ^= 1
	cases[3].wal[len(wal)-1] ^= 1
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := ReadLegacy(bytes.NewReader(tc.wal), int64(len(tc.wal)), tc.marker, func(int64, []byte, []store.Row) error { t.Fatal("visited corrupt frame"); return nil }); err == nil {
				t.Fatal("expected error")
			}
		})
	}
}

func TestReadLegacyCallbackError(t *testing.T) {
	wal, marker := legacyFixture(`{"ts":"2024-01-01T00:00:00Z","line":"x"}`)
	sentinel := errors.New("stop")
	_, err := ReadLegacy(bytes.NewReader(wal), int64(len(wal)), marker, func(int64, []byte, []store.Row) error { return sentinel })
	if !errors.Is(err, sentinel) {
		t.Fatalf("got %v", err)
	}
}
