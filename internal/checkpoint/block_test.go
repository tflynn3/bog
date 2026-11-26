package checkpoint

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"hash/crc32"
	"strings"
	"testing"
	"time"

	"bog/internal/store"
)

func TestBlockV1Fixture(t *testing.T) {
	// Generated independently with little-endian struct packing and a bitwise
	// CRC32C implementation (polynomial 0x82f63b78), not EncodeBlock.
	const fixture = "010000000300000050000000ffffffffffffffffffc99a3b" +
		"01000000000000007b000000d60931c2cca3bf52" +
		"01000000000000007b0000000900000000000000020000006869" +
		"ffffffffffffffffffc99a3b0300000000000000040000006261636b" +
		"01000000000000007b0000000a00000000000000020000006869"
	want, err := hex.DecodeString(fixture)
	if err != nil {
		t.Fatal(err)
	}
	rows := []BlockRow{
		{store.Row{TS: time.Unix(1, 123).UTC(), Line: "hi"}, 9},
		{store.Row{TS: time.Unix(-1, 999999999).UTC(), Line: "back"}, 3},
		{store.Row{TS: time.Unix(1, 123).UTC(), Line: "hi"}, 10},
	}
	encoded, err := EncodeBlock(rows)
	if err != nil || !bytes.Equal(encoded, want) {
		t.Fatalf("encoded fixture: %x, error: %v", encoded, err)
	}
	decoded, err := DecodeBlock(want)
	if err != nil || len(decoded) != len(rows) {
		t.Fatalf("decoded fixture: %+v, error: %v", decoded, err)
	}
	for i := range rows {
		if !decoded[i].Row.TS.Equal(rows[i].Row.TS) || decoded[i].Row.Line != rows[i].Row.Line || decoded[i].Ordinal != rows[i].Ordinal {
			t.Fatalf("fixture row %d: got %+v, want %+v", i, decoded[i], rows[i])
		}
	}
}

func TestBlockRoundTrip(t *testing.T) {
	ts := time.Date(2024, 1, 1, 0, 0, 0, 1, time.UTC)
	rows := []BlockRow{
		{store.Row{TS: ts, Line: "same"}, 4},
		{store.Row{TS: ts, Line: "same"}, 5},
		{store.Row{TS: minBlockTime, Line: "backfill"}, 7},
		{store.Row{TS: ts.Add(-time.Nanosecond), Line: "tie"}, 2},
		{store.Row{TS: maxBlockTime.Add(-time.Nanosecond), Line: "last"}, 8},
	}
	b, err := EncodeBlock(rows)
	if err != nil {
		t.Fatal(err)
	}
	got, err := DecodeBlock(b)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != len(rows) {
		t.Fatal("row count")
	}
	for i := range rows {
		if !got[i].Row.TS.Equal(rows[i].Row.TS) || got[i].Row.Line != rows[i].Row.Line || got[i].Ordinal != rows[i].Ordinal {
			t.Fatalf("row %d: %+v", i, got[i])
		}
	}
}

func TestBlockLargestLegacyRow(t *testing.T) {
	// An actual valid legacy batch at the maximum permitted frame size.
	prefix := `{"ts":"2024-01-01T00:00:00Z","line":"`
	suffix := `"}`
	frame := []byte(prefix + strings.Repeat("x", store.MaxBatch-len(prefix)-len(suffix)) + suffix)
	rows, err := store.Decode(frame)
	if err != nil {
		t.Fatal(err)
	}
	b, err := EncodeBlock([]BlockRow{{Row: rows[0]}})
	if err != nil {
		t.Fatal(err)
	}
	got, err := DecodeBlock(b)
	if err != nil || len(got) != 1 || got[0].Row.Line != rows[0].Line {
		t.Fatalf("round trip: %v", err)
	}
}

func TestBlockRejectsCorruption(t *testing.T) {
	b, err := EncodeBlock([]BlockRow{{Row: store.Row{TS: minBlockTime, Line: "abc"}, Ordinal: 1}})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < len(b); i++ {
		if rows, err := DecodeBlock(b[:i]); err == nil || rows != nil {
			t.Fatalf("truncation %d accepted", i)
		}
	}
	cases := []struct {
		name    string
		mutate  func([]byte)
		payload bool
	}{
		{"version", func(b []byte) { binary.LittleEndian.PutUint32(b, 2) }, false},
		{"count", func(b []byte) { binary.LittleEndian.PutUint32(b[4:], 2) }, false},
		{"length", func(b []byte) { binary.LittleEndian.PutUint32(b[8:], 1) }, false},
		{"bounds", func(b []byte) { binary.LittleEndian.PutUint64(b[12:], uint64(maxBlockTime.Unix())) }, false},
		{"nanoseconds", func(b []byte) { binary.LittleEndian.PutUint32(b[blockHeaderSize+8:], 1e9) }, true},
		{"row length", func(b []byte) { binary.LittleEndian.PutUint32(b[blockHeaderSize+20:], ^uint32(0)) }, true},
		{"row utf8", func(b []byte) { b[len(b)-1] = 0xff }, true},
		{"extra bytes", func(b []byte) { binary.LittleEndian.PutUint32(b[blockHeaderSize+20:], 2) }, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := bytes.Clone(b)
			tc.mutate(c)
			if tc.payload {
				binary.LittleEndian.PutUint32(c[36:], crc32.Checksum(c[blockHeaderSize:], castagnoli))
			}
			binary.LittleEndian.PutUint32(c[40:], crc32.Checksum(c[:40], castagnoli))
			if rows, err := DecodeBlock(c); err == nil || rows != nil {
				t.Fatal("accepted corruption")
			}
		})
	}
	for _, pos := range []int{0, 36, 40, len(b) - 1} {
		c := bytes.Clone(b)
		c[pos] ^= 1
		if rows, err := DecodeBlock(c); err == nil || rows != nil {
			t.Fatalf("accepted checksum corruption at %d", pos)
		}
	}
	if rows, err := DecodeBlock(append(bytes.Clone(b), 0)); err == nil || rows != nil {
		t.Fatal("accepted trailing byte")
	}
}

func TestBlockEncodeRejectsInvalid(t *testing.T) {
	for _, rows := range [][]BlockRow{nil, {{Row: store.Row{TS: maxBlockTime}}}, {{Row: store.Row{TS: minBlockTime.Add(-time.Nanosecond)}}}, {{Row: store.Row{TS: minBlockTime, Line: string([]byte{0xff})}}}, {{Row: store.Row{TS: minBlockTime, Line: strings.Repeat("x", MaxBlockSize)}}}} {
		if _, err := EncodeBlock(rows); err == nil {
			t.Fatal("accepted invalid input")
		}
	}
}
