package fuzz

import (
	"bytes"
	"encoding/json"
	"errors"
	"hash/crc32"
	"strings"
	"testing"
	"time"

	"bog/internal/checkpoint"
	"bog/internal/store"
)

func TestScanWorkerReadiness(t *testing.T) {
	want := scanWorkerReady{
		Version: scanStartupVersion, PID: 42, Readers: 8, Arch: "arm64", GoVersion: "go1.27.0", Race: true,
		Memory: scanProcessMemory{VirtualBytes: 4 << 40, RSSBytes: 12 << 20},
	}
	data, err := json.Marshal(want)
	if err != nil {
		t.Fatal(err)
	}
	record := string(data)
	for _, tc := range []struct {
		name, input string
		valid       bool
	}{
		{"ready", record + "\n", true},
		{"unavailable memory", strings.Replace(record, `"virtual_bytes":4398046511104,"rss_bytes":12582912`, `"unavailable":"no proc status"`, 1), true},
		{"empty", "", false},
		{"truncated", record[:len(record)-1], false},
		{"unknown field", record[:len(record)-1] + `,"unexpected":true}`, false},
		{"second record", record + "\n" + record, false},
		{"trailing garbage", record + "\nx", false},
		{"byte limit", record + strings.Repeat(" ", scanStartupBytes-len(record)), true},
		{"oversized", record + strings.Repeat(" ", scanStartupBytes-len(record)+1), false},
		{"wrong pid", strings.Replace(record, `"pid":42`, `"pid":43`, 1), false},
		{"wrong reader count", strings.Replace(record, `"readers":8`, `"readers":1`, 1), false},
		{"wrong architecture", strings.Replace(record, "arm64", "amd64", 1), false},
		{"wrong instrumentation", strings.Replace(record, "true", "false", 1), false},
		{"impossible memory", strings.Replace(record, "4398046511104", "1", 1), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := readScanWorkerReady(strings.NewReader(tc.input), want)
			if (err == nil) != tc.valid {
				t.Fatalf("ready=%+v err=%v, want valid=%t", got, err, tc.valid)
			}
		})
	}
}

func TestScanWorkerOutcome(t *testing.T) {
	failed := errors.New("exit status 1")
	for _, tc := range []struct {
		name, output string
		started      bool
		err          error
		want         scanWorkerOutcome
	}{
		{"ready and passed", "PASS", true, nil, scanWorkerPassed},
		{"missing handshake", "FAIL", false, failed, scanWorkerStartup},
		{"address layout", "FATAL: ThreadSanitizer: unexpected memory mapping 0xc0", false, failed, scanWorkerSanitizer},
		{"shadow growth", "ThreadSanitizer failed to allocate 0x2000 bytes", true, failed, scanWorkerSanitizer},
		{"allocator check", "ThreadSanitizer: CHECK failed: sanitizer_allocator_primary64.h:123", true, failed, scanWorkerSanitizer},
		{"race report", "WARNING: DATA RACE", true, failed, scanWorkerFailed},
		{"budget validation", "", true, errors.Join(failed, errScanReaderBudget), scanWorkerBudget},
		{"worker budget", scanBudgetMarker + "\nFAIL", true, failed, scanWorkerBudget},
		{"marker anywhere else", "garbled " + scanBudgetMarker, true, failed, scanWorkerFailed},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := scanOutcome(tc.started, []byte(tc.output), tc.err); got != tc.want {
				t.Fatalf("outcome=%q, want %q", got, tc.want)
			}
		})
	}
}

func TestScanMemoryParserLimits(t *testing.T) {
	// A valid block header and checksum must still reject large declared
	// allocations before exposing rows.
	const blockHeader = 44
	original, err := checkpoint.EncodeBlock([]checkpoint.BlockRow{{Row: store.Row{TS: time.Unix(0, 0).UTC(), Line: "row"}}})
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name   string
		mutate func([]byte)
	}{
		{"row count", func(b []byte) { le.PutUint32(b[4:8], ^uint32(0)) }},
		{"payload length", func(b []byte) { le.PutUint32(b[8:12], ^uint32(0)) }},
		{"line length", func(b []byte) { le.PutUint32(b[blockHeader+20:blockHeader+24], ^uint32(0)) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			data := bytes.Clone(original)
			tc.mutate(data)
			le.PutUint32(data[36:40], crc32.Checksum(data[blockHeader:], crcTable))
			le.PutUint32(data[40:44], crc32.Checksum(data[:40], crcTable))
			rows, err := checkpoint.DecodeBlock(data)
			if len(rows) != 0 || err == nil {
				t.Fatalf("malformed block exposed %d rows with error %v", len(rows), err)
			}
		})
	}
	oversized := make([]byte, checkpoint.MaxBlockSize+1)
	copy(oversized, original)
	if rows, err := checkpoint.DecodeBlock(oversized); len(rows) != 0 || err == nil {
		t.Fatalf("oversized block exposed %d rows with error %v", len(rows), err)
	}
	if rows, err := checkpoint.DecodeBlock(original); len(rows) != 1 || err != nil {
		t.Fatalf("retry exposed %d rows with error %v", len(rows), err)
	}
}
