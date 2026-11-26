package main

import (
	"io"
	"slices"
	"testing"
)

func TestParseConfig(t *testing.T) {
	for _, tc := range []struct {
		name    string
		args    []string
		scale   string
		batches int
		runs    int
		wantErr bool
	}{
		{name: "default", scale: "medium", batches: 2048, runs: 21},
		{name: "small", args: []string{"-scale", "small"}, scale: "small", batches: 64, runs: 21},
		{name: "large", args: []string{"-scale", "large", "-fixture", "entropy", "-runs", "2"}, scale: "large", batches: 8192, runs: 2},
		{name: "custom", args: []string{"-batches", "17"}, scale: "custom", batches: 17, runs: 21},
		{name: "smoke", args: []string{"-smoke", "-runs", "5"}, scale: "smoke", batches: 64, runs: 1},
		{name: "unknown scale", args: []string{"-scale", "gigantic"}, wantErr: true},
		{name: "conflict", args: []string{"-batches", "2", "-scale", "small"}, wantErr: true},
		{name: "zero batches", args: []string{"-batches", "0"}, wantErr: true},
		{name: "too many batches", args: []string{"-batches", "10001"}, wantErr: true},
		{name: "bad runs", args: []string{"-runs", "0"}, wantErr: true},
		{name: "bad profile", args: []string{"-fixture", "random"}, wantErr: true},
		{name: "positional", args: []string{"small"}, wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg, err := parseConfig(tc.args, "test", io.Discard)
			if (err != nil) != tc.wantErr {
				t.Fatalf("parseConfig error = %v", err)
			}
			if err == nil && (cfg.scale != tc.scale || cfg.batches != tc.batches || cfg.runs != tc.runs || cfg.revision != "test") {
				t.Errorf("unexpected config %+v", cfg)
			}
		})
	}
}

func TestP95NearestRank(t *testing.T) {
	samples := make([]int64, 20)
	for i := range samples {
		samples[i] = int64(20 - i)
	}
	original := slices.Clone(samples)
	if got := p95(samples); got != 19 || !slices.Equal(samples, original) {
		t.Errorf("p95 = %d, input after calculation = %v", got, samples)
	}
	if got := p95(samples[:1]); got != 20 {
		t.Errorf("single-sample p95 = %d", got)
	}
}

func TestComparePayloadReads(t *testing.T) {
	base := measurement{Name: "sparse", Literal: "literal", payloadReads: payloadReads{Blocks: 4, Bytes: 200}}
	for _, tc := range []struct {
		name          string
		reads         payloadReads
		baselineName  string
		blocksAvoided int
		bytesAvoided  int64
		wantErr       bool
	}{
		{"same reads", payloadReads{Blocks: 4, Bytes: 200}, "sparse", 0, 0, false},
		{"pruned", payloadReads{Blocks: 1, Bytes: 50}, "sparse", 3, 150, false},
		{"no reads", payloadReads{}, "sparse", 4, 200, false},
		{"increased reads", payloadReads{Blocks: 5, Bytes: 50}, "sparse", 0, 0, true},
		{"increased bytes", payloadReads{Blocks: 4, Bytes: 201}, "sparse", 0, 0, true},
		{"different query", payloadReads{Blocks: 1, Bytes: 50}, "medium", 0, 0, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := measurement{Name: tc.baselineName, Literal: "literal", payloadReads: tc.reads}
			err := m.compareReads(base)
			if (err != nil) != tc.wantErr {
				t.Fatalf("compareReads error = %v", err)
			}
			if err == nil && (m.BlocksAvoided != tc.blocksAvoided || m.BytesAvoided != tc.bytesAvoided ||
				m.BaselineBlocksRead != base.Blocks || m.BaselineBytesRead != base.Bytes) {
				t.Errorf("wrong baseline accounting: %+v", m)
			}
		})
	}
}

func TestParsePeakRSS(t *testing.T) {
	for _, tc := range []struct {
		name, status string
		bytes        int64
	}{
		{"high water", "Name:\tsegment-layout\nVmHWM:\t2048 kB\nVmRSS:\t1024 kB\n", 2 << 20},
		{"absent", "VmRSS:\t1024 kB\n", 0},
		{"overflow", "VmHWM: 9007199254740992 kB\n", 0},
		{"negative", "VmHWM: -1 kB\n", 0},
		{"zero", "VmHWM: 0 kB\n", 0},
		{"bad unit", "VmHWM: 1 MB\n", 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parsePeakRSS([]byte(tc.status))
			if got != tc.bytes || (err != nil) != (tc.bytes == 0) {
				t.Fatalf("parsePeakRSS = %d, %v", got, err)
			}
		})
	}
}
