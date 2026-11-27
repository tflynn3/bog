package main

import (
	"io"
	"slices"
	"testing"
)

func TestParseConfig(t *testing.T) {
	for _, tc := range []struct {
		name        string
		args        []string
		scale       string
		batches     int
		runs        int
		wantErr     bool
		incremental bool
	}{
		{name: "default", scale: "medium", batches: 2048, runs: 21},
		{name: "small", args: []string{"-scale", "small"}, scale: "small", batches: 64, runs: 21},
		{name: "large", args: []string{"-scale", "large", "-fixture", "entropy", "-runs", "2"}, scale: "large", batches: 8192, runs: 2},
		{name: "custom", args: []string{"-batches", "17"}, scale: "custom", batches: 17, runs: 21},
		{name: "smoke", args: []string{"-smoke", "-runs", "5"}, scale: "smoke", batches: 64, runs: 1},
		{name: "incremental smoke", args: []string{"-incremental", "-smoke"}, scale: "smoke", batches: 64, runs: 1, incremental: true},
		{name: "incremental with production binary", args: []string{"-incremental", "-bog-bin", "candidate", "-bog-revision", "test"}, wantErr: true},
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
			if err == nil && (cfg.scale != tc.scale || cfg.batches != tc.batches || cfg.runs != tc.runs || cfg.incremental != tc.incremental || cfg.revision != "test") {
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
		{"same reads", payloadReads{4, 200}, "sparse", 0, 0, false},
		{"pruned", payloadReads{1, 50}, "sparse", 3, 150, false},
		{"no reads", payloadReads{}, "sparse", 4, 200, false},
		{"increased reads", payloadReads{5, 50}, "sparse", 0, 0, true},
		{"increased bytes", payloadReads{4, 201}, "sparse", 0, 0, true},
		{"different query", payloadReads{1, 50}, "medium", 0, 0, true},
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

func TestSampleAggregation(t *testing.T) {
	t.Parallel()
	stats := newMeasurement(query{name: "sparse"}, 2, 10)
	reads := payloadReads{2, 128}
	if err := stats.addSample(reads, 10, 512, 4096); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		reads   payloadReads
		elapsed int64
		decode  int
		rss     uint64
	}{
		{payloadReads{1, 128}, 0, 64, 8192},
		{reads, -1, 64, 8192},
		{reads, 0, 0, 8192},
		{reads, 0, 64, 0},
		{payloadReads{0, 128}, 0, 0, 8192},
	} {
		if err := stats.addSample(tc.reads, tc.elapsed, tc.decode, tc.rss); err == nil {
			t.Fatalf("accepted invalid sample %+v", tc)
		}
	}
	if err := stats.addSample(reads, 2, 1024, 8192); err != nil {
		t.Fatal(err)
	}
	if err := stats.addSample(reads, 6, 64, 4096); err != nil {
		t.Fatal(err)
	}
	if stats.MatchingFraction != 0.2 || stats.payloadReads != reads || stats.PeakDecodeBytes != 1024 ||
		stats.PeakRSSBytes != 8192 || p95(stats.ColdSamplesNS) != 10 ||
		!slices.Equal(stats.ColdSamplesNS, []int64{10, 2, 6}) ||
		!slices.Equal(stats.SampleRSSBytes, []uint64{4096, 8192, 4096}) {
		t.Fatalf("wrong aggregate %+v", stats)
	}
	if err := stats.compareReads(newMeasurement(query{name: "different"}, 2, 10)); err == nil {
		t.Fatal("accepted unrelated baseline")
	}
}

func TestWriteAmplification(t *testing.T) {
	t.Parallel()
	for _, boundary := range []string{benchmarkBoundary, componentBoundary, liveBoundary} {
		got := writeAmplification(500, 200, boundary)
		if got.WrittenBytes != 500 || got.AppendedWALBytes != 200 || got.Ratio != 2.5 || got.Boundary != boundary {
			t.Fatalf("bad amplification %+v", got)
		}
	}
}

func TestValidationConfig(t *testing.T) {
	for _, args := range [][]string{
		{"-validate-report", ""},
		{"-validate-report", "report.json", "-incremental"},
		{"-validate-report", "report.json", "extra"},
	} {
		if _, err := parseConfig(args, "revision", io.Discard); err == nil {
			t.Fatalf("accepted invalid validation args %q", args)
		}
	}
	cfg, err := parseConfig([]string{"-validate-report", "report.json"}, "revision", io.Discard)
	if err != nil || cfg.validateReport != "report.json" {
		t.Fatalf("validation config %+v: %v", cfg, err)
	}
}
