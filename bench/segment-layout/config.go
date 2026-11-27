package main

import (
	"flag"
	"fmt"
	"io"
	"math"
	"time"
)

const (
	seed       = 44
	maxBatches = 10000
	maxRuns    = 1000
)

var scaleBatches = map[string]int{
	"small":  64,
	"medium": 2048,
	"large":  8192,
}

type config struct {
	dir, revision, profile, scale string
	validateReport                string
	batches, runs                 int
	fixtureSeed                   uint64
	binary, binaryRevision        string
	timeout                       time.Duration
	incremental                   bool
	cold                          bool
}

func parseConfig(args []string, revision string, output io.Writer) (config, error) {
	var cfg config
	flags := flag.NewFlagSet("segment-layout", flag.ContinueOnError)
	flags.SetOutput(output)
	flags.StringVar(&cfg.scale, "scale", "medium", "fixture scale (small, medium, or large)")
	flags.IntVar(&cfg.batches, "batches", 0, "custom ordinary batch count in place of -scale")
	flags.IntVar(&cfg.runs, "runs", 21, "cold samples per query and layout")
	flags.Uint64Var(&cfg.fixtureSeed, "seed", seed, "fixture PCG seed")
	flags.DurationVar(&cfg.timeout, "timeout", 5*time.Minute, "timeout per query sample or compaction scenario")
	flags.StringVar(&cfg.binary, "bog-bin", "", "optional pinned Linux bog binary")
	flags.StringVar(&cfg.binaryRevision, "bog-revision", "", "full clean revision of -bog-bin")
	flags.StringVar(&cfg.profile, "fixture", "mixed", "fixture profile (mixed or entropy)")
	flags.StringVar(&cfg.dir, "dir", "", "parent for temporary benchmark files (defaults to OS temp directory)")
	flags.StringVar(&cfg.revision, "revision", revision, "source revision to record")
	flags.BoolVar(&cfg.incremental, "incremental", false, "compare successive segment publications against production v1")
	flags.BoolVar(&cfg.cold, "cold", false, "compare cold incremental snapshots with production v1")
	flags.StringVar(&cfg.validateReport, "validate-report", "", "validate an incremental or cold benchmark report and exit")
	smoke := flags.Bool("smoke", false, "use the fixed small fixture and one sample per query")
	if err := flags.Parse(args); err != nil {
		return config{}, err
	}
	var explicitBatches, explicitScale, validate bool
	flags.Visit(func(f *flag.Flag) {
		switch f.Name {
		case "batches":
			explicitBatches = true
		case "scale":
			explicitScale = true
		case "validate-report":
			validate = true
		}
	})
	if validate {
		if cfg.validateReport == "" || flags.NFlag() != 1 || flags.NArg() != 0 {
			return config{}, fmt.Errorf("-validate-report requires a path and must be used alone")
		}
		return cfg, nil
	}
	if explicitBatches && explicitScale {
		return config{}, fmt.Errorf("choose either -batches or -scale")
	}
	if explicitBatches {
		cfg.scale = "custom"
	} else {
		var ok bool
		cfg.batches, ok = scaleBatches[cfg.scale]
		if !ok {
			return config{}, fmt.Errorf("require -scale small, medium, or large")
		}
	}
	if *smoke {
		cfg.scale, cfg.batches, cfg.runs = "smoke", scaleBatches["small"], 1
	}
	if cfg.batches < 1 || cfg.batches > maxBatches || cfg.runs < 1 || cfg.runs > maxRuns || flags.NArg() != 0 {
		return config{}, fmt.Errorf("require 1–%d batches, 1–%d runs and no positional arguments", maxBatches, maxRuns)
	}
	if cfg.profile != "mixed" && cfg.profile != "entropy" {
		return config{}, fmt.Errorf("require -fixture mixed or -fixture entropy")
	}
	if cfg.timeout <= 0 || (cfg.binary == "") != (cfg.binaryRevision == "") {
		return config{}, fmt.Errorf("require a positive -timeout and provide -bog-bin and -bog-revision together")
	}
	if cfg.incremental && cfg.timeout > math.MaxInt64/5 {
		return config{}, fmt.Errorf("-timeout exceeds the incremental worker limit")
	}
	if cfg.incremental && cfg.binary != "" {
		return config{}, fmt.Errorf("-bog-bin requires the default or -cold benchmark")
	}
	if cfg.incremental && cfg.cold {
		return config{}, fmt.Errorf("choose either -cold or -incremental")
	}
	return cfg, nil
}
