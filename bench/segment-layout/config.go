package main

import (
	"flag"
	"fmt"
	"io"
)

const (
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
	batches, runs                 int
}

func parseConfig(args []string, revision string, output io.Writer) (config, error) {
	var cfg config
	flags := flag.NewFlagSet("segment-layout", flag.ContinueOnError)
	flags.SetOutput(output)
	flags.StringVar(&cfg.scale, "scale", "medium", "fixture scale (small, medium, or large)")
	flags.IntVar(&cfg.batches, "batches", 0, "custom ordinary batch count in place of -scale")
	flags.IntVar(&cfg.runs, "runs", 21, "cold samples per query and layout")
	flags.StringVar(&cfg.profile, "fixture", "mixed", "fixture profile (mixed or entropy)")
	flags.StringVar(&cfg.dir, "dir", "", "parent for temporary benchmark files (defaults to OS temp directory)")
	flags.StringVar(&cfg.revision, "revision", revision, "source revision to record")
	smoke := flags.Bool("smoke", false, "use the fixed small fixture and one sample per query")
	if err := flags.Parse(args); err != nil {
		return config{}, err
	}
	var explicitBatches, explicitScale bool
	flags.Visit(func(f *flag.Flag) {
		switch f.Name {
		case "batches":
			explicitBatches = true
		case "scale":
			explicitScale = true
		}
	})
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
	return cfg, nil
}
