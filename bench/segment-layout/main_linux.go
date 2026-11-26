package main

import (
	"errors"
	"flag"
	"log"
	"os"
	"runtime/debug"
)

func sourceRevision() string {
	if build, ok := debug.ReadBuildInfo(); ok {
		var revision string
		var dirty bool
		for _, setting := range build.Settings {
			switch setting.Key {
			case "vcs.revision":
				revision = setting.Value
			case "vcs.modified":
				dirty = setting.Value == "true"
			}
		}
		if revision != "" {
			if dirty {
				return revision + "-dirty"
			}
			return revision
		}
	}
	return "unknown"
}

func main() {
	log.SetFlags(0)
	cfg, err := parseConfig(os.Args[1:], sourceRevision(), os.Stderr)
	if errors.Is(err, flag.ErrHelp) {
		return
	}
	if err != nil {
		log.Print(err)
		os.Exit(2)
	}
	if err := run(cfg, os.Stdout); err != nil {
		log.Print(err)
		os.Exit(1)
	}
}
