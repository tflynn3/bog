package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"os"
	"os/signal"
	"runtime/debug"
	"syscall"
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
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if len(os.Args) == 3 && os.Args[1] == workerFlag {
		if err := worker(ctx, os.Args[2], os.Stdout); err != nil {
			log.Print(err)
			os.Exit(1)
		}
		return
	}
	cfg, err := parseConfig(os.Args[1:], sourceRevision(), os.Stderr)
	if errors.Is(err, flag.ErrHelp) {
		return
	}
	if err != nil {
		log.Print(err)
		os.Exit(2)
	}
	if err := run(ctx, cfg, os.Stdout); err != nil {
		log.Print(err)
		os.Exit(1)
	}
}
