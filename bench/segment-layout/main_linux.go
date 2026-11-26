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
	if len(os.Args) > 1 && os.Args[1] == workerArgument {
		if err := workerMain(os.Args[1:], os.Stdout, coldCache, peakWorkerRSS); err != nil {
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
	if err := runController(cfg); err != nil {
		log.Print(err)
		os.Exit(1)
	}
}

func runController(cfg config) error {
	executable, err := os.Executable()
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	return run(ctx, cfg, executable, os.Stdout)
}
