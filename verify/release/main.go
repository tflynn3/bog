package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func run(ctx context.Context, args []string) error {
	if len(args) > 0 && args[0] == "qualify" {
		flags := flag.NewFlagSet("release qualify", flag.ContinueOnError)
		root := flags.String("evidence", "", "directory containing both architecture evidence bundles")
		revision := flags.String("revision", "", "full candidate commit SHA")
		fixture := flags.String("fixture", "", "release fixture ID")
		if err := flags.Parse(args[1:]); err != nil {
			return err
		}
		if flags.NArg() != 0 {
			return errors.New("unexpected positional arguments")
		}
		return qualifyRelease(*root, *revision, *fixture)
	}
	validate := len(args) > 0 && args[0] == "validate"
	if validate {
		args = args[1:]
	}
	flags := flag.NewFlagSet("release", flag.ContinueOnError)
	binary := flags.String("binary", "", "absolute path to the candidate binary")
	revision := flags.String("revision", "", "full candidate commit SHA")
	fixture := flags.String("fixture", "", "release fixture ID")
	var output, arch string
	var reports releaseReports
	if validate {
		flags.StringVar(&reports.admission, "admission", "", "admission report")
		flags.StringVar(&reports.durability, "durability", "", "durability report")
		flags.StringVar(&reports.capacity, "capacity", "", "capacity report")
		flags.StringVar(&reports.parity, "parity", "", "restored parity report")
		flags.StringVar(&arch, "arch", "", "require native Linux release evidence for amd64 or arm64")
	} else {
		flags.StringVar(&output, "report", "", "admission report output path")
	}
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return errors.New("unexpected positional arguments")
	}
	if !validate && output == "" {
		return errors.New("-report is required")
	}
	if arch != "" {
		if *fixture != releaseFixture {
			return fmt.Errorf("native validation requires fixture %s", releaseFixture)
		}
		if err := verifyNativeHost(arch); err != nil {
			return err
		}
	}
	id, err := readCandidateIdentity(*binary, *revision, *fixture, arch)
	if err != nil {
		return err
	}
	if validate {
		return reports.validate(id, arch != "")
	}

	ctx, cancel := context.WithTimeout(ctx, 90*time.Second)
	defer cancel()
	results := report{id, make(map[string]outcome)}
	probeErr := probeAdmission(ctx, *binary, results.Cases)
	for _, name := range admissionCases {
		if _, ok := results.Cases[name]; !ok {
			detail := "prerequisite failed"
			results.Cases[name] = outcome{Status: "skip", Detail: &detail}
		}
	}
	data, err := json.MarshalIndent(results, "", "  ")
	if err == nil {
		err = os.WriteFile(output, append(data, '\n'), 0600)
	}
	return errors.Join(probeErr, err)
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	err := run(ctx, os.Args[1:])
	stop()
	if err != nil && !errors.Is(err, flag.ErrHelp) {
		log.Fatal(err)
	}
}
