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
	validate := len(args) > 0 && args[0] == "validate"
	if validate {
		args = args[1:]
	}
	flags := flag.NewFlagSet("release", flag.ContinueOnError)
	binary := flags.String("binary", "", "absolute path to the candidate binary")
	revision := flags.String("revision", "", "full candidate commit SHA")
	fixture := flags.String("fixture", "", "release fixture ID")
	var output, admission, durability, capacity string
	if validate {
		flags.StringVar(&admission, "admission", "", "admission report")
		flags.StringVar(&durability, "durability", "", "durability report")
		flags.StringVar(&capacity, "capacity", "", "capacity report")
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
	id, err := readIdentity(*binary, *revision, *fixture)
	if err != nil {
		return err
	}
	if validate {
		for _, check := range []struct {
			name, path string
			cases      []string
		}{
			{"admission", admission, admissionCases},
			{"durability", durability, durabilityCases},
			{"capacity", capacity, capacityCases},
		} {
			if checkErr := validateFile(check.path, id, check.cases); checkErr != nil {
				err = errors.Join(err, fmt.Errorf("%s report: %w", check.name, checkErr))
			}
		}
		return err
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
