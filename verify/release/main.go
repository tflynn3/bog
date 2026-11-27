package main

import (
	"context"
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
	var output, admission, durability, capacity, parity string
	var queryModelEvents, queryHandlerEvents string
	var opts validationOptions
	if validate {
		opts.requireRetarget = os.Getenv("BOG_RELEASE_REQUIRE_RETARGET") == "1"
		flags.BoolVar(&opts.requireCutover, "require-cutover", os.Getenv("BOG_RELEASE_REQUIRE_CUTOVER") == "1", "require live capacity, recovery parity, immediate post-restore crash, and two-cycle tail-only evidence")
		flags.StringVar(&admission, "admission", "", "admission report")
		flags.StringVar(&durability, "durability", "", "durability report")
		flags.StringVar(&capacity, "capacity", "", "capacity report")
		flags.StringVar(&parity, "parity", "", "recovery parity report (required with -require-cutover)")
		flags.StringVar(&queryModelEvents, "query-model-events", "", "native pinned query replay and #104 Go test JSON events")
		flags.StringVar(&queryHandlerEvents, "query-handler-events", "", "native #104 candidate handler Go test JSON events")
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
			if checkErr := validateFile(check.path, id, check.cases, opts); checkErr != nil {
				err = errors.Join(err, fmt.Errorf("%s report: %w", check.name, checkErr))
			}
		}
		if parity == "" && opts.requireCutover {
			err = errors.Join(err, errors.New("restored parity report: -parity is required with -require-cutover"))
		} else if parity != "" {
			cases := []string{restoredParityCase}
			if opts.requireCutover {
				cases = cutoverParityCases
			}
			if checkErr := validateFile(parity, id, cases, opts); checkErr != nil {
				err = errors.Join(err, fmt.Errorf("restored parity report: %w", checkErr))
			}
		}
		if queryModelEvents != "" || queryHandlerEvents != "" {
			err = errors.Join(err, validateQueryQualification(queryModelEvents, queryHandlerEvents, id))
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
	return errors.Join(probeErr, writeReport(output, results))
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	err := run(ctx, os.Args[1:])
	stop()
	if err != nil && !errors.Is(err, flag.ErrHelp) {
		log.Fatal(err)
	}
}
