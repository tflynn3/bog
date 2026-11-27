package main

import (
	"bufio"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"strings"
)

const queryEventPackage = "bog/verify/query-model"

const (
	pinnedQueryReplay = "TestPinnedQueryModelReplay"
	pinnedQuery104    = "TestPinnedQueryModel104"
)

var pinnedQueryTests = []string{
	pinnedQueryReplay + "/ordinary",
	pinnedQueryReplay + "/retained",
	pinnedQuery104 + "/legacy/opening-limit-1",
	pinnedQuery104 + "/legacy/opening-limit-10",
	pinnedQuery104 + "/retained/opening-limit-1",
	pinnedQuery104 + "/retained/opening-limit-10",
	"TestPinnedQueryCandidateStartup",
	"TestPinnedQueryCandidateRecoversFromTransientForeignListener",
}

var queryHandlerTests = []string{
	"TestQuery104CaptureLifecycle/legacy-slot-reuse",
	"TestQuery104CaptureLifecycle/retained-slot-reuse",
	"TestQuery104CaptureLifecycle/legacy-retirement",
	"TestQuery104FittingPages/legacy/opening-limit-1",
	"TestQuery104FittingPages/legacy/opening-limit-10",
	"TestQuery104FittingPages/retained/opening-limit-1",
	"TestQuery104FittingPages/retained/opening-limit-10",
	"TestQueryCaptureSurvivesFailedOpeningWrite/legacy",
	"TestQueryCaptureSurvivesFailedOpeningWrite/retained",
	"TestRetainedCaptureRetirementDrainsAtCapacity",
}

func validateQueryQualification(model, handlers string, id identity) error {
	if model == "" || handlers == "" {
		return errors.New("query qualification requires both -query-model-events and -query-handler-events")
	}
	return errors.Join(
		validateQueryEventFile(model, pinnedQueryTests, []string{pinnedQueryReplay, pinnedQuery104}, id),
		validateQueryEventFile(handlers, queryHandlerTests, nil, id),
	)
}

func validateQueryEventFile(path string, tests, pinned []string, id identity) (err error) {
	file, err := openRegular(path)
	if err != nil {
		return err
	}
	defer func() {
		if err = errors.Join(err, file.Close()); err != nil {
			err = fmt.Errorf("query events %s: %w", path, err)
		}
	}()
	return validateQueryEvents(file, tests, pinned, id)
}

type queryTestEvent struct {
	Action  string
	Package string
	Test    string
	Output  string
}

func validateQueryEvents(r io.Reader, tests, pinned []string, id identity) error {
	// An empty test name denotes the package result. A parent can pass
	// even when a required subtest skips, so require every named level.
	required := []string{""}
	passes := map[string]int{"": 0}
	roots := make(map[string]bool)
	for _, test := range tests {
		for {
			if _, exists := passes[test]; !exists {
				passes[test] = 0
				required = append(required, test)
			}
			separator := strings.LastIndexByte(test, '/')
			if separator == -1 {
				roots[test] = true
				break
			}
			test = test[:separator]
		}
	}
	identities := make(map[string]int, len(pinned))
	for _, test := range pinned {
		identities[test] = 0
	}
	wantIdentity := fmt.Sprintf("%s binary_sha256=%s", id.Revision, id.BinarySHA256)

	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		var event queryTestEvent
		if err := json.Unmarshal(scanner.Bytes(), &event); err != nil {
			return fmt.Errorf("decode Go test event: %w", err)
		}
		if event.Package != queryEventPackage {
			continue
		}
		root, _, _ := strings.Cut(event.Test, "/")
		switch event.Action {
		case "fail":
			return fmt.Errorf("%s reported failure", queryEventName(event.Test))
		case "skip":
			if event.Test == "" || roots[root] {
				return fmt.Errorf("%s reported skip", queryEventName(event.Test))
			}
		case "pass":
			if _, required := passes[event.Test]; required {
				passes[event.Test]++
			}
		case "output":
			if _, pinned := identities[event.Test]; !pinned {
				continue
			}
			if _, logged, found := strings.Cut(event.Output, "candidate revision="); found {
				if strings.TrimSuffix(logged, "\n") != wantIdentity {
					return fmt.Errorf("%s candidate identity does not match the pinned build", event.Test)
				}
				identities[event.Test]++
			}
		}
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("read Go test events: %w", err)
	}
	for _, test := range required {
		if passes[test] != 1 {
			return fmt.Errorf("%s reported %d passes, expected 1", queryEventName(test), passes[test])
		}
	}
	for _, test := range pinned {
		if identities[test] != 1 {
			return fmt.Errorf("%s reported %d candidate identities, expected 1", test, identities[test])
		}
	}
	return nil
}

func queryEventName(test string) string {
	if test == "" {
		return queryEventPackage
	}
	return test
}
