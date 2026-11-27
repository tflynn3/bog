package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
)

const (
	queryEventPackage      = "bog/verify/query-model"
	maxQueryEventBytes     = 16 << 20                   // Combined wire bytes, including separators and ignored lines.
	maxQueryEventCount     = 100_000                    // Combined records, including build and unrelated events.
	maxQueryEventLineBytes = bufio.MaxScanTokenSize - 1 // Bytes before the newline.
)

var errQueryEventCount = errors.New("query event artifacts exceed event count limit")

// A qualification owns one budget and reads its artifacts in order. Charge
// bytes at the bounded reader, so scanner read-ahead and line endings count.
type queryEventBudget struct {
	byteLimit, bytesLeft   int64
	eventLimit, eventsLeft int
	byteOverflow           bool
}

func newQueryEventBudget(byteLimit int64, eventLimit int) *queryEventBudget {
	return &queryEventBudget{
		byteLimit: byteLimit, bytesLeft: byteLimit,
		eventLimit: eventLimit, eventsLeft: eventLimit,
	}
}

func (b *queryEventBudget) countError() error {
	return fmt.Errorf("%w (%d events)", errQueryEventCount, b.eventLimit)
}

func (b *queryEventBudget) byteError() error {
	return fmt.Errorf("aggregate query event budget: %w", artifactSizeError(b.byteLimit))
}

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

func validateQueryQualification(ctx context.Context, model, handlers string, id identity) error {
	return validateQueryQualificationWithBudget(ctx, model, handlers, id, newQueryEventBudget(maxQueryEventBytes, maxQueryEventCount))
}

func validateQueryQualificationWithBudget(ctx context.Context, model, handlers string, id identity, budget *queryEventBudget) error {
	if model == "" || handlers == "" {
		var missing []string
		if model == "" {
			missing = append(missing, "-query-model-events")
		}
		if handlers == "" {
			missing = append(missing, "-query-handler-events")
		}
		return fmt.Errorf("query qualification requires both -query-model-events and -query-handler-events: missing %s: %w",
			strings.Join(missing, ", "), os.ErrNotExist)
	}
	modelErr := validateQueryEventFile(ctx, "query model events", model, pinnedQueryTests, []string{pinnedQueryReplay, pinnedQuery104}, id, budget)
	if budget.byteOverflow || errors.Is(modelErr, errQueryEventCount) {
		return modelErr
	}
	handlerErr := validateQueryEventFile(ctx, "query handler events", handlers, queryHandlerTests, nil, id, budget)
	return errors.Join(modelErr, handlerErr)
}

func validateQueryEventFile(ctx context.Context, phase, path string, tests, pinned []string, id identity, budget *queryEventBudget) error {
	file, _, err := openRegular(ctx, nil, path, budget.bytesLeft)
	if err != nil {
		if errors.Is(err, errArtifactOversized) {
			err = fmt.Errorf("aggregate query event budget (%d bytes): %w", budget.byteLimit, err)
		}
		return fmt.Errorf("%s: open %q: %w", phase, path, err)
	}
	err = validateQueryEventsWithBudget(ctx, file, budget, tests, pinned, id)
	if err := errors.Join(err, file.Close()); err != nil {
		return fmt.Errorf("%s: validate %q: %w", phase, path, err)
	}
	return nil
}

type queryTestEvent struct {
	Action  string
	Package string
	Test    string
	Output  string
}

func validateQueryEvents(ctx context.Context, r io.Reader, limit int64, tests, pinned []string, id identity) error {
	return validateQueryEventsWithBudget(ctx, r, newQueryEventBudget(limit, maxQueryEventCount), tests, pinned, id)
}

func validateQueryEventsWithBudget(ctx context.Context, r io.Reader, budget *queryEventBudget, tests, pinned []string, id identity) error {
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

	reader := newArtifactReader(ctx, r, budget.bytesLeft)
	defer func() {
		budget.bytesLeft = reader.remaining
		budget.byteOverflow = reader.overflow
	}()
	scanner := bufio.NewScanner(reader)
	scanner.Buffer(nil, maxQueryEventLineBytes+1)
	scanner.Split(scanQueryEventLine)
	var observed error
	for line := 1; ; line++ {
		scanned := scanner.Scan()
		// A failed read can leave an unterminated partial token. Do not
		// interpret it as an event or mask the underlying read failure.
		if err := scanner.Err(); err != nil || ctx.Err() != nil {
			if reader.overflow {
				err = budget.byteError()
			}
			return errors.Join(observed, fmt.Errorf("read Go test events at line %d: %w", line, errors.Join(err, ctx.Err())))
		}
		if !scanned {
			break
		}
		if budget.eventsLeft == 0 {
			return errors.Join(observed, fmt.Errorf("read Go test events at line %d: %w", line, budget.countError()))
		}
		budget.eventsLeft--
		if observed != nil {
			// A bad event already invalidated the artifact. Continue
			// charging its remaining wire bytes and event records.
			continue
		}
		event, err := decodeQueryEvent(scanner.Bytes())
		if err != nil {
			observed = fmt.Errorf("decode Go test event at line %d: %w", line, err)
			continue
		}
		if event.Package != queryEventPackage {
			continue
		}
		root, _, _ := strings.Cut(event.Test, "/")
		switch event.Action {
		case "fail":
			observed = fmt.Errorf("%s reported failure", queryEventName(event.Test))
		case "skip":
			if event.Test == "" || roots[root] {
				observed = fmt.Errorf("%s reported skip", queryEventName(event.Test))
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
					observed = fmt.Errorf("%s candidate identity does not match the pinned build", event.Test)
					continue
				}
				identities[event.Test]++
			}
		}
	}
	if observed != nil {
		return observed
	}
	var missing []error
	for _, test := range required {
		if passes[test] != 1 {
			missing = append(missing, fmt.Errorf("%s reported %d passes, expected 1", queryEventName(test), passes[test]))
		}
	}
	for _, test := range pinned {
		if identities[test] != 1 {
			missing = append(missing, fmt.Errorf("%s reported %d candidate identities, expected 1", test, identities[test]))
		}
	}
	return errors.Join(missing...)
}

func scanQueryEventLine(data []byte, atEOF bool) (int, []byte, error) {
	if len(data) > maxQueryEventLineBytes && bytes.IndexByte(data, '\n') == -1 {
		return 0, nil, fmt.Errorf("%w (Go test event line exceeds %d bytes)", errArtifactOversized, maxQueryEventLineBytes)
	}
	return bufio.ScanLines(data, atEOF)
}

func decodeQueryEvent(data []byte) (queryTestEvent, error) {
	var event queryTestEvent
	fields, err := decodeFields(data, map[string]any{"Action": &event.Action})
	if err != nil {
		return event, err
	}
	if event.Action == "" {
		return event, fmt.Errorf("%w: empty Action", errArtifactMalformed)
	}
	// The go command can interleave build diagnostics with test events.
	// These use ImportPath in place of Package.
	if event.Action == "build-output" || event.Action == "build-fail" {
		var path string
		if err := readQueryEventField(fields["ImportPath"], "ImportPath", &path); err != nil {
			return event, err
		}
		if path == "" {
			return event, fmt.Errorf("%w: empty ImportPath", errArtifactMalformed)
		}
		return event, nil
	}
	if err := readQueryEventField(fields["Package"], "Package", &event.Package); err != nil {
		return event, err
	}
	if event.Package == "" {
		return event, fmt.Errorf("%w: empty Package", errArtifactMalformed)
	}
	if raw, ok := fields["Test"]; ok {
		if err := readQueryEventField(raw, "Test", &event.Test); err != nil {
			return event, err
		}
	}
	if raw, ok := fields["Output"]; ok || event.Action == "output" {
		if err := readQueryEventField(raw, "Output", &event.Output); err != nil {
			return event, err
		}
	}
	return event, nil
}

func readQueryEventField(raw jsontext.Value, key string, dest *string) error {
	if len(raw) == 0 || raw.Kind() == 'n' {
		return fmt.Errorf("%w: missing or null %q", errArtifactMalformed, key)
	}
	if err := json.Unmarshal(raw, dest); err != nil {
		return fmt.Errorf("%w: %s: %w", errArtifactMalformed, key, err)
	}
	return nil
}

func queryEventName(test string) string {
	if test == "" {
		return queryEventPackage
	}
	return test
}
