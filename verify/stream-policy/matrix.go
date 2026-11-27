package streampolicy

import (
	"fmt"
	"slices"
)

// matrixTarget pairs a request selector with the fixture's expected canonical
// stream and independent grants. The unknown selector has no scope or grants.
type matrixTarget struct {
	grant                         Grant
	stream                        Stream
	principalIndex, selectorIndex int
}

func (t matrixTarget) label() string {
	return fmt.Sprintf("principal %d, selector %d", t.principalIndex, t.selectorIndex)
}

func (f Fixture) targets() []matrixTarget {
	grants := make(map[Target]Grant, len(f.Grants))
	var principals []string
	for _, grant := range f.Grants {
		grants[grant.Target] = grant
		if !slices.Contains(principals, grant.Principal) {
			principals = append(principals, grant.Principal)
		}
	}

	streams := append(slices.Clone(f.Streams[:]), Stream{Selector: unknown})
	targets := make([]matrixTarget, 0, len(principals)*len(streams))
	for i, principal := range principals {
		for j, stream := range streams {
			target := Target{principal, stream.Selector}
			grant := grants[target]
			grant.Target = target
			targets = append(targets, matrixTarget{grant, stream, i + 1, j + 1})
		}
	}
	return targets
}

// rejection records the first clean response for one endpoint and status.
// Error body and application headers must then be identical across that class.
type rejection struct {
	status   int
	baseline Response
}

func (s *scenario) checkRejection(response Response, expected *rejection, references []string) error {
	if err := requireResponse(response, expected.status); err != nil {
		return err
	}
	if err := inspectResponse(response, s.denialMarkers); err != nil {
		return err
	}
	if err := inspectReferences(response, references); err != nil {
		return err
	}
	if !expected.baseline.Complete {
		expected.baseline = response
	} else if !sameResponse(response, expected.baseline) {
		return fmt.Errorf("%w: rejection exposes different response evidence", ErrViolation)
	}
	return nil
}
