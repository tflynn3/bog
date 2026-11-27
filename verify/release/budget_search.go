package main

type budgetProbeOutcome string

const (
	probeIndeterminate budgetProbeOutcome = "indeterminate"
	probeRejected      budgetProbeOutcome = "clean_rejection"
	probeIssued        budgetProbeOutcome = "issued_cursor"
	probeExhausted     budgetProbeOutcome = "exhausted"
)

func (o budgetProbeOutcome) issued() bool {
	return o == probeIssued || o == probeExhausted
}

// A budget probe reports what the candidate demonstrated. An error stops the
// search even when the opening outcome is known. Admission can report an
// issuing observation with a broken continuation and keep narrowing, while
// native backfill returns an error for any walk that issues but fails to exhaust.
type budgetProbeFunc func(budget int) (budgetProbeOutcome, error)

type budgetBoundary struct {
	issuing, rejected int // Lowest issuing and highest cleanly rejected budgets observed.
}

func (b *budgetBoundary) record(budget int, outcome budgetProbeOutcome) {
	if outcome == probeRejected {
		b.rejected = max(b.rejected, budget)
	}
	if outcome.issued() && (b.issuing == 0 || budget < b.issuing) {
		b.issuing = budget
	}
}

// At the budget floor, zero denotes that no predecessor is required.
func (b budgetBoundary) exact() bool {
	return b.issuing > 0 && b.rejected == b.issuing-1
}

// bisectBudget searches the inclusive range [1, maxBudget], assuming a
// monotone opening result. A zero reference starts at the midpoint. A
// positive reference starts at that budget, capped at maxBudget.
func bisectBudget(maxBudget, reference int, probe budgetProbeFunc) (budgetBoundary, error) {
	var boundary budgetBoundary
	for low, high := 1, maxBudget; low <= high; {
		budget := low + (high-low)/2
		if reference > 0 {
			budget = min(reference, high)
			reference = 0
		}
		outcome, err := probe(budget)
		boundary.record(budget, outcome)
		if err != nil {
			return boundary, err
		}
		switch outcome {
		case probeRejected:
			if budget == high {
				return boundary, nil
			}
			low = budget + 1
		case probeIssued, probeExhausted:
			high = budget - 1
		default:
			return boundary, nil
		}
	}
	return boundary, nil
}
