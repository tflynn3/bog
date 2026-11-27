//go:build cgo

package fuzz

import (
	"errors"
	"testing"

	"bog/internal/checkpoint"
	"bog/internal/history"
)

// Exercise the same scanner and barrier outcomes on the development host.
func TestScanMemoryPortableWorkload(t *testing.T) {
	const readers = scanMemoryQualificationReaders
	path := t.TempDir()
	fixture := seedScanMemory(t, path)
	budget := checkpoint.NewBudget(checkpoint.Limits{
		checkpoint.PinnedGenerations: 1, checkpoint.LiveCaptures: readers,
	})
	catalog := history.NewCatalog(budget)
	defer func() { requireRetainedOK(t, catalog.Close()) }()
	gen, err := catalog.Replace(t.Context(), path)
	requireRetainedOK(t, err)
	if gen.Boundary.NextOrdinal != uint64(fixture.Rows) {
		t.Fatalf("loaded boundary %+v does not match fixture %+v", gen.Boundary, fixture)
	}
	work := scanMemoryWorkload{catalog: catalog, budget: budget, path: path, rows: scanMemoryRows(t), readers: readers}
	limits := scanMemoryLimits(readers)
	for _, phase := range scanMemoryPhases {
		t.Run(string(phase), func(t *testing.T) {
			if phase == scanRetry {
				// All readers finish successfully, but the accounting verdict
				// fails independently. The same catalog must allow the retry.
				tight := limits
				tight.Allocated = 1
				rejected, err := work.measure(t.Context(), scanSuccess, tight)
				if !errors.Is(err, errScanWorkerBudget) || rejected.Allocated <= tight.Allocated ||
					rejected.CompletedReaders != readers || rejected.PostCaptures != 0 || rejected.PostPins != 1 {
					t.Fatalf("allocation budget rejection left invalid evidence: %+v, %v", rejected, err)
				}
			}
			report, err := work.measure(t.Context(), phase, limits)
			requireRetainedOK(t, err)
			t.Logf("%+v", report)
		})
	}
	requireRetainedOK(t, catalog.Close())
	if got := budget.Usage().InUse; got != (checkpoint.Limits{}) {
		t.Fatalf("live reservations after catalog close: %v", got)
	}
	select {
	case <-gen.Drained:
	default:
		t.Fatal("generation still pinned after catalog close")
	}
}
