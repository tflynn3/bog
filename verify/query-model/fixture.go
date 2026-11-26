package querymodel

import (
	"math/rand/v2"
	"time"
)

var fixtureOffsets = [...]*time.Location{
	time.UTC,
	time.UTC,
	time.FixedZone("west", -7*60*60),
	time.FixedZone("east", (5*60+30)*60),
}

// SeededBatches generates count rows starting at epoch, four per millisecond.
// Ordered fixtures have nondecreasing timestamps across all batches. Shuffled
// fixtures mix historical and newer rows while preserving the same row multiset.
// Each full group of four includes an exact duplicate and offset-equivalent
// timestamps. The returned batches own their rows and are safe to edit.
func SeededBatches(seed int64, epoch time.Time, count int, ordered bool) [][]Row {
	rng := rand.New(rand.NewPCG(uint64(seed), 0))
	lines := [...]string{
		"prefix needle suffix", "prefix Needle suffix", "needle界",
		"literal .* [x]", "miss", "", "trailing needle",
	}
	rows := make([]Row, 0, max(count, 0))
	for i := 0; i < count; i++ {
		if i%4 == 1 {
			rows = append(rows, rows[i-1])
			continue
		}
		instant := epoch.Add(time.Duration(i/4) * time.Millisecond)
		rows = append(rows, Row{
			Time: instant.In(fixtureOffsets[i%4]),
			Line: lines[rng.IntN(len(lines))],
		})
	}

	// Choose batch boundaries before shuffling so both modes use the same sizes.
	var batches [][]Row
	for i := 0; i < len(rows); {
		end := i + min(len(rows)-i, 1+rng.IntN(64))
		batches = append(batches, rows[i:end:end])
		i = end
	}
	if !ordered {
		rng.Shuffle(len(rows), func(i, j int) { rows[i], rows[j] = rows[j], rows[i] })
	}
	return batches
}
