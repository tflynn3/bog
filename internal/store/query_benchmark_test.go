package store

import (
	"testing"
	"time"
)

func BenchmarkLiteralQuery(b *testing.B) {
	epoch := time.Unix(1700000000, 0)
	var s Store
	// Seven runs with interleaved timestamps, as after multiple replay batches.
	for batch := 0; batch < 7; batch++ {
		rows := make([]Row, 10000)
		for i := range rows {
			n := batch*len(rows) + i
			line := "dense ordinary log message"
			if n%10000 == 0 {
				line += " raretoken"
			}
			if batch == 0 {
				line += " skewtoken"
			}
			rows[i] = Row{epoch.Add(time.Duration((i*7+batch)/3) * time.Second), line}
		}
		s.Publish(rows)
	}
	for _, tc := range []struct {
		name, literal string
		end           time.Time
	}{
		{"dense", "dense", epoch.Add(8 * time.Hour)},
		{"narrow", "dense", epoch.Add(100 * time.Second)},
		{"skewed", "skewtoken", epoch.Add(8 * time.Hour)},
		{"rare", "raretoken", epoch.Add(8 * time.Hour)},
		{"absent", "absenttoken", epoch.Add(8 * time.Hour)},
	} {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				s.QueryPage(epoch, tc.end, tc.literal, 100, nil, nil)
			}
		})
	}
}
