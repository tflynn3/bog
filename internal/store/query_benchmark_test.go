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

func BenchmarkLiteralFallbackUnfiltered(b *testing.B) {
	epoch := time.Unix(1700000000, 0)
	var s Store
	line := string(make([]byte, 520))
	for batch := 0; batch < 7; batch++ {
		rows := make([]Row, 9072)
		for i := range rows {
			rows[i] = Row{epoch.Add(time.Duration(i*7+batch) * time.Second), line}
		}
		s.Publish(rows)
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		s.QueryPage(epoch, epoch.Add(24*time.Hour), "absenttoken", 100, nil, nil)
	}
}

func BenchmarkLiteralContinuationBloom(b *testing.B) {
	epoch := time.Unix(1700000000, 0)
	var s Store
	rows := make([]Row, 100000)
	for i := range rows {
		line := "ordinary log message"
		if i == 80000 || i == len(rows)-1 {
			line = "rare needle"
		}
		rows[i] = Row{epoch.Add(time.Duration(i) * time.Second), line}
	}
	s.Publish(rows)
	count := len(rows)
	after := Position{rows[80000].TS, 80000}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		s.QueryPage(epoch, epoch.Add(48*time.Hour), "rare needle", 1, &count, &after)
	}
}
