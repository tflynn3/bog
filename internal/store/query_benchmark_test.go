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

func BenchmarkRestoredSingleRun(b *testing.B) {
	epoch := time.Unix(1700000000, 0)
	rows := make([]Row, 100000)
	for i := range rows {
		line := "dense ordinary log message"
		if i%25000 == 0 || i == len(rows)-1 {
			line += " raretoken"
		}
		rows[i] = Row{epoch.Add(time.Duration(i) * time.Second), line}
	}
	count := len(rows)
	// A dense prefix followed by a sparse tail exercises the bounded attempt and fallback.
	for i := 1000; i < len(rows)-1; i++ {
		rows[i].Line = "ordinary log message"
	}
	rows[len(rows)-1].Line = "dense raretoken"
	builder := NewRecoveryBuilder()
	builder.Add(rows)
	var s Store
	builder.Install(&s)
	after := Position{rows[999].TS, 999}
	for _, tc := range []struct {
		name, literal string
		snapshot      *int
		after         *Position
	}{
		{"dense", "ordinary", nil, nil}, {"rare", "raretoken", nil, nil},
		{"absent", "absenttoken", nil, nil}, {"dense_to_sparse", "dense", &count, &after},
	} {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				s.QueryPage(epoch, epoch.Add(48*time.Hour), tc.literal, 100, tc.snapshot, tc.after)
			}
		})
	}
}

func BenchmarkRecoveryRange(b *testing.B) {
	epoch := time.Unix(1700000000, 0)
	for _, form := range []string{"ordered", "sorted", "mixed"} {
		rows := make([]Row, 100000)
		for i := range rows {
			n := i
			if form == "sorted" {
				n = len(rows) - 1 - i
			}
			line := "dense ordinary"
			if n%10000 == 0 {
				line += " raretoken"
			}
			rows[i] = Row{epoch.Add(time.Duration(n) * time.Second), line}
		}
		builder := NewRecoveryBuilder()
		builder.Add(rows)
		var s Store
		builder.Install(&s)
		if form == "mixed" {
			s.Publish([]Row{{epoch.Add(500 * time.Second), "dense ordinary"}})
		}
		count := len(rows)
		cursorOrdinal := 80000
		if form == "sorted" {
			cursorOrdinal = len(rows) - 1 - 80000
		}
		after := Position{rows[cursorOrdinal].TS, cursorOrdinal}
		for _, tc := range []struct {
			name, literal string
			snapshot      *int
			after         *Position
		}{
			{"dense", "dense", nil, nil}, {"rare", "raretoken", nil, nil}, {"absent", "absenttoken", nil, nil}, {"late", "raretoken", &count, &after},
		} {
			b.Run(form+"/"+tc.name, func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					s.QueryPage(epoch, epoch.Add(48*time.Hour), tc.literal, 100, tc.snapshot, tc.after)
				}
			})
		}
	}
}
