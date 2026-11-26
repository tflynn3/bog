package store

import (
	"testing"
	"time"
)

func BenchmarkPublishSmallBatches(b *testing.B) {
	batch := make([]Row, 16)
	for i := range batch {
		batch[i] = Row{TS: time.Unix(int64(i), 0), Line: "ordinary log message"}
	}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		var s Store
		for j := 0; j < 2048; j++ {
			s.Publish(batch)
		}
	}
}

func BenchmarkPublishOrderedAndShuffled(b *testing.B) {
	for _, shuffled := range []bool{false, true} {
		name := "ordered"
		if shuffled {
			name = "shuffled"
		}
		b.Run(name, func(b *testing.B) {
			batches := make([][]Row, 256)
			for j := range batches {
				batches[j] = make([]Row, 16)
				for i := range batches[j] {
					value := j*16 + i
					if shuffled {
						value = (value * 4051) % 4096
					}
					batches[j][i] = Row{TS: time.Unix(int64(value), 0), Line: "ordinary log message"}
				}
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				var s Store
				for _, batch := range batches {
					s.Publish(batch)
				}
			}
		})
	}
}
