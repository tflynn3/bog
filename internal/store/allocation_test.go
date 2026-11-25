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
