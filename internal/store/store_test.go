package store

import (
	"sync"
	"testing"
	"time"
)

func TestDecodeRejectsMalformedPrefix(t *testing.T) {
	for _, s := range []string{`{"ts":"2025-01-01T00:00:00Z","line":"ok"}` + "\n" + `{"ts":"bad","line":"x"}`, `{"ts":"2025-01-01T00:00:00Z","ts":"2025-01-01T00:00:00Z","line":"x"}`, `{"ts":"2025-01-01T00:00:00Z","line":"x"}junk`, "\xff"} {
		if _, err := Decode([]byte(s)); err == nil {
			t.Fatalf("accepted %q", s)
		}
	}
}

func TestReservedQueryEnd(t *testing.T) {
	for _, ts := range []string{"9999-12-31T23:59:59.999999999Z", "9999-12-31T22:59:59.999999999-01:00"} {
		if _, err := ParseTime(ts); err != nil {
			t.Fatalf("query bound %s: %v", ts, err)
		}
		if _, err := Decode([]byte(`{"ts":"` + ts + `","line":"x"}`)); err == nil {
			t.Fatalf("accepted reserved timestamp %s", ts)
		}
	}
	if _, err := Decode([]byte(`{"ts":"9999-12-31T23:59:59.999999998Z","line":"x"}`)); err != nil {
		t.Fatal(err)
	}
}
func TestQuerySnapshotAndTies(t *testing.T) {
	var s Store
	now := time.Now().UTC()
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			s.Publish([]Row{{now, "a"}, {now, "b"}})
		}
	}()
	for i := 0; i < 100; i++ {
		rows := s.Query(now, now.Add(time.Second), "", 1000)
		if len(rows)%2 != 0 {
			t.Fatal("partial batch")
		}
		for j := 0; j < len(rows); j += 2 {
			if rows[j].Line != "a" || rows[j+1].Line != "b" {
				t.Fatal("tie order")
			}
		}
	}
	wg.Wait()
}
