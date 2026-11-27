package client_test

import (
	"testing"
	"time"

	"bog/internal/client"
)

func TestParseQueryTime(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, input string
		want        time.Time
	}{
		{"UTC", "2025-01-01T00:00:00Z", time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)},
		{"positive offset with nanoseconds", "2025-01-01T00:15:00.123456789+01:30", time.Date(2024, 12, 31, 22, 45, 0, 123456789, time.UTC)},
		{"negative offset with fractional seconds", "2024-12-31T20:00:00.1-04:00", time.Date(2025, 1, 1, 0, 0, 0, 100000000, time.UTC)},
		{"minimum UTC year", "0001-01-01T01:00:00+01:00", time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC)},
		{"maximum UTC year", "9999-12-31T22:59:59.999999999-01:00", time.Date(9999, 12, 31, 23, 59, 59, 999999999, time.UTC)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := client.ParseQueryTime(tc.input)
			if err != nil {
				t.Fatal(err)
			}
			if !got.Equal(tc.want) || got.Location() != time.UTC {
				t.Errorf("ParseQueryTime(%q) = %v in %v, want %v in UTC", tc.input, got, got.Location(), tc.want)
			}
		})
	}
}

func TestParseQueryTimeRejectsInvalidInput(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct{ name, input string }{
		{"empty", ""},
		{"date only", "2025-01-01"},
		{"missing zone", "2025-01-01T00:00:00"},
		{"leading whitespace", " 2025-01-01T00:00:00Z"},
		{"trailing newline", "2025-01-01T00:00:00Z\n"},
		{"lowercase time separator", "2025-01-01t00:00:00Z"},
		{"lowercase zone", "2025-01-01T00:00:00z"},
		{"one-digit hour", "2025-01-01T0:00:00Z"},
		{"comma decimal separator", "2025-01-01T00:00:00,1Z"},
		{"excess precision", "2025-01-01T00:00:00.1234567890Z"},
		{"invalid calendar date", "2025-02-29T00:00:00Z"},
		{"leap second", "2025-01-01T00:00:60Z"},
		{"offset hours overflow", "2025-01-01T00:00:00+24:00"},
		{"offset minutes overflow", "2025-01-01T00:00:00-00:60"},
		{"before supported UTC range", "0001-01-01T00:00:00+00:01"},
		{"after supported UTC range", "9999-12-31T23:59:59-00:01"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got, err := client.ParseQueryTime(tc.input); err == nil || !got.IsZero() {
				t.Errorf("ParseQueryTime(%q) = (%v, %v), want zero time and an error", tc.input, got, err)
			}
		})
	}
}
