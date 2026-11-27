package client

import (
	"time"

	"bog/internal/store"
)

// ParseQueryTime parses a query bound using Bog's strict RFC3339 format
// and returns it in UTC.
func ParseQueryTime(s string) (time.Time, error) {
	return store.ParseTime(s)
}
