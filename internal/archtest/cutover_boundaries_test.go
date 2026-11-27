package archtest

import (
	"fmt"
	"slices"
	"testing"
)

func TestHTTPPagingBoundary(t *testing.T) {
	t.Parallel()
	for _, problem := range pagingBoundaryProblems(registry) {
		t.Error(problem)
	}
}

// TestLinuxArchitecture checks that the registry exactly matches the Linux
// production graph within the root module. Check the paging seam independently
// so registering every edge of a bypass still fails the suite.
func pagingBoundaryProblems(rules map[string]rule) []string {
	const httpapi = "bog/internal/httpapi"
	reached := registeredReachability(rules, httpapi)

	var problems []string
	for _, storage := range []string{"bog/internal/backup", "bog/internal/checkpoint", "bog/internal/history"} {
		if reached[storage] {
			problems = append(problems, fmt.Sprintf("HTTP must use the page source seam: %s reaches %s", httpapi, storage))
		}
	}
	return problems
}

func TestHTTPPagingBoundaryFixtures(t *testing.T) {
	t.Parallel()
	const (
		backup     = "bog/internal/backup"
		checkpoint = "bog/internal/checkpoint"
		history    = "bog/internal/history"
		httpapi    = "bog/internal/httpapi"
		relay      = "bog/internal/relay"
		store      = "bog/internal/store"
		wal        = "bog/internal/wal"
	)
	wantBypass := []string{
		"HTTP must use the page source seam: " + httpapi + " reaches " + backup,
		"HTTP must use the page source seam: " + httpapi + " reaches " + checkpoint,
		"HTTP must use the page source seam: " + httpapi + " reaches " + history,
	}
	tests := []struct {
		name       string
		bypassFrom string
		want       []string
	}{
		{name: "command injects its own page source"},
		{name: "HTTP reaches retained storage through a relay", bypassFrom: httpapi, want: wantBypass},
		{name: "HTTP reaches retained storage through WAL", bypassFrom: wal, want: wantBypass},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			rules := map[string]rule{
				server:     {serverCommand, []string{backup, history, httpapi, wal}},
				backup:     {runtimeLibrary, []string{store}},
				checkpoint: {runtimeLibrary, []string{store}},
				history:    {runtimeLibrary, []string{checkpoint, store}},
				httpapi:    {runtimeLibrary, []string{store, wal}},
				store:      {runtimeLibrary, nil},
				wal:        {runtimeLibrary, []string{store}},
			}
			if tt.bypassFrom != "" {
				rules[relay] = rule{runtimeLibrary, []string{backup, history}}
				entry := rules[tt.bypassFrom]
				entry.imports = append(entry.imports, relay)
				rules[tt.bypassFrom] = entry
			}
			requireValidCommandFixture(t, rules)
			if got := pagingBoundaryProblems(rules); !slices.Equal(got, tt.want) {
				t.Errorf("pagingBoundaryProblems() = %q, want %q", got, tt.want)
			}
		})
	}
}
