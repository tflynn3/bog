package archtest

import (
	"fmt"
	"slices"
	"testing"
)

// Follow the measured production graph so even a dependency outside the
// root-module registry cannot bring retained storage into the HTTP adapter.
func pagingBoundaryProblems(packages map[string]listedPackage) []string {
	const httpapi = "bog/internal/httpapi"
	reached := reachableImports(packages, httpapi)

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
		external   = "bog/bridge"
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
		bypassVia  string
		want       []string
	}{
		{name: "command injects its own page source"},
		{name: "HTTP reaches retained storage through a relay", bypassFrom: httpapi, bypassVia: relay, want: wantBypass},
		{name: "HTTP reaches retained storage through WAL", bypassFrom: wal, bypassVia: relay, want: wantBypass},
		{name: "HTTP reaches retained storage outside the registry", bypassFrom: httpapi, bypassVia: external, want: wantBypass},
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
			if tt.bypassVia == relay {
				rules[relay] = rule{runtimeLibrary, []string{backup, history}}
				entry := rules[tt.bypassFrom]
				entry.imports = append(entry.imports, relay)
				rules[tt.bypassFrom] = entry
			}
			packages := make(map[string]listedPackage, len(rules))
			for path, entry := range rules {
				name := "library"
				if path == server {
					name = "main"
				}
				packages[path] = productionPackage(name, entry.imports...)
			}
			if tt.bypassVia == external {
				pkg := packages[tt.bypassFrom]
				pkg.Imports = append(pkg.Imports, external)
				packages[tt.bypassFrom] = pkg
				packages[external] = listedPackage{
					GoFiles: []string{"bridge.go"},
					Imports: []string{backup, history},
				}
			}
			if got := architectureProblems(packages, rules); !slices.Equal(got, tt.want) {
				t.Errorf("architectureProblems() = %q, want %q", got, tt.want)
			}
		})
	}
}
