package archtest

import (
	"fmt"
	"slices"
	"testing"
)

// Use actual production imports so every path through the HTTP seam is checked,
// including paths with unregistered edges.
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
		adapter    = "bog/internal/retainedpages"
		backup     = "bog/internal/backup"
		checkpoint = "bog/internal/checkpoint"
		external   = "example.com/page-bridge"
		history    = "bog/internal/history"
		httpapi    = "bog/internal/httpapi"
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
		{name: "command injects adapter"},
		{name: "HTTP imports adapter", bypassFrom: httpapi, want: wantBypass},
		{name: "HTTP reaches adapter through WAL", bypassFrom: wal, want: wantBypass},
		{name: "HTTP reaches adapter through an external package", bypassFrom: external, want: wantBypass},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			rules := map[string]rule{
				server:     {serverCommand, []string{adapter, httpapi, wal}},
				adapter:    {runtimeLibrary, []string{backup, history}},
				backup:     {runtimeLibrary, []string{store}},
				checkpoint: {runtimeLibrary, []string{store}},
				history:    {runtimeLibrary, []string{checkpoint, store}},
				httpapi:    {runtimeLibrary, []string{store, wal}},
				store:      {runtimeLibrary, nil},
				wal:        {runtimeLibrary, []string{store}},
			}
			if tt.bypassFrom != "" && tt.bypassFrom != external {
				entry := rules[tt.bypassFrom]
				entry.imports = append(entry.imports, adapter)
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
			if tt.bypassFrom == external {
				packages[httpapi] = productionPackage("httpapi", store, wal, external)
				packages[external] = listedPackage{Imports: []string{adapter}}
			}
			if problems := architectureProblems(packages, rules); len(problems) != 0 {
				t.Fatalf("fixture violates the existing architecture rules: %v", problems)
			}
			if got := linuxGraphProblems(packages, rules); !slices.Equal(got, tt.want) {
				t.Errorf("linuxGraphProblems() = %q, want %q", got, tt.want)
			}
		})
	}
}
