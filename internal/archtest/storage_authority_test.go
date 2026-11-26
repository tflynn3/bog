package archtest

import (
	"fmt"
	"slices"
	"testing"
)

// Validate ownership even for dependencies declared in the registry.
func storageAuthorityProblems(packages map[string]listedPackage) []string {
	const wal = "bog/internal/wal"

	var problems []string
	for _, from := range []string{"bog/internal/backup", "bog/internal/checkpoint", "bog/internal/history"} {
		if reachableImports(packages, from)[wal] {
			problems = append(problems, fmt.Sprintf("forbidden storage authority dependency: %s reaches %s", from, wal))
		}
	}
	return problems
}

func TestStorageAuthorityBoundaryFixtures(t *testing.T) {
	t.Parallel()
	const (
		backup     = "bog/internal/backup"
		checkpoint = "bog/internal/checkpoint"
		external   = "example.com/authority-bridge"
		history    = "bog/internal/history"
		httpapi    = "bog/internal/httpapi"
		relay      = "bog/internal/relay"
		store      = "bog/internal/store"
		wal        = "bog/internal/wal"
	)
	tests := []struct {
		name          string
		from, through string
		want          []string
	}{
		{name: "command-local composition"},
		{
			name: "backup imports the owner directly",
			from: backup, through: wal,
			want: []string{"forbidden storage authority dependency: " + backup + " reaches " + wal},
		},
		{
			name: "backup reaches the owner through a relay",
			from: backup, through: relay,
			want: []string{"forbidden storage authority dependency: " + backup + " reaches " + wal},
		},
		{
			name: "checkpoint bypass also exposes the owner to history",
			from: checkpoint, through: relay,
			want: []string{
				"forbidden storage authority dependency: " + checkpoint + " reaches " + wal,
				"forbidden storage authority dependency: " + history + " reaches " + wal,
			},
		},
		{
			name: "history reaches the owner through a relay",
			from: history, through: relay,
			want: []string{"forbidden storage authority dependency: " + history + " reaches " + wal},
		},
		{
			name: "backup reaches the owner through an external package",
			from: backup, through: external,
			want: []string{"forbidden storage authority dependency: " + backup + " reaches " + wal},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			rules := map[string]rule{
				server:     {serverCommand, []string{backup, checkpoint, history, httpapi, wal}},
				backup:     {runtimeLibrary, []string{store}},
				checkpoint: {runtimeLibrary, []string{store}},
				history:    {runtimeLibrary, []string{checkpoint, store}},
				httpapi:    {runtimeLibrary, []string{store, wal}},
				store:      {runtimeLibrary, nil},
				wal:        {runtimeLibrary, []string{store}},
			}
			if tt.through == relay {
				rules[relay] = rule{runtimeLibrary, []string{wal}}
			}
			if tt.from != "" && tt.through != external {
				entry := rules[tt.from]
				entry.imports = append(entry.imports, tt.through)
				rules[tt.from] = entry
			}
			packages := make(map[string]listedPackage, len(rules))
			for path, entry := range rules {
				name := "library"
				if path == server {
					name = "main"
				}
				packages[path] = productionPackage(name, entry.imports...)
			}
			if tt.through == external {
				packages[tt.from] = productionPackage("backup", store, external)
				packages[external] = listedPackage{Imports: []string{wal}}
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
