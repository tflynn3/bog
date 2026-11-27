package archtest

import (
	"fmt"
	"slices"
	"testing"
)

func TestStorageAuthorityBoundary(t *testing.T) {
	t.Parallel()
	for _, problem := range storageAuthorityProblems(registry) {
		t.Error(problem)
	}
}

// The Linux architecture test checks the registry against the production graph.
// Enforce ownership independently so registering a bypass cannot permit it.
func storageAuthorityProblems(rules map[string]rule) []string {
	const wal = "bog/internal/wal"
	packages := make(map[string]listedPackage, len(rules))
	for path, entry := range rules {
		packages[path] = listedPackage{Imports: entry.imports}
	}

	var problems []string
	for _, from := range []string{
		"bog/internal/backup",
		"bog/internal/checkpoint",
		"bog/internal/history",
		"bog/internal/idempotency",
	} {
		if reachableImports(packages, from)[wal] {
			problems = append(problems, fmt.Sprintf("forbidden storage authority dependency: %s reaches %s", from, wal))
		}
	}
	return problems
}

func TestStorageAuthorityBoundaryFixtures(t *testing.T) {
	t.Parallel()
	const (
		backup      = "bog/internal/backup"
		checkpoint  = "bog/internal/checkpoint"
		history     = "bog/internal/history"
		httpapi     = "bog/internal/httpapi"
		idempotency = "bog/internal/idempotency"
		relay       = "bog/internal/relay"
		store       = "bog/internal/store"
		wal         = "bog/internal/wal"
	)
	tests := []struct {
		name          string
		from, through string
		want          []string
	}{
		{name: "command-local composition supplies the keyed ingest backend"},
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
			name: "idempotency imports the owner directly",
			from: idempotency, through: wal,
			want: []string{"forbidden storage authority dependency: " + idempotency + " reaches " + wal},
		},
		{
			name: "idempotency reaches the owner through a registered relay",
			from: idempotency, through: relay,
			want: []string{"forbidden storage authority dependency: " + idempotency + " reaches " + wal},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			// The command can adapt its WAL owner to idempotency.Backend and
			// inject it into the coordinator without a coordinator-to-WAL import.
			rules := map[string]rule{
				server:      {serverCommand, []string{backup, checkpoint, history, httpapi, idempotency, wal}},
				backup:      {runtimeLibrary, []string{store}},
				checkpoint:  {runtimeLibrary, []string{store}},
				history:     {runtimeLibrary, []string{checkpoint, store}},
				httpapi:     {runtimeLibrary, []string{store, wal}},
				idempotency: {runtimeLibrary, nil},
				store:       {runtimeLibrary, nil},
				wal:         {runtimeLibrary, []string{store}},
			}
			if tt.through == relay {
				rules[relay] = rule{runtimeLibrary, []string{wal}}
			}
			if tt.from != "" {
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
			if problems := architectureProblems(packages, rules); len(problems) != 0 {
				t.Fatalf("fixture violates the existing architecture rules: %v", problems)
			}
			if got := storageAuthorityProblems(rules); !slices.Equal(got, tt.want) {
				t.Errorf("storageAuthorityProblems() = %q, want %q", got, tt.want)
			}
		})
	}
}
