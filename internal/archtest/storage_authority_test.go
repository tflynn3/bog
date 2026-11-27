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

	retained := []string{
		"bog/internal/backup",
		"bog/internal/checkpoint",
		"bog/internal/history",
	}
	var problems []string
	for _, from := range append(retained, "bog/internal/idempotency") {
		if reachableImports(packages, from)[wal] {
			problems = append(problems, fmt.Sprintf("forbidden storage authority dependency: %s reaches %s", from, wal))
		}
	}
	walReachable := reachableImports(packages, wal)
	for _, to := range retained {
		if walReachable[to] {
			problems = append(problems, fmt.Sprintf("forbidden storage authority dependency: %s reaches %s", wal, to))
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
		readiness   = "bog/internal/readiness"
		relay       = "bog/internal/relay"
		store       = "bog/internal/store"
		wal         = "bog/internal/wal"
		forbidden   = "forbidden storage authority dependency: "
	)
	tests := []struct {
		name     string
		from, to string
		viaRelay bool
		want     []string
	}{
		{name: "command-local composition supplies retained storage and the keyed ingest backend"},
		{
			name: "WAL imports backup directly",
			from: wal, to: backup,
			want: []string{forbidden + wal + " reaches " + backup},
		},
		{
			name: "WAL reaches backup through a registered relay",
			from: wal, to: backup, viaRelay: true,
			want: []string{forbidden + wal + " reaches " + backup},
		},
		{
			name: "WAL imports checkpoint directly",
			from: wal, to: checkpoint,
			want: []string{forbidden + wal + " reaches " + checkpoint},
		},
		{
			name: "WAL reaches checkpoint through a registered relay",
			from: wal, to: checkpoint, viaRelay: true,
			want: []string{forbidden + wal + " reaches " + checkpoint},
		},
		{
			name: "WAL imports history directly and reaches checkpoint",
			from: wal, to: history,
			want: []string{forbidden + wal + " reaches " + checkpoint, forbidden + wal + " reaches " + history},
		},
		{
			name: "WAL reaches history and checkpoint through a registered relay",
			from: wal, to: history, viaRelay: true,
			want: []string{forbidden + wal + " reaches " + checkpoint, forbidden + wal + " reaches " + history},
		},
		{
			name: "backup imports the owner directly",
			from: backup, to: wal,
			want: []string{forbidden + backup + " reaches " + wal},
		},
		{
			name: "backup reaches the owner through a relay",
			from: backup, to: wal, viaRelay: true,
			want: []string{forbidden + backup + " reaches " + wal},
		},
		{
			name: "checkpoint bypass also exposes the owner to history",
			from: checkpoint, to: wal, viaRelay: true,
			want: []string{
				forbidden + checkpoint + " reaches " + wal,
				forbidden + history + " reaches " + wal,
			},
		},
		{
			name: "history reaches the owner through a relay",
			from: history, to: wal, viaRelay: true,
			want: []string{forbidden + history + " reaches " + wal},
		},
		{
			name: "idempotency imports the owner directly",
			from: idempotency, to: wal,
			want: []string{forbidden + idempotency + " reaches " + wal},
		},
		{
			name: "idempotency reaches the owner through a registered relay",
			from: idempotency, to: wal, viaRelay: true,
			want: []string{forbidden + idempotency + " reaches " + wal},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			// The command adapts the WAL owner for retained storage and keyed
			// ingest. HTTP and readiness get their inputs from the command
			// without importing WAL, so their rules cannot detect its bypasses.
			rules := map[string]rule{
				server:      {serverCommand, []string{backup, checkpoint, history, httpapi, idempotency, readiness, wal}},
				backup:      {runtimeLibrary, []string{store}},
				checkpoint:  {runtimeLibrary, []string{store}},
				history:     {runtimeLibrary, []string{checkpoint, store}},
				httpapi:     {runtimeLibrary, []string{store}},
				idempotency: {runtimeLibrary, nil},
				readiness:   {runtimeLibrary, []string{store}},
				store:       {runtimeLibrary, nil},
				wal:         {runtimeLibrary, []string{store}},
			}
			if tt.from != "" {
				target := tt.to
				if tt.viaRelay {
					rules[relay] = rule{runtimeLibrary, []string{tt.to}}
					target = relay
				}
				entry := rules[tt.from]
				entry.imports = append(entry.imports, target)
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
			if problems := pagingBoundaryProblems(rules); len(problems) != 0 {
				t.Fatalf("fixture violates the HTTP paging boundary: %v", problems)
			}
			if got := storageAuthorityProblems(rules); !slices.Equal(got, tt.want) {
				t.Errorf("storageAuthorityProblems() = %q, want %q", got, tt.want)
			}
		})
	}
}
