package archtest

import (
	"fmt"
	"maps"
	"slices"
	"strings"
	"testing"
)

func TestStorageAuthorityBoundary(t *testing.T) {
	t.Parallel()
	for _, problem := range storageAuthorityProblems(registry) {
		t.Error(problem)
	}
}

// Check ownership against both the registry and the measured Linux graphs.
// Registering an import cannot permit a bypass.
func storageAuthorityProblems(rules map[string]rule) []string {
	const (
		backup      = "bog/internal/backup"
		checkpoint  = "bog/internal/checkpoint"
		history     = "bog/internal/history"
		idempotency = "bog/internal/idempotency"
		wal         = "bog/internal/wal"
	)
	packages := ruleImportGraph(rules)

	retained := []string{backup, checkpoint, history}
	walPeers := append(retained, idempotency)
	var problems []string
	for _, from := range walPeers {
		reached := reachableImports(packages, from)
		if reached[wal] {
			problems = append(problems, fmt.Sprintf("forbidden storage authority dependency: %s reaches %s", from, wal))
		}
		// Backup validates its own recovery view. History may use checkpoint
		// validation, and both consume the view independently of backup.
		switch from {
		case backup:
			for _, to := range []string{checkpoint, history} {
				if reached[to] {
					problems = append(problems, fmt.Sprintf("forbidden recovery view dependency: %s reaches %s", from, to))
				}
			}
		case checkpoint, history:
			if reached[backup] {
				problems = append(problems, fmt.Sprintf("forbidden recovery view dependency: %s reaches %s", from, backup))
			}
		}
	}
	walReachable := reachableImports(packages, wal)
	for _, to := range walPeers {
		if walReachable[to] {
			problems = append(problems, fmt.Sprintf("forbidden storage authority dependency: %s reaches %s", wal, to))
		}
	}

	for _, from := range slices.Sorted(maps.Keys(rules)) {
		if from == server || from == wal || rules[from].role == testOnly {
			continue
		}
		reached := reachableImports(packages, from)
		if !reached[wal] {
			continue
		}
		if from != idempotency && reached[idempotency] {
			problems = append(problems, fmt.Sprintf("forbidden keyed ingest composition: %s reaches %s and %s. Compose in %s",
				from, wal, idempotency, server))
		}
		// Retained roots that reach WAL already have a direction diagnostic.
		if slices.Contains(retained, from) {
			continue
		}
		var dependencies []string
		for _, path := range retained {
			if reached[path] {
				dependencies = append(dependencies, path)
			}
		}
		if len(dependencies) > 0 {
			problems = append(problems, fmt.Sprintf("forbidden retained storage composition: %s reaches %s and %s. Compose in %s",
				from, wal, strings.Join(dependencies, ", "), server))
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
		{
			name: "WAL imports idempotency directly",
			from: wal, to: idempotency,
			want: []string{forbidden + wal + " reaches " + idempotency},
		},
		{
			name: "WAL reaches idempotency through a registered relay",
			from: wal, to: idempotency, viaRelay: true,
			want: []string{forbidden + wal + " reaches " + idempotency},
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
			requireValidCommandFixture(t, rules)
			if problems := pagingBoundaryProblems(rules); len(problems) != 0 {
				t.Fatalf("fixture violates the HTTP paging boundary: %v", problems)
			}
			if got := storageAuthorityProblems(rules); !slices.Equal(got, tt.want) {
				t.Errorf("storageAuthorityProblems() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestStorageCompositionFixtures(t *testing.T) {
	t.Parallel()
	const (
		adapter     = "bog/internal/retained"
		backup      = "bog/internal/backup"
		checkpoint  = "bog/internal/checkpoint"
		history     = "bog/internal/history"
		httpapi     = "bog/internal/httpapi"
		idempotency = "bog/internal/idempotency"
		readiness   = "bog/internal/readiness"
		relay       = "bog/internal/relay"
		store       = "bog/internal/store"
		wal         = "bog/internal/wal"
		forbidden   = "forbidden retained storage composition: "
		keyed       = "forbidden keyed ingest composition: "
		resolution  = ". Compose in " + server
	)
	tests := []struct {
		name           string
		adapterImports []string
		relayImports   []string
		want           []string
	}{
		{name: "command injects keyed ingest and retained paging while readiness observes WAL and the operator reads backup"},
		{
			name:           "command composes WAL with a retained-only helper",
			adapterImports: []string{history, backup},
		},
		{
			name:           "shared adapter combines WAL and backup directly",
			adapterImports: []string{wal, backup},
			want:           []string{forbidden + adapter + " reaches " + wal + " and " + backup + resolution},
		},
		{
			name:           "shared adapter reaches checkpoint through a registered relay",
			adapterImports: []string{wal, relay},
			relayImports:   []string{checkpoint},
			want:           []string{forbidden + adapter + " reaches " + wal + " and " + checkpoint + resolution},
		},
		{
			name:           "shared adapter reaches WAL through a registered relay",
			adapterImports: []string{history, relay},
			relayImports:   []string{wal},
			want:           []string{forbidden + adapter + " reaches " + wal + " and " + checkpoint + ", " + history + resolution},
		},
		{
			name:           "registered relay and its shared importer both reach WAL and retained storage",
			adapterImports: []string{relay},
			relayImports:   []string{wal, history},
			want: []string{
				forbidden + relay + " reaches " + wal + " and " + checkpoint + ", " + history + resolution,
				forbidden + adapter + " reaches " + wal + " and " + checkpoint + ", " + history + resolution,
			},
		},
		{
			name:           "shared adapter combines WAL and idempotency directly",
			adapterImports: []string{wal, idempotency},
			want:           []string{keyed + adapter + " reaches " + wal + " and " + idempotency + resolution},
		},
		{
			name:           "shared adapter reaches idempotency through a registered relay",
			adapterImports: []string{wal, relay},
			relayImports:   []string{idempotency},
			want:           []string{keyed + adapter + " reaches " + wal + " and " + idempotency + resolution},
		},
		{
			name:           "shared adapter reaches WAL through a registered relay for keyed ingest",
			adapterImports: []string{idempotency, relay},
			relayImports:   []string{wal},
			want:           []string{keyed + adapter + " reaches " + wal + " and " + idempotency + resolution},
		},
		{
			name:           "registered relay and its shared importer both reach WAL and idempotency",
			adapterImports: []string{relay},
			relayImports:   []string{wal, idempotency},
			want: []string{
				keyed + relay + " reaches " + wal + " and " + idempotency + resolution,
				keyed + adapter + " reaches " + wal + " and " + idempotency + resolution,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			rules := map[string]rule{
				server:          {serverCommand, []string{backup, history, httpapi, idempotency, readiness, wal}},
				operatorCommand: {verifier, []string{backup, goClient, store}},
				backup:          {runtimeLibrary, []string{store}},
				checkpoint:      {runtimeLibrary, []string{store}},
				history:         {runtimeLibrary, []string{checkpoint, store}},
				httpapi:         {runtimeLibrary, []string{store}},
				idempotency:     {runtimeLibrary, nil},
				readiness:       {runtimeLibrary, []string{wal}},
				store:           {runtimeLibrary, nil},
				wal:             {runtimeLibrary, []string{store}},
				goClient:        {standalone, []string{store}},
			}
			if len(tt.adapterImports) > 0 {
				command := rules[server]
				command.imports = append(command.imports, adapter)
				rules[server] = command
				rules[adapter] = rule{runtimeLibrary, tt.adapterImports}
			}
			if len(tt.relayImports) > 0 {
				rules[relay] = rule{runtimeLibrary, tt.relayImports}
			}
			requireValidCommandFixture(t, rules)
			if problems := pagingBoundaryProblems(rules); len(problems) > 0 {
				t.Fatalf("fixture violates the HTTP paging boundary: %v", problems)
			}
			if problems := operatorBoundaryProblems(rules); len(problems) > 0 {
				t.Fatalf("fixture violates the operator boundary: %v", problems)
			}
			if got := storageAuthorityProblems(rules); !slices.Equal(got, tt.want) {
				t.Errorf("storageAuthorityProblems() = %q, want %q", got, tt.want)
			}
		})
	}
}
