package archtest

import (
	"fmt"
	"maps"
	"slices"
	"strings"
	"testing"
)

func TestServerCommandBoundary(t *testing.T) {
	t.Parallel()
	for _, problem := range serverCommandBoundaryProblems(registry) {
		t.Error(problem)
	}
}

// TestLinuxArchitecture checks exact imports and cycles. Check forbidden
// destinations by path so assigning them a runtime role cannot permit them.
func serverCommandBoundaryProblems(rules map[string]rule) []string {
	if entry, ok := rules[server]; !ok || entry.role != serverCommand {
		return []string{fmt.Sprintf("%s must be registered as a %s", server, serverCommand)}
	}
	reached := registeredReachability(rules, server)

	var problems []string
	for _, path := range slices.Sorted(maps.Keys(reached)) {
		if path == goClient || path == "bog/verify" || strings.HasPrefix(path, "bog/verify/") ||
			path == "bog/bench" || strings.HasPrefix(path, "bog/bench/") {
			problems = append(problems, fmt.Sprintf("forbidden server command dependency: %s reaches %s", server, path))
		}
	}
	return problems
}

func TestServerCommandRegistration(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name  string
		rules map[string]rule
	}{
		{name: "missing"},
		{name: "reclassified as library", rules: map[string]rule{server: {role: runtimeLibrary}}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			want := []string{"bog/cmd/bog must be registered as a server command"}
			if got := serverCommandBoundaryProblems(tt.rules); !slices.Equal(got, want) {
				t.Errorf("serverCommandBoundaryProblems() = %q, want %q", got, want)
			}
		})
	}
}

func TestServerCommandBoundaryFixtures(t *testing.T) {
	t.Parallel()
	const (
		backup     = "bog/internal/backup"
		checkpoint = "bog/internal/checkpoint"
		history    = "bog/internal/history"
		httpapi    = "bog/internal/httpapi"
		readiness  = "bog/internal/readiness"
		wal        = "bog/internal/wal"
		helper     = "bog/internal/commandhelper"
		store      = "bog/internal/store"
		verifyLeaf = "bog/verify/report/format"
		benchLeaf  = "bog/bench/retained-history/metrics"
		forbidden  = "forbidden server command dependency: " + server + " reaches "
	)
	tests := []struct {
		name          string
		retained      bool
		dependencies  []string
		throughHelper bool
		want          []string
	}{
		{name: "operator uses staged backup and shares row types with the server"},
		{name: "composed storage shares row types with the operator and evidence producers", retained: true},
		{
			name:         "sibling package names are outside the evidence trees",
			dependencies: []string{"bog/verifyextra/format", "bog/benchmark/metrics"},
		},
		{
			name:         "direct client import",
			dependencies: []string{goClient},
			want:         []string{forbidden + goClient},
		},
		{
			name:         "direct oracle import",
			dependencies: []string{queryModel},
			want:         []string{forbidden + queryModel},
		},
		{
			name:          "client through a registered helper",
			retained:      true,
			dependencies:  []string{goClient},
			throughHelper: true,
			want:          []string{forbidden + goClient},
		},
		{
			name:          "oracle through a registered helper",
			retained:      true,
			dependencies:  []string{queryModel},
			throughHelper: true,
			want:          []string{forbidden + queryModel},
		},
		{
			name:         "direct evidence imports",
			retained:     true,
			dependencies: []string{verifyLeaf, benchLeaf},
			want:         []string{forbidden + benchLeaf, forbidden + verifyLeaf},
		},
		{
			name:          "evidence imports through a registered helper",
			retained:      true,
			dependencies:  []string{verifyLeaf, benchLeaf},
			throughHelper: true,
			want:          []string{forbidden + benchLeaf, forbidden + verifyLeaf},
		},
		{
			name:         "packages at the evidence tree roots",
			dependencies: []string{"bog/verify", "bog/bench"},
			want:         []string{forbidden + "bog/bench", forbidden + "bog/verify"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			rules := map[string]rule{
				server:          {serverCommand, []string{httpapi, readiness, store, wal}},
				operatorCommand: {verifier, []string{backup, goClient, store}},
				backup:          {staged, []string{store}},
				history:         {staged, []string{checkpoint, store}},
				checkpoint:      {staged, []string{store}},
				httpapi:         {runtimeLibrary, []string{store, wal}},
				readiness:       {runtimeLibrary, []string{store, wal}},
				store:           {runtimeLibrary, nil},
				wal:             {runtimeLibrary, []string{store}},
				goClient:        {standalone, []string{store}},
				queryModel:      {verifierModel, nil},
				releaseVerifier: {verifier, []string{goClient, queryModel, verifyLeaf}},
				retainedBench:   {verifier, []string{benchLeaf}},
				verifyLeaf:      {verifierModel, []string{store}},
				benchLeaf:       {verifierModel, []string{store}},
			}
			command := rules[server]
			if tt.retained {
				command.imports = append(command.imports, backup, history)
				for _, path := range []string{backup, history, checkpoint} {
					entry := rules[path]
					entry.role = runtimeLibrary
					rules[path] = entry
				}
			}
			for _, target := range tt.dependencies {
				// Update the destination's role so the fixture satisfies the
				// existing reachability check even with the forbidden edge.
				entry := rules[target]
				entry.role = runtimeLibrary
				rules[target] = entry
			}
			if tt.throughHelper {
				rules[helper] = rule{runtimeLibrary, tt.dependencies}
				command.imports = append(command.imports, helper)
			} else {
				command.imports = append(command.imports, tt.dependencies...)
			}
			rules[server] = command

			requireValidCommandFixture(t, rules)
			if got := operatorBoundaryProblems(rules); len(got) != 0 {
				t.Errorf("shared dependencies violate the operator boundary: %v", got)
			}
			if got := serverCommandBoundaryProblems(rules); !slices.Equal(got, tt.want) {
				t.Errorf("serverCommandBoundaryProblems() = %q, want %q", got, tt.want)
			}
		})
	}
}

func requireValidCommandFixture(t *testing.T, rules map[string]rule) {
	t.Helper()
	packages := make(map[string]listedPackage, len(rules))
	for path, entry := range rules {
		name := "library"
		if entry.role == serverCommand || entry.role == verifier {
			name = "main"
		}
		packages[path] = productionPackage(name, entry.imports...)
	}
	if problems := architectureProblems(packages, rules); len(problems) != 0 {
		t.Fatalf("fixture violates the existing architecture rules: %v", problems)
	}
}
