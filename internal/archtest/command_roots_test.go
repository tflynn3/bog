package archtest

import (
	"fmt"
	"slices"
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
	for _, path := range []string{goClient, queryModel} {
		if reached[path] {
			problems = append(problems, fmt.Sprintf("forbidden server command dependency: %s reaches %s", server, path))
		}
	}
	return problems
}

func registeredReachability(rules map[string]rule, root string) map[string]bool {
	packages := make(map[string]listedPackage, len(rules))
	for path, entry := range rules {
		packages[path] = listedPackage{Imports: entry.imports}
	}
	return reachableImports(packages, root)
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
		helper     = "bog/internal/commandhelper"
		store      = "bog/internal/store"
		release    = "bog/verify/release"
	)
	tests := []struct {
		name           string
		retained       bool
		clientOrOracle string
		throughHelper  bool
		want           []string
	}{
		{name: "operator uses staged backup and shares row types with the server"},
		{name: "server and operator share retained read-side libraries", retained: true},
		{
			name:           "direct client import",
			clientOrOracle: goClient,
			want:           []string{"forbidden server command dependency: bog/cmd/bog reaches bog/internal/client"},
		},
		{
			name:           "direct oracle import",
			clientOrOracle: queryModel,
			want:           []string{"forbidden server command dependency: bog/cmd/bog reaches bog/verify/query-model"},
		},
		{
			name:           "client through a registered helper",
			retained:       true,
			clientOrOracle: goClient,
			throughHelper:  true,
			want:           []string{"forbidden server command dependency: bog/cmd/bog reaches bog/internal/client"},
		},
		{
			name:           "oracle through a registered helper",
			retained:       true,
			clientOrOracle: queryModel,
			throughHelper:  true,
			want:           []string{"forbidden server command dependency: bog/cmd/bog reaches bog/verify/query-model"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			rules := map[string]rule{
				server:          {serverCommand, []string{store}},
				operatorCommand: {verifier, []string{backup, goClient, store}},
				backup:          {staged, []string{store}},
				history:         {staged, []string{checkpoint, store}},
				checkpoint:      {staged, []string{store}},
				store:           {runtimeLibrary, nil},
				goClient:        {standalone, []string{store}},
				queryModel:      {verifierModel, nil},
				release:         {verifier, []string{goClient, queryModel}},
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
			if target := tt.clientOrOracle; target != "" {
				// Update the destination's role so the fixture satisfies the
				// existing reachability check even with the forbidden edge.
				entry := rules[target]
				entry.role = runtimeLibrary
				rules[target] = entry
				if tt.throughHelper {
					rules[helper] = rule{runtimeLibrary, []string{target}}
					target = helper
				}
				command.imports = append(command.imports, target)
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
