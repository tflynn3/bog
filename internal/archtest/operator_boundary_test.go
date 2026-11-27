package archtest

import (
	"fmt"
	"slices"
	"testing"
)

const operatorCommand = "bog/cmd/bog-verify"

func TestOperatorVerificationBoundary(t *testing.T) {
	t.Parallel()
	for _, problem := range operatorBoundaryProblems(registry) {
		t.Error(problem)
	}
}

// TestLinuxArchitecture checks that the registry matches the Linux production
// imports. Enforce the operator boundary even when every edge is registered.
func operatorBoundaryProblems(rules map[string]rule) []string {
	entry := rules[operatorCommand]
	if entry.role != verifier {
		return []string{fmt.Sprintf("%s must be registered as a %s", operatorCommand, verifier)}
	}
	reached := registeredReachability(rules, operatorCommand)

	var problems []string
	const backup = "bog/internal/backup"
	if !slices.Contains(entry.imports, backup) {
		problems = append(problems, fmt.Sprintf("%s must directly import %s for offline verification", operatorCommand, backup))
	}
	for _, path := range []string{"bog/internal/httpapi", "bog/internal/readiness", "bog/internal/wal"} {
		if reached[path] {
			problems = append(problems, fmt.Sprintf("forbidden operator verification dependency: %s reaches %s", operatorCommand, path))
		}
	}
	return problems
}

func TestOperatorCommandRegistration(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name  string
		rules map[string]rule
	}{
		{name: "missing"},
		{name: "reclassified as library", rules: map[string]rule{operatorCommand: {role: standalone}}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			want := []string{"bog/cmd/bog-verify must be registered as a verifier command"}
			if got := operatorBoundaryProblems(tt.rules); !slices.Equal(got, want) {
				t.Errorf("operatorBoundaryProblems() = %q, want %q", got, want)
			}
		})
	}
}

func TestOperatorVerificationBoundaryFixtures(t *testing.T) {
	t.Parallel()
	const (
		backup    = "bog/internal/backup"
		httpapi   = "bog/internal/httpapi"
		readiness = "bog/internal/readiness"
		relay     = "bog/internal/verification"
		store     = "bog/internal/store"
		wal       = "bog/internal/wal"
	)
	tests := []struct {
		name            string
		operatorImports []string
		relayImports    []string
		missingBackup   bool
		forbidden       []string
	}{
		{
			name:            "backup and client share storage types with server",
			operatorImports: []string{backup, goClient},
		},
		{
			name:            "operator keeps offline backup with the client behind a helper",
			operatorImports: []string{backup, relay},
			relayImports:    []string{goClient},
		},
		{
			name:            "operator drops offline backup after server promotion",
			operatorImports: []string{goClient},
			missingBackup:   true,
		},
		{
			name:            "transitive backup use cannot replace the direct offline edge",
			operatorImports: []string{goClient, relay},
			relayImports:    []string{backup},
			missingBackup:   true,
		},
		{
			name:            "direct WAL owner import",
			operatorImports: []string{backup, wal},
			forbidden:       []string{wal},
		},
		{
			name:            "direct server HTTP import",
			operatorImports: []string{backup, httpapi},
			forbidden:       []string{httpapi, wal},
		},
		{
			name:            "direct readiness import",
			operatorImports: []string{backup, readiness},
			forbidden:       []string{readiness, wal},
		},
		{
			name:            "WAL owner through a registered helper",
			operatorImports: []string{backup, relay},
			relayImports:    []string{wal},
			forbidden:       []string{wal},
		},
		{
			name:            "server HTTP through a registered helper",
			operatorImports: []string{backup, relay},
			relayImports:    []string{httpapi},
			forbidden:       []string{httpapi, wal},
		},
		{
			name:            "readiness through a registered helper",
			operatorImports: []string{backup, relay},
			relayImports:    []string{readiness},
			forbidden:       []string{readiness, wal},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			rules := map[string]rule{
				server:          {serverCommand, []string{backup, httpapi, readiness, wal}},
				operatorCommand: {verifier, tt.operatorImports},
				backup:          {runtimeLibrary, []string{store}},
				goClient:        {standalone, []string{store}},
				httpapi:         {runtimeLibrary, []string{store, wal}},
				readiness:       {runtimeLibrary, []string{store, wal}},
				store:           {runtimeLibrary, nil},
				wal:             {runtimeLibrary, []string{store}},
			}
			if len(tt.relayImports) > 0 {
				rules[relay] = rule{standalone, tt.relayImports}
			}
			requireValidCommandFixture(t, rules)
			var want []string
			if tt.missingBackup {
				want = append(want, operatorCommand+" must directly import "+backup+" for offline verification")
			}
			for _, path := range tt.forbidden {
				want = append(want, "forbidden operator verification dependency: "+operatorCommand+" reaches "+path)
			}
			if got := operatorBoundaryProblems(rules); !slices.Equal(got, want) {
				t.Errorf("operatorBoundaryProblems() = %q, want %q", got, want)
			}
		})
	}
}
