package archtest

import (
	"fmt"
	"slices"
	"testing"
)

const operatorCommand = "bog/cmd/bog-verify"

func TestOperatorVerificationBoundary(t *testing.T) {
	t.Parallel()
	if _, registered := registry[operatorCommand]; !registered {
		t.Skip("operator verification command registration is pending in #67")
	}
	for _, problem := range operatorBoundaryProblems(registry) {
		t.Error(problem)
	}
}

// TestLinuxArchitecture checks that the registry matches the Linux production
// imports. Enforce the operator boundary even when every edge is registered.
func operatorBoundaryProblems(rules map[string]rule) []string {
	packages := make(map[string]listedPackage, len(rules))
	for path, entry := range rules {
		packages[path] = listedPackage{Imports: entry.imports}
	}
	reached := reachableImports(packages, operatorCommand)

	var problems []string
	for _, path := range []string{"bog/internal/httpapi", "bog/internal/readiness", "bog/internal/wal"} {
		if reached[path] {
			problems = append(problems, fmt.Sprintf("forbidden operator verification dependency: %s reaches %s", operatorCommand, path))
		}
	}
	return problems
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
		forbidden       []string
	}{
		{
			name:            "backup and client share storage types with server",
			operatorImports: []string{backup, goClient},
		},
		{
			name:            "read-only verification through a helper",
			operatorImports: []string{relay},
			relayImports:    []string{backup, goClient},
		},
		{
			name:            "direct WAL owner import",
			operatorImports: []string{wal},
			forbidden:       []string{wal},
		},
		{
			name:            "direct server HTTP import",
			operatorImports: []string{httpapi},
			forbidden:       []string{httpapi, wal},
		},
		{
			name:            "direct readiness import",
			operatorImports: []string{readiness},
			forbidden:       []string{readiness, wal},
		},
		{
			name:            "WAL owner through a registered helper",
			operatorImports: []string{relay},
			relayImports:    []string{wal},
			forbidden:       []string{wal},
		},
		{
			name:            "server HTTP through a registered helper",
			operatorImports: []string{relay},
			relayImports:    []string{httpapi},
			forbidden:       []string{httpapi, wal},
		},
		{
			name:            "readiness through a registered helper",
			operatorImports: []string{relay},
			relayImports:    []string{readiness},
			forbidden:       []string{readiness, wal},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			rules := map[string]rule{
				server:          {serverCommand, []string{httpapi, readiness, wal}},
				operatorCommand: {verifier, tt.operatorImports},
				backup:          {staged, []string{store}},
				goClient:        {standalone, []string{store}},
				httpapi:         {runtimeLibrary, []string{store, wal}},
				readiness:       {runtimeLibrary, []string{store, wal}},
				store:           {runtimeLibrary, nil},
				wal:             {runtimeLibrary, []string{store}},
			}
			if len(tt.relayImports) > 0 {
				rules[relay] = rule{standalone, tt.relayImports}
			}
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
			var want []string
			for _, path := range tt.forbidden {
				want = append(want, "forbidden operator verification dependency: "+operatorCommand+" reaches "+path)
			}
			if got := operatorBoundaryProblems(rules); !slices.Equal(got, want) {
				t.Errorf("operatorBoundaryProblems() = %q, want %q", got, want)
			}
		})
	}
}
