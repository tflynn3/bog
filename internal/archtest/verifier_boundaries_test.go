package archtest

import (
	"fmt"
	"maps"
	"slices"
	"testing"
)

const (
	queryModel = "bog/verify/query-model"
	goClient   = "bog/internal/client"
)

func TestQueryOracleIndependence(t *testing.T) {
	t.Parallel()
	for _, problem := range queryOracleProblems(registry) {
		t.Error(problem)
	}
}

// TestLinuxArchitecture checks that the registry matches the root-module
// production graph. Enforce oracle independence even for registered paths.
func queryOracleProblems(rules map[string]rule) []string {
	packages := make(map[string]listedPackage, len(rules))
	for path, entry := range rules {
		packages[path] = listedPackage{Imports: entry.imports}
	}
	reached := reachableImports(packages, queryModel)

	var problems []string
	for _, path := range slices.Sorted(maps.Keys(reached)) {
		role := rules[path].role
		if path == goClient || role == serverCommand || role == runtimeLibrary || role == staged {
			problems = append(problems, fmt.Sprintf("forbidden query oracle dependency: %s reaches %s", queryModel, path))
		}
	}
	return problems
}

func TestQueryOracleIndependenceFixtures(t *testing.T) {
	t.Parallel()
	const (
		release = "bog/verify/release"
		helper  = "bog/verify/helper"
		httpapi = "bog/internal/httpapi"
		store   = "bog/internal/store"
		history = "bog/internal/history"
	)
	tests := []struct {
		name          string
		oracleImports []string
		helperImports []string
		want          []string
	}{
		{name: "release verifier imports the client and oracle", oracleImports: []string{helper}},
		{
			name:          "oracle imports client directly",
			oracleImports: []string{goClient},
			want: []string{
				"forbidden query oracle dependency: " + queryModel + " reaches " + goClient,
				"forbidden query oracle dependency: " + queryModel + " reaches " + store,
			},
		},
		{
			name:          "oracle reaches client through registered helper",
			oracleImports: []string{helper},
			helperImports: []string{goClient},
			want: []string{
				"forbidden query oracle dependency: " + queryModel + " reaches " + goClient,
				"forbidden query oracle dependency: " + queryModel + " reaches " + store,
			},
		},
		{
			name:          "oracle reaches HTTP through registered helper",
			oracleImports: []string{helper},
			helperImports: []string{httpapi},
			want: []string{
				"forbidden query oracle dependency: " + queryModel + " reaches " + httpapi,
				"forbidden query oracle dependency: " + queryModel + " reaches " + store,
			},
		},
		{
			name:          "oracle imports staged storage",
			oracleImports: []string{history},
			want: []string{
				"forbidden query oracle dependency: " + queryModel + " reaches " + history,
				"forbidden query oracle dependency: " + queryModel + " reaches " + store,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			rules := map[string]rule{
				server:     {serverCommand, []string{httpapi}},
				httpapi:    {runtimeLibrary, []string{store}},
				store:      {runtimeLibrary, nil},
				history:    {staged, []string{store}},
				goClient:   {standalone, []string{store}},
				release:    {verifier, []string{goClient, queryModel}},
				queryModel: {verifierModel, tt.oracleImports},
				helper:     {verifierModel, tt.helperImports},
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
			if got := queryOracleProblems(rules); !slices.Equal(got, tt.want) {
				t.Errorf("queryOracleProblems() = %q, want %q", got, tt.want)
			}
		})
	}
}
