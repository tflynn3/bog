package archtest

import (
	"maps"
	"slices"
	"testing"
)

// Project the supplied rules onto the import walk's graph. Registry rules
// contain direct root-module imports. Rules from measuredCutoverBoundaryProblems
// also carry production imports of external packages, including relays with no
// registered role.
func ruleImportGraph(rules map[string]rule) map[string]listedPackage {
	packages := make(map[string]listedPackage, len(rules))
	for path, entry := range rules {
		packages[path] = listedPackage{Imports: entry.imports}
	}
	return packages
}

func registeredReachability(rules map[string]rule, root string) map[string]bool {
	return reachableImports(ruleImportGraph(rules), root)
}

func TestRegisteredReachability(t *testing.T) {
	t.Parallel()
	const (
		localRelay    = "bog/internal/relay"
		externalRelay = "example.com/relay"
		store         = "bog/internal/store"
	)
	for _, tt := range []struct {
		name  string
		rules map[string]rule
		want  []string
	}{
		{
			name: "declared imports",
			rules: map[string]rule{
				server:     {serverCommand, []string{localRelay}},
				localRelay: {role: runtimeLibrary},
				store:      {role: staged},
			},
			want: []string{server, localRelay},
		},
		{
			name: "measured imports through an external relay without a role",
			rules: map[string]rule{
				server:        {serverCommand, []string{localRelay}},
				localRelay:    {runtimeLibrary, []string{externalRelay}},
				externalRelay: {imports: []string{store}},
				store:         {role: staged},
			},
			want: []string{server, localRelay, store, externalRelay},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := slices.Sorted(maps.Keys(registeredReachability(tt.rules, server)))
			if !slices.Equal(got, tt.want) {
				t.Errorf("registeredReachability() reached %q, want %q", got, tt.want)
			}
		})
	}
}
