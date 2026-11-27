package archtest

import (
	"fmt"
	"maps"
	"slices"
	"testing"
)

const listenerVerifier = "bog/verify/listener"

// Follow measured imports, including paths through another module.
func listenerBoundaryProblems(packages map[string]listedPackage, rules map[string]rule) []string {
	var problems []string
	if entry, ok := rules[listenerVerifier]; !ok || entry.role != verifierModel {
		problems = append(problems, fmt.Sprintf("%s must be registered as %s", listenerVerifier, verifierModel))
	}
	for _, root := range []string{releaseVerifier, retainedBench} {
		if !reachableImports(packages, root)[listenerVerifier] {
			problems = append(problems, fmt.Sprintf("%s must reach %s", root, listenerVerifier))
		}
	}
	for _, path := range slices.Sorted(maps.Keys(rules)) {
		switch path {
		case listenerVerifier, releaseVerifier, retainedBench:
			continue
		}
		if reachableImports(packages, path)[listenerVerifier] {
			problems = append(problems, fmt.Sprintf("forbidden candidate listener dependency: %s reaches %s", path, listenerVerifier))
		}
	}
	return problems
}

func TestCandidateListenerBoundaryFixtures(t *testing.T) {
	t.Parallel()
	const relay = "bog/internal/relay"
	for _, tc := range []struct {
		name   string
		change func(map[string]rule)
		want   []string
	}{
		{name: "release and capacity", change: func(map[string]rule) {}},
		{
			name: "capacity bypasses listener verification",
			change: func(rules map[string]rule) {
				rules[retainedBench] = rule{verifier, nil}
			},
			want: []string{retainedBench + " must reach " + listenerVerifier},
		},
		{
			name: "operator imports listener",
			change: func(rules map[string]rule) {
				rules[operatorCommand] = rule{verifier, []string{listenerVerifier}}
			},
			want: []string{"forbidden candidate listener dependency: " + operatorCommand + " reaches " + listenerVerifier},
		},
		{
			name: "server reaches listener through registered runtime library",
			change: func(rules map[string]rule) {
				rules[server] = rule{serverCommand, []string{relay}}
				rules[relay] = rule{runtimeLibrary, []string{listenerVerifier}}
				rules[listenerVerifier] = rule{runtimeLibrary, nil}
			},
			want: []string{
				listenerVerifier + " must be registered as verifier support",
				"forbidden candidate listener dependency: " + server + " reaches " + listenerVerifier,
				"forbidden candidate listener dependency: " + relay + " reaches " + listenerVerifier,
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			rules := map[string]rule{
				server:           {serverCommand, nil},
				operatorCommand:  {verifier, nil},
				releaseVerifier:  {verifier, []string{listenerVerifier}},
				retainedBench:    {verifier, []string{listenerVerifier}},
				listenerVerifier: {verifierModel, nil},
			}
			tc.change(rules)
			requireValidCommandFixture(t, rules)
			packages := make(map[string]listedPackage, len(rules))
			for path, entry := range rules {
				packages[path] = listedPackage{Imports: entry.imports}
			}
			if got := listenerBoundaryProblems(packages, rules); !slices.Equal(got, tc.want) {
				t.Errorf("listenerBoundaryProblems() = %q, want %q", got, tc.want)
			}
		})
	}
}
