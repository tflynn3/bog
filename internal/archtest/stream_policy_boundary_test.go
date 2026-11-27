package archtest

import (
	"fmt"
	"maps"
	"slices"
	"strings"
	"testing"
)

const streamPolicyVerifier = "bog/verify/stream-policy"

func TestStreamPolicyVerifierBoundary(t *testing.T) {
	t.Parallel()
	for _, problem := range streamPolicyProblems(registry) {
		t.Error(problem)
	}
}

func streamPolicyProblems(rules map[string]rule) []string {
	var problems []string
	if entry, ok := rules[streamPolicyVerifier]; !ok || entry.role != verifierModel {
		problems = append(problems, fmt.Sprintf("%s must be registered as %s", streamPolicyVerifier, verifierModel))
	}

	reached := registeredReachability(rules, streamPolicyVerifier)
	for _, path := range slices.Sorted(maps.Keys(reached)) {
		if path == streamPolicyVerifier {
			continue
		}
		kind := rules[path].role
		if strings.HasPrefix(path, "bog/internal/") || strings.HasPrefix(path, "bog/cmd/") ||
			kind == runtimeLibrary || kind == staged || kind == serverCommand || kind == verifier {
			problems = append(problems, fmt.Sprintf("forbidden stream policy dependency: %s reaches %s", streamPolicyVerifier, path))
		}
	}
	for _, path := range slices.Sorted(maps.Keys(rules)) {
		kind := rules[path].role
		if (kind == serverCommand || kind == verifier) && path != streamPolicyVerifier &&
			registeredReachability(rules, path)[streamPolicyVerifier] {
			problems = append(problems, fmt.Sprintf("forbidden command dependency: %s reaches %s", path, streamPolicyVerifier))
		}
	}
	return problems
}

func TestStreamPolicyBoundaryFixtures(t *testing.T) {
	t.Parallel()
	const (
		release  = "bog/verify/release"
		helper   = "bog/verify/helper"
		httpapi  = "bog/internal/httpapi"
		storage  = "bog/internal/staged"
		outgoing = "forbidden stream policy dependency: " + streamPolicyVerifier + " reaches "
		incoming = " reaches " + streamPolicyVerifier
	)
	for _, tc := range []struct {
		name  string
		edges map[string][]string
		want  []string
	}{
		{name: "independent verifier support", edges: map[string][]string{streamPolicyVerifier: {helper}}},
		{
			name:  "direct internal dependency",
			edges: map[string][]string{streamPolicyVerifier: {httpapi}},
			want:  []string{outgoing + httpapi},
		},
		{
			name:  "indirect staged dependency",
			edges: map[string][]string{streamPolicyVerifier: {helper}, helper: {storage}},
			want:  []string{outgoing + storage},
		},
		{
			name:  "direct command dependency",
			edges: map[string][]string{streamPolicyVerifier: {release}},
			want:  []string{outgoing + release},
		},
		{
			name:  "operator via verifier support",
			edges: map[string][]string{operatorCommand: {helper}, helper: {streamPolicyVerifier}},
			want:  []string{"forbidden command dependency: " + operatorCommand + incoming},
		},
		{
			name:  "release command imports verifier directly",
			edges: map[string][]string{release: {streamPolicyVerifier}},
			want:  []string{"forbidden command dependency: " + release + incoming},
		},
		{
			name:  "server reclassifies verifier as runtime",
			edges: map[string][]string{server: {httpapi}, httpapi: {streamPolicyVerifier}},
			want: []string{
				streamPolicyVerifier + " must be registered as verifier support",
				"forbidden command dependency: " + server + incoming,
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			rules := map[string]rule{
				server:               {serverCommand, []string{httpapi}},
				httpapi:              {role: runtimeLibrary},
				operatorCommand:      {role: verifier},
				release:              {role: verifier},
				helper:               {role: verifierModel},
				storage:              {role: staged},
				streamPolicyVerifier: {role: verifierModel},
			}
			for path, imports := range tc.edges {
				entry := rules[path]
				entry.imports = imports
				rules[path] = entry
			}
			if slices.Contains(tc.edges[httpapi], streamPolicyVerifier) {
				rules[streamPolicyVerifier] = rule{role: runtimeLibrary}
			}
			// All fixtures must pass the exact-import and reachability rules.
			requireValidCommandFixture(t, rules)
			if got := streamPolicyProblems(rules); !slices.Equal(got, tc.want) {
				t.Errorf("streamPolicyProblems() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestStreamPolicyVerifierRegistration(t *testing.T) {
	t.Parallel()
	want := []string{streamPolicyVerifier + " must be registered as verifier support"}
	if got := streamPolicyProblems(nil); !slices.Equal(got, want) {
		t.Errorf("streamPolicyProblems(nil) = %q, want %q", got, want)
	}
}
