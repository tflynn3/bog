package archtest

import (
	"fmt"
	"maps"
	"slices"
	"testing"
)

func TestStandaloneProductionConsumers(t *testing.T) {
	t.Parallel()
	for _, problem := range standaloneConsumerProblems(registry) {
		t.Error(problem)
	}
}

// TestLinuxArchitecture checks that registered roles and edges match both
// Linux production inventories. This check gives standalone libraries a
// positive reachability requirement from the registered command roots.
func standaloneConsumerProblems(rules map[string]rule) []string {
	reached := make(map[string]bool)
	for path, entry := range rules {
		if entry.role == serverCommand || entry.role == verifier {
			maps.Copy(reached, registeredReachability(rules, path))
		}
	}

	var problems []string
	for _, path := range slices.Sorted(maps.Keys(rules)) {
		if rules[path].role == standalone && !reached[path] {
			problems = append(problems, fmt.Sprintf("standalone library %s has no registered production command consumer", path))
		}
	}
	return problems
}

func TestStandaloneProductionConsumerFixtures(t *testing.T) {
	t.Parallel()
	const (
		release = "bog/verify/release"
		helper  = "bog/internal/operatorhelper"
		backup  = "bog/internal/backup"
		orphan  = "bog/internal/orphan"
	)
	tests := []struct {
		name  string
		rules map[string]rule
		want  []string
	}{
		{
			name:  "operator consumes the client",
			rules: map[string]rule{operatorCommand: {verifier, []string{goClient, backup}}},
		},
		{
			name:  "evidence command consumes the client",
			rules: map[string]rule{release: {verifier, []string{goClient}}},
		},
		{
			name: "operator consumes the client through a registered helper",
			rules: map[string]rule{
				operatorCommand: {verifier, []string{helper}},
				helper:          {standalone, []string{goClient}},
			},
		},
		{
			name: "standalone library without any imports pointing to it",
			rules: map[string]rule{
				operatorCommand: {verifier, []string{goClient}},
				orphan:          {role: standalone},
			},
			want: []string{"standalone library bog/internal/orphan has no registered production command consumer"},
		},
		{
			name: "imports from unrooted libraries do not establish a consumer",
			rules: map[string]rule{
				operatorCommand: {verifier, []string{goClient}},
				backup:          {staged, []string{orphan}},
				queryModel:      {verifierModel, []string{orphan}},
				orphan:          {role: standalone},
			},
			want: []string{"standalone library bog/internal/orphan has no registered production command consumer"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			rules := map[string]rule{
				server:     {role: serverCommand},
				backup:     {role: staged},
				goClient:   {role: standalone},
				queryModel: {role: verifierModel},
			}
			maps.Copy(rules, tt.rules)
			requireValidCommandFixture(t, rules)

			// Test-only registrations carry no production edges.
			rules[fuzzVerifier] = rule{role: testOnly}
			if got := standaloneConsumerProblems(rules); !slices.Equal(got, tt.want) {
				t.Errorf("standaloneConsumerProblems() = %q, want %q", got, tt.want)
			}
		})
	}
}
