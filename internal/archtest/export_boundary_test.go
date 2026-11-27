package archtest

import (
	"fmt"
	"maps"
	"slices"
	"strings"
	"testing"
)

const exportCommand = "bog/cmd/bog-export"

func TestOperatorExportBoundary(t *testing.T) {
	t.Parallel()
	for _, problem := range exportBoundaryProblems(registry) {
		t.Error(problem)
	}
}

// The Linux inventories check the observed imports against the registry.
// Enforce ADR 0032 even when every dependency is registered.
func exportBoundaryProblems(rules map[string]rule) []string {
	entry, ok := rules[exportCommand]
	if !ok || entry.role != verifier {
		return []string{fmt.Sprintf("%s must be registered as an operator command", exportCommand)}
	}

	var problems []string
	if !slices.Equal(entry.imports, []string{goClient}) {
		problems = append(problems, fmt.Sprintf("%s must import only %s directly in production, got %q", exportCommand, goClient, entry.imports))
	}
	for _, path := range slices.Sorted(maps.Keys(registeredReachability(rules, exportCommand))) {
		if forbiddenExportDependency(path) {
			problems = append(problems, fmt.Sprintf("forbidden operator export dependency: %s reaches %s", exportCommand, path))
		}
	}
	return problems
}

func forbiddenExportDependency(path string) bool {
	switch path {
	case server, "bog/internal/httpapi", "bog/internal/readiness", "bog/internal/wal",
		"bog/internal/backup", "bog/internal/checkpoint", "bog/internal/history", "bog/internal/idempotency",
		"bog/verify", "bog/bench":
		return true
	default:
		return strings.HasPrefix(path, "bog/verify/") || strings.HasPrefix(path, "bog/bench/")
	}
}

func TestOperatorExportRegistration(t *testing.T) {
	t.Parallel()
	const store = "bog/internal/store"
	const missing = exportCommand + " must be registered as an operator command"
	const direct = exportCommand + " must import only " + goClient + " directly in production, got "
	for _, tt := range []struct {
		name  string
		rules map[string]rule
		want  string
	}{
		{name: "missing", want: missing},
		{name: "misclassified", rules: map[string]rule{exportCommand: {standalone, []string{goClient}}}, want: missing},
		{name: "no client", rules: map[string]rule{exportCommand: {role: verifier}}, want: direct + `[]`},
		{name: "shared row types replace client", rules: map[string]rule{exportCommand: {verifier, []string{store}}}, want: direct + `["` + store + `"]`},
		{name: "extra direct edge", rules: map[string]rule{exportCommand: {verifier, []string{goClient, store}}}, want: direct + `["` + goClient + `" "` + store + `"]`},
		{name: "duplicate client edge", rules: map[string]rule{exportCommand: {verifier, []string{goClient, goClient}}}, want: direct + `["` + goClient + `" "` + goClient + `"]`},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := exportBoundaryProblems(tt.rules); !slices.Equal(got, []string{tt.want}) {
				t.Errorf("exportBoundaryProblems() = %q, want [%q]", got, tt.want)
			}
		})
	}
}

func TestOperatorExportBoundaryFixtures(t *testing.T) {
	t.Parallel()
	const (
		store     = "bog/internal/store"
		relay     = "bog/internal/exportrelay"
		wal       = "bog/internal/wal"
		forbidden = "forbidden operator export dependency: " + exportCommand + " reaches "
	)
	fixture := func() map[string]rule {
		return map[string]rule{
			server:        {serverCommand, []string{store}},
			exportCommand: {verifier, []string{goClient}},
			goClient:      {standalone, []string{store}},
			store:         {role: runtimeLibrary},
		}
	}
	t.Run("client shares row types with the server", func(t *testing.T) {
		t.Parallel()
		rules := fixture()
		requireValidCommandFixture(t, rules)
		if got := exportBoundaryProblems(rules); len(got) != 0 {
			t.Errorf("exportBoundaryProblems() = %q, want no problems", got)
		}
	})
	t.Run("direct WAL import", func(t *testing.T) {
		t.Parallel()
		rules := fixture()
		rules[wal] = rule{role: staged}
		rules[exportCommand] = rule{verifier, []string{goClient, wal}}
		requireValidCommandFixture(t, rules)
		want := []string{
			exportCommand + " must import only " + goClient + ` directly in production, got ["` + goClient + `" "` + wal + `"]`,
			forbidden + wal,
		}
		if got := exportBoundaryProblems(rules); !slices.Equal(got, want) {
			t.Errorf("exportBoundaryProblems() = %q, want %q", got, want)
		}
	})
	for _, tt := range []struct {
		path     string
		excluded bool
	}{
		{server, true},
		{"bog/internal/httpapi", true},
		{"bog/internal/readiness", true},
		{wal, true},
		{"bog/internal/backup", true},
		{"bog/internal/checkpoint", true},
		{"bog/internal/history", true},
		{"bog/internal/idempotency", true},
		{"bog/verify", true},
		{"bog/verify/query-model", true},
		{"bog/bench", true},
		{"bog/bench/retained-history/metrics", true},
		{"bog/verifyextra/format", false},
		{"bog/benchmark/metrics", false},
	} {
		t.Run("registered relay to "+tt.path, func(t *testing.T) {
			t.Parallel()
			rules := fixture()
			rules[goClient] = rule{standalone, []string{store, relay}}
			rules[relay] = rule{standalone, []string{tt.path}}
			if _, registered := rules[tt.path]; !registered {
				rules[tt.path] = rule{role: standalone}
			}
			requireValidCommandFixture(t, rules)
			var want []string
			if tt.excluded {
				want = []string{forbidden + tt.path}
			}
			if got := exportBoundaryProblems(rules); !slices.Equal(got, want) {
				t.Errorf("exportBoundaryProblems() = %q, want %q", got, want)
			}
		})
	}
}
