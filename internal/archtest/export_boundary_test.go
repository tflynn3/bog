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

func exportBoundaryProblems(rules map[string]rule) []string {
	entry, ok := rules[exportCommand]
	if !ok || entry.role != verifier {
		return []string{fmt.Sprintf("%s must be registered as an operator command", exportCommand)}
	}
	var problems []string
	if !slices.Contains(entry.imports, goClient) {
		problems = append(problems, fmt.Sprintf("%s must import %s directly", exportCommand, goClient))
	}
	reached := registeredReachability(rules, exportCommand)
	for _, path := range slices.Sorted(maps.Keys(reached)) {
		switch path {
		case server, operatorCommand, "bog/internal/httpapi", "bog/internal/readiness", "bog/internal/wal",
			"bog/internal/backup", "bog/internal/checkpoint", "bog/internal/history", "bog/internal/idempotency":
		default:
			if path != "bog/verify" && !strings.HasPrefix(path, "bog/verify/") &&
				path != "bog/bench" && !strings.HasPrefix(path, "bog/bench/") {
				continue
			}
		}
		problems = append(problems, fmt.Sprintf("forbidden operator export dependency: %s reaches %s", exportCommand, path))
	}
	return problems
}

func TestOperatorExportBoundaryFixtures(t *testing.T) {
	t.Parallel()
	const (
		store  = "bog/internal/store"
		wal    = "bog/internal/wal"
		backup = "bog/internal/backup"
		relay  = "bog/internal/exportrelay"
	)
	for _, tt := range []struct {
		name    string
		imports []string
		via     []string
		want    []string
	}{
		{name: "client and shared row types", imports: []string{goClient, store}},
		{name: "missing client", imports: []string{store}, want: []string{exportCommand + " must import " + goClient + " directly"}},
		{name: "direct WAL", imports: []string{goClient, wal}, want: []string{"forbidden operator export dependency: " + exportCommand + " reaches " + wal}},
		{name: "retained storage and evidence through a relay", imports: []string{goClient, relay}, via: []string{backup, queryModel}, want: []string{
			"forbidden operator export dependency: " + exportCommand + " reaches " + backup,
			"forbidden operator export dependency: " + exportCommand + " reaches " + queryModel,
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			rules := map[string]rule{
				exportCommand: {verifier, tt.imports},
				goClient:      {standalone, []string{store}},
				store:         {standalone, nil},
				wal:           {staged, []string{store}},
				backup:        {staged, []string{store}},
				queryModel:    {verifierModel, nil},
			}
			if len(tt.via) > 0 {
				rules[relay] = rule{standalone, tt.via}
			}
			requireValidCommandFixture(t, rules)
			if got := exportBoundaryProblems(rules); !slices.Equal(got, tt.want) {
				t.Errorf("exportBoundaryProblems() = %q, want %q", got, tt.want)
			}
		})
	}
}
