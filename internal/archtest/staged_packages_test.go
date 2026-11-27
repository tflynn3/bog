package archtest

import (
	"fmt"
	"maps"
	"slices"
	"strings"
	"testing"
)

type stagingDeclaration struct {
	owner      string
	activation string
}

var stagedPackages = map[string]stagingDeclaration{
	"bog/internal/checkpoint": {
		owner:      "bog-storage",
		activation: "#53 validates and publishes checkpoint generations through the pinned WAL owner and proves durable replacement at the requested cutoff.",
	},
	"bog/internal/history": {
		owner:      "bog-storage",
		activation: "#53 serves ordered checkpoint and tail rows through the command-owned page source and proves a held HTTP cursor survives generation replacement.",
	},
	"bog/internal/backup": {
		owner:      "bog-storage",
		activation: "#53 uses backup APIs in server composition and proves verified writable restore, an acknowledged append, and exact HTTP parity after restart.",
	},
	"bog/internal/idempotency": {
		owner:      "bog-storage",
		activation: "The production WAL atomically commits and recovers retry records with their batches, and HTTP ingest covers concurrent same-key submissions, conflicting bodies, and retry after an ambiguous response and restart.",
	},
}

func TestStagedPackageDeclarations(t *testing.T) {
	t.Parallel()
	for _, problem := range stagedPackageProblems(registry, stagedPackages) {
		t.Error(problem)
	}
}

// TestLinuxArchitecture validates the roles against measured Linux production
// reachability. This check gives each staged role a reviewable activation plan.
func stagedPackageProblems(rules map[string]rule, declarations map[string]stagingDeclaration) []string {
	var problems []string
	for _, path := range slices.Sorted(maps.Keys(rules)) {
		if rules[path].role != staged {
			continue
		}
		declaration, exists := declarations[path]
		if !exists {
			problems = append(problems, fmt.Sprintf("staged package %s has no staging declaration", path))
			continue
		}
		if strings.TrimSpace(declaration.owner) == "" {
			problems = append(problems, fmt.Sprintf("staged package %s must name an owner", path))
		}
		if strings.TrimSpace(declaration.activation) == "" {
			problems = append(problems, fmt.Sprintf("staged package %s must state activation criteria", path))
		}
	}
	for _, path := range slices.Sorted(maps.Keys(declarations)) {
		entry, exists := rules[path]
		switch {
		case !exists:
			problems = append(problems, fmt.Sprintf("stale staging declaration for %s: package is unregistered", path))
		case entry.role != staged:
			problems = append(problems, fmt.Sprintf("stale staging declaration for %s: package is registered as %s", path, entry.role))
		}
	}
	return problems
}

func TestStagedPackageDeclarationFixtures(t *testing.T) {
	t.Parallel()
	const backup = "bog/internal/backup"
	valid := stagingDeclaration{owner: "bog-storage", activation: "Prove verified writable restore and restart parity."}
	tests := []struct {
		name         string
		rules        map[string]rule
		declarations map[string]stagingDeclaration
		want         []string
	}{
		{
			name:  "missing declaration",
			rules: map[string]rule{backup: {role: staged}},
			want:  []string{"staged package bog/internal/backup has no staging declaration"},
		},
		{
			name:         "owner is whitespace",
			rules:        map[string]rule{backup: {role: staged}},
			declarations: map[string]stagingDeclaration{backup: {owner: " \t", activation: valid.activation}},
			want:         []string{"staged package bog/internal/backup must name an owner"},
		},
		{
			name:         "criteria are whitespace",
			rules:        map[string]rule{backup: {role: staged}},
			declarations: map[string]stagingDeclaration{backup: {owner: valid.owner, activation: "\n "}},
			want:         []string{"staged package bog/internal/backup must state activation criteria"},
		},
		{
			name:         "package removed",
			declarations: map[string]stagingDeclaration{backup: valid},
			want:         []string{"stale staging declaration for bog/internal/backup: package is unregistered"},
		},
		{
			name: "server promotes backup with a stale declaration",
			rules: map[string]rule{
				server: {serverCommand, []string{backup}},
				backup: {role: runtimeLibrary},
			},
			declarations: map[string]stagingDeclaration{backup: valid},
			want:         []string{"stale staging declaration for bog/internal/backup: package is registered as runtime library"},
		},
		{
			name: "server promotes backup and removes its declaration",
			rules: map[string]rule{
				server: {serverCommand, []string{backup}},
				backup: {role: runtimeLibrary},
			},
		},
		{
			name: "operator consumption keeps backup staged",
			rules: map[string]rule{
				operatorCommand: {verifier, []string{backup}},
				backup:          {role: staged},
			},
			declarations: map[string]stagingDeclaration{backup: valid},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			rules := map[string]rule{server: {role: serverCommand}}
			maps.Copy(rules, tt.rules)
			requireValidCommandFixture(t, rules)
			if got := stagedPackageProblems(rules, tt.declarations); !slices.Equal(got, tt.want) {
				t.Errorf("stagedPackageProblems() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestRetainedPromotionKeepsKeyedIngestStaged(t *testing.T) {
	t.Parallel()
	const (
		backup      = "bog/internal/backup"
		checkpoint  = "bog/internal/checkpoint"
		history     = "bog/internal/history"
		idempotency = "bog/internal/idempotency"
		store       = "bog/internal/store"
		wal         = "bog/internal/wal"
	)
	rules := map[string]rule{
		server:          {serverCommand, []string{backup, history, wal}},
		operatorCommand: {verifier, []string{backup, goClient, store}},
		backup:          {runtimeLibrary, []string{store}},
		checkpoint:      {runtimeLibrary, []string{store}},
		history:         {runtimeLibrary, []string{checkpoint, store}},
		idempotency:     {staged, nil},
		store:           {runtimeLibrary, nil},
		wal:             {runtimeLibrary, []string{store}},
		goClient:        {standalone, []string{store}},
	}
	requireValidCommandFixture(t, rules)
	declarations := map[string]stagingDeclaration{
		idempotency: {owner: "bog-storage", activation: "Recover durable retry records atomically with their batches."},
	}
	if got := stagedPackageProblems(rules, declarations); len(got) != 0 {
		t.Errorf("retained promotion with keyed ingest staged: %v", got)
	}
	delete(declarations, idempotency)
	want := []string{"staged package " + idempotency + " has no staging declaration"}
	if got := stagedPackageProblems(rules, declarations); !slices.Equal(got, want) {
		t.Errorf("stagedPackageProblems() = %q, want %q", got, want)
	}
}
