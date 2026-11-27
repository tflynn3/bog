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

// Both Linux inventories at 74e56a7d2f46 leave the retained libraries outside
// cmd/bog. Remove each #108 declaration with its role change when the reviewed
// Storage revision shows reachability and satisfies ADR 0049.
var stagedPackages = map[string]stagingDeclaration{
	"bog/internal/checkpoint": {
		owner:      "bog-storage",
		activation: "#108 validates and durably selects a checkpoint successor under pinned storage authority before live activation at the requested cutoff.",
	},
	"bog/internal/history": {
		owner:      "bog-storage",
		activation: "#108 serves ordered checkpoint and suffix rows through the command-owned page source and proves a held cursor through replacement and acknowledged backfill, exact fresh and restarted walks, and final pin drain.",
	},
	"bog/internal/backup": {
		owner:      "bog-storage",
		activation: "#108 uses backup APIs in server composition and proves verified writable restore, an acknowledged append, and exact HTTP parity after restart.",
	},
	"bog/internal/idempotency": {
		owner:      "bog-storage",
		activation: "The production WAL atomically commits and recovers retry records with their batches. HTTP ingest verifies concurrent same-key submissions, conflicting bodies, and retry after an ambiguous response and restart.",
	},
	streamPolicyEngine: {
		owner:      "Core Engineering",
		activation: "ADR 0050 requires measured cmd/bog reachability on Linux amd64 and arm64 and independent serving evidence on both native targets for admission and paging, durable redaction and replay, keyed retries where enabled, and scoped export and writable restore.",
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

func TestRetainedCutoverStaging(t *testing.T) {
	t.Parallel()
	const (
		backup      = "bog/internal/backup"
		checkpoint  = "bog/internal/checkpoint"
		history     = "bog/internal/history"
		idempotency = "bog/internal/idempotency"
		wal         = "bog/internal/wal"
	)
	tests := []struct {
		name                  string
		operatorOnly          bool
		promoteOperatorBackup bool
		keepDeclarations      []string
		dropIdempotency       bool
		dropStreamPolicy      bool
		wireIdempotency       bool
		leaveCheckpointStaged bool
		wantArchitecture      []string
		want                  []string
	}{
		{name: "retained libraries promoted while keyed ingest and stream policy stay staged"},
		{
			name:             "operator-only backup stays staged when history and checkpoint are wired",
			operatorOnly:     true,
			keepDeclarations: []string{backup},
		},
		{
			name:         "operator-only backup still needs its declaration",
			operatorOnly: true,
			want:         []string{"staged package " + backup + " has no staging declaration"},
		},
		{
			name:             "promoted history still needs its staging declaration removed",
			operatorOnly:     true,
			keepDeclarations: []string{backup, history},
			want:             []string{"stale staging declaration for " + history + ": package is registered as runtime library"},
		},
		{
			name:                  "operator-only backup cannot be promoted",
			operatorOnly:          true,
			promoteOperatorBackup: true,
			wantArchitecture:      []string{backup + " is registered as runtime library but reachability from " + server + " is false"},
		},
		{
			name:             "promoted packages retain stale declarations",
			keepDeclarations: []string{backup, checkpoint, history},
			want: []string{
				"stale staging declaration for " + backup + ": package is registered as runtime library",
				"stale staging declaration for " + checkpoint + ": package is registered as runtime library",
				"stale staging declaration for " + history + ": package is registered as runtime library",
			},
		},
		{
			name:                  "wired checkpoint keeps its staged role",
			leaveCheckpointStaged: true,
			keepDeclarations:      []string{checkpoint},
			wantArchitecture:      []string{checkpoint + " is registered as staged but reachability from " + server + " is true"},
		},
		{
			name:                  "wired checkpoint loses its declaration before its role is updated",
			leaveCheckpointStaged: true,
			wantArchitecture:      []string{checkpoint + " is registered as staged but reachability from " + server + " is true"},
			want:                  []string{"staged package " + checkpoint + " has no staging declaration"},
		},
		{
			name:            "keyed ingest declaration removed with the retained declarations",
			dropIdempotency: true,
			want:            []string{"staged package " + idempotency + " has no staging declaration"},
		},
		{
			name:             "stream policy declaration removed with the retained declarations",
			dropStreamPolicy: true,
			want:             []string{"staged package " + streamPolicyEngine + " has no staging declaration"},
		},
		{
			name:             "keyed ingest is wired while still staged",
			wireIdempotency:  true,
			wantArchitecture: []string{idempotency + " is registered as staged but reachability from " + server + " is true"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			// Illustrate promotion by reachability without prescribing the
			// exact set of direct imports in the Storage draft.
			rules := map[string]rule{
				server:             {serverCommand, []string{backup, history, wal}},
				operatorCommand:    {verifier, []string{backup}},
				backup:             {runtimeLibrary, nil},
				checkpoint:         {runtimeLibrary, nil},
				history:            {runtimeLibrary, []string{checkpoint}},
				idempotency:        {staged, nil},
				streamPolicyEngine: {staged, nil},
				wal:                {runtimeLibrary, nil},
			}
			if tt.operatorOnly {
				rules[server] = rule{serverCommand, []string{history, wal}}
				if !tt.promoteOperatorBackup {
					rules[backup] = rule{staged, nil}
				}
			}
			if tt.leaveCheckpointStaged {
				rules[checkpoint] = rule{staged, nil}
			}
			if tt.wireIdempotency {
				command := rules[server]
				command.imports = append(command.imports, idempotency)
				rules[server] = command
			}
			packages := make(map[string]listedPackage, len(rules))
			for path, entry := range rules {
				name := "library"
				if path == server || path == operatorCommand {
					name = "main"
				}
				packages[path] = productionPackage(name, entry.imports...)
			}
			if got := architectureProblems(packages, rules); !slices.Equal(got, tt.wantArchitecture) {
				t.Errorf("architectureProblems() = %q, want %q", got, tt.wantArchitecture)
			}
			declarations := make(map[string]stagingDeclaration)
			if !tt.dropIdempotency {
				declarations[idempotency] = stagingDeclaration{
					owner: "bog-storage", activation: "Commit and recover retry records atomically with their batches.",
				}
			}
			if !tt.dropStreamPolicy {
				declarations[streamPolicyEngine] = stagedPackages[streamPolicyEngine]
			}
			for _, path := range tt.keepDeclarations {
				declarations[path] = stagingDeclaration{
					owner: "bog-storage", activation: "Prove the retained operation through the server.",
				}
			}
			if got := stagedPackageProblems(rules, declarations); !slices.Equal(got, tt.want) {
				t.Errorf("stagedPackageProblems() = %q, want %q", got, tt.want)
			}
		})
	}
}
