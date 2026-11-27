package archtest

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"os/exec"
	"slices"
	"strings"
	"testing"
	"time"
)

const streamPolicyEngine = "bog/internal/streampolicy"

func TestStreamPolicyEngineBoundary(t *testing.T) {
	for _, arch := range []string{"amd64", "arm64"} {
		t.Run(arch, func(t *testing.T) {
			t.Parallel()
			ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
			defer cancel()

			cmd := linuxProductionList(ctx, "../..", arch, "-json=ImportPath,Module,GoFiles,CgoFiles,Imports", "./...")
			data, err := cmd.Output()
			if err != nil {
				var exitErr *exec.ExitError
				if errors.As(err, &exitErr) {
					t.Fatalf("list Linux production packages: %v\n%s", err, exitErr.Stderr)
				}
				t.Fatalf("list Linux production packages: %v", err)
			}
			packages := make(map[string]listedPackage)
			decoder := json.NewDecoder(bytes.NewReader(data))
			for {
				var pkg listedPackage
				err := decoder.Decode(&pkg)
				if err == io.EOF {
					break
				}
				if err != nil {
					t.Fatalf("decode Linux package list: %v", err)
				}
				if pkg.production() {
					packages[pkg.ImportPath] = pkg
				}
			}
			for _, problem := range streamPolicyEngineProblems(packages, registry) {
				t.Error(problem)
			}
		})
	}
}

func streamPolicyEngineProblems(packages map[string]listedPackage, rules map[string]rule) []string {
	var problems []string
	if entry, registered := rules[streamPolicyEngine]; registered && entry.role != staged && entry.role != runtimeLibrary {
		problems = append(problems, fmt.Sprintf("%s must be registered as %s or %s", streamPolicyEngine, staged, runtimeLibrary))
	}
	if !packages[streamPolicyEngine].production() {
		// TestLinuxArchitecture handles stale registrations and exact imports.
		return problems
	}

	// Follow dependency paths through other modules as well as the root module.
	reached := reachableImports(packages, streamPolicyEngine)
	for _, path := range slices.Sorted(maps.Keys(reached)) {
		pkg := packages[path]
		if path != streamPolicyEngine && pkg.Module != nil && pkg.Module.Main {
			problems = append(problems, fmt.Sprintf("forbidden stream policy engine dependency: %s reaches %s", streamPolicyEngine, path))
		}
	}
	for _, path := range slices.Sorted(maps.Keys(packages)) {
		switch rules[path].role {
		case verifier, verifierModel:
			// Registered operator and evidence packages must stay independent.
		default:
			if path != "bog/verify" && !strings.HasPrefix(path, "bog/verify/") &&
				path != "bog/bench" && !strings.HasPrefix(path, "bog/bench/") {
				continue
			}
		}
		if reachableImports(packages, path)[streamPolicyEngine] {
			problems = append(problems, fmt.Sprintf("forbidden verifier dependency: %s reaches %s", path, streamPolicyEngine))
		}
	}
	return problems
}

func TestStreamPolicyEngineBoundaryFixtures(t *testing.T) {
	t.Parallel()
	const (
		relay    = "example.com/relay"
		httpapi  = "bog/internal/httpapi"
		wal      = "bog/internal/wal"
		outgoing = "forbidden stream policy engine dependency: " + streamPolicyEngine + " reaches "
		incoming = "forbidden verifier dependency: "
	)
	for _, tc := range []struct {
		name  string
		role  role
		edges map[string][]string
		want  []string
	}{
		{name: "engine absent before handoff"},
		{
			name:  "staged engine with pure dependencies",
			role:  staged,
			edges: map[string][]string{streamPolicyEngine: {"strings", relay}, relay: {"sort"}},
		},
		{
			name:  "promoted engine consumed by runtime",
			role:  runtimeLibrary,
			edges: map[string][]string{server: {httpapi}, httpapi: {streamPolicyEngine}, streamPolicyEngine: {"strings"}},
		},
		{
			name:  "engine cannot be classified as standalone",
			role:  standalone,
			edges: map[string][]string{streamPolicyEngine: nil},
			want:  []string{streamPolicyEngine + " must be registered as staged or runtime library"},
		},
		{
			name:  "engine depends on transport",
			role:  staged,
			edges: map[string][]string{streamPolicyEngine: {httpapi, goClient}},
			want:  []string{outgoing + goClient, outgoing + httpapi},
		},
		{
			name:  "unregistered engine reaches WAL through another module",
			edges: map[string][]string{streamPolicyEngine: {relay}, relay: {wal}},
			want:  []string{outgoing + wal},
		},
		{
			name:  "engine uses verifier as policy code",
			role:  staged,
			edges: map[string][]string{streamPolicyEngine: {streamPolicyVerifier}},
			want:  []string{outgoing + streamPolicyVerifier},
		},
		{
			name:  "conformance runner uses engine to determine expected results",
			role:  staged,
			edges: map[string][]string{streamPolicyVerifier: {streamPolicyEngine}, streamPolicyEngine: nil},
			want:  []string{incoming + streamPolicyVerifier + " reaches " + streamPolicyEngine},
		},
		{
			name: "operator and evidence packages reach engine through another module",
			role: staged,
			edges: map[string][]string{
				operatorCommand:    {relay},
				releaseVerifier:    {relay},
				retainedBench:      {relay},
				relay:              {streamPolicyEngine},
				streamPolicyEngine: nil,
			},
			want: []string{
				incoming + retainedBench + " reaches " + streamPolicyEngine,
				incoming + operatorCommand + " reaches " + streamPolicyEngine,
				incoming + releaseVerifier + " reaches " + streamPolicyEngine,
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			rules := map[string]rule{
				operatorCommand:      {role: verifier},
				streamPolicyVerifier: {role: verifierModel},
			}
			if tc.role != "" {
				rules[streamPolicyEngine] = rule{role: tc.role}
			}
			packages := make(map[string]listedPackage)
			for path, imports := range tc.edges {
				pkg := streamPolicyFixturePackage(path)
				pkg.Imports = imports
				packages[path] = pkg
				for _, imported := range imports {
					if _, exists := packages[imported]; !exists {
						packages[imported] = streamPolicyFixturePackage(imported)
					}
				}
			}
			if got := streamPolicyEngineProblems(packages, rules); !slices.Equal(got, tc.want) {
				t.Errorf("streamPolicyEngineProblems() = %q, want %q", got, tc.want)
			}
		})
	}
}

func streamPolicyFixturePackage(path string) listedPackage {
	if strings.HasPrefix(path, "bog/") {
		return productionPackage("library")
	}
	return listedPackage{GoFiles: []string{"fixture.go"}}
}
