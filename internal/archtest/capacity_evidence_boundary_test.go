package archtest

import (
	"fmt"
	"maps"
	"slices"
	"testing"
)

const capacityEvidenceVerifier = "bog/verify/capacityevidence"

func capacityEvidenceBoundaryProblems(packages map[string]listedPackage, rules map[string]rule) []string {
	var problems []string
	if entry, ok := rules[capacityEvidenceVerifier]; !ok || entry.role != verifierModel {
		problems = append(problems, fmt.Sprintf("%s must be registered as %s", capacityEvidenceVerifier, verifierModel))
	}
	for _, imported := range packages[capacityEvidenceVerifier].Imports {
		if dependency := packages[imported]; dependency.Module != nil && dependency.Module.Main && dependency.production() {
			problems = append(problems, fmt.Sprintf("forbidden capacity evidence import: %s -> %s", capacityEvidenceVerifier, imported))
		}
	}
	for _, consumer := range []string{releaseVerifier, retainedBench} {
		if !slices.Contains(packages[consumer].Imports, capacityEvidenceVerifier) {
			problems = append(problems, fmt.Sprintf("%s must directly import %s", consumer, capacityEvidenceVerifier))
		}
	}
	// Measure reachability through the full production inventory, including
	// dependencies outside the registered root module.
	for _, path := range slices.Sorted(maps.Keys(rules)) {
		switch path {
		case capacityEvidenceVerifier, releaseVerifier, retainedBench:
			continue
		}
		if reachableImports(packages, path)[capacityEvidenceVerifier] {
			problems = append(problems, fmt.Sprintf("forbidden capacity evidence dependency: %s reaches %s", path, capacityEvidenceVerifier))
		}
	}
	return problems
}

func TestCapacityEvidenceBoundaryFixtures(t *testing.T) {
	t.Parallel()
	const (
		relay         = "bog/verify/relay"
		externalRelay = "example.org/relay"
		library       = "bog/internal/library"
		roleProblem   = capacityEvidenceVerifier + " must be registered as verifier support"
		directImport  = " must directly import " + capacityEvidenceVerifier
		forbidden     = "forbidden capacity evidence dependency: "
		reaches       = " reaches " + capacityEvidenceVerifier
	)
	for _, tc := range []struct {
		name             string
		changes          map[string]rule
		externalImporter string
		want             []string
	}{
		{name: "both consumers"},
		{
			name:    "release omits the validator",
			changes: map[string]rule{releaseVerifier: {role: verifier}},
			want:    []string{releaseVerifier + directImport},
		},
		{
			name: "benchmark only imports the validator indirectly",
			changes: map[string]rule{
				retainedBench: {verifier, []string{relay}},
				relay:         {verifierModel, []string{capacityEvidenceVerifier}},
			},
			want: []string{retainedBench + directImport, forbidden + relay + reaches},
		},
		{
			name:    "another evidence command imports the validator",
			changes: map[string]rule{linuxVerifier: {verifier, []string{capacityEvidenceVerifier}}},
			want:    []string{forbidden + linuxVerifier + reaches},
		},
		{
			name: "operator reaches validator through a registered support package",
			changes: map[string]rule{
				operatorCommand: {verifier, []string{relay}},
				relay:           {verifierModel, []string{capacityEvidenceVerifier}},
			},
			want: []string{forbidden + operatorCommand + reaches, forbidden + relay + reaches},
		},
		{
			name:             "operator reaches validator through a different module",
			externalImporter: operatorCommand,
			want:             []string{forbidden + operatorCommand + reaches},
		},
		{
			name: "server reaches a validator reclassified as runtime",
			changes: map[string]rule{
				server:                   {serverCommand, []string{library}},
				library:                  {runtimeLibrary, []string{capacityEvidenceVerifier}},
				capacityEvidenceVerifier: {role: runtimeLibrary},
			},
			want: []string{roleProblem, forbidden + server + reaches, forbidden + library + reaches},
		},
		{
			name: "validator imports another root-module library",
			changes: map[string]rule{
				capacityEvidenceVerifier: {verifierModel, []string{relay}},
				relay:                    {role: verifierModel},
			},
			want: []string{"forbidden capacity evidence import: " + capacityEvidenceVerifier + " -> " + relay},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			rules := map[string]rule{
				server:                   {role: serverCommand},
				operatorCommand:          {role: verifier},
				releaseVerifier:          {verifier, []string{capacityEvidenceVerifier}},
				retainedBench:            {verifier, []string{capacityEvidenceVerifier}},
				linuxVerifier:            {role: verifier},
				capacityEvidenceVerifier: {role: verifierModel},
			}
			maps.Copy(rules, tc.changes)
			packages := make(map[string]listedPackage, len(rules))
			for path, entry := range rules {
				name := "library"
				if entry.role == serverCommand || entry.role == verifier {
					name = "main"
				}
				packages[path] = productionPackage(name, entry.imports...)
			}
			if path := tc.externalImporter; path != "" {
				pkg := packages[path]
				pkg.Imports = append(slices.Clone(pkg.Imports), externalRelay)
				packages[path] = pkg
				packages[externalRelay] = listedPackage{GoFiles: []string{"relay.go"}, Imports: []string{capacityEvidenceVerifier}}
			}
			if problems := architectureProblems(packages, rules); len(problems) != 0 {
				t.Fatalf("fixture violates the existing architecture rules: %v", problems)
			}
			if got := capacityEvidenceBoundaryProblems(packages, rules); !slices.Equal(got, tc.want) {
				t.Errorf("capacityEvidenceBoundaryProblems() = %q, want %q", got, tc.want)
			}
		})
	}
}
