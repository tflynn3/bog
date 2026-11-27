package archtest

import (
	"fmt"
	"maps"
	"slices"
	"testing"
)

const reportPublisher = "bog/verify/reportpublication"

func reportPublicationBoundaryProblems(packages map[string]listedPackage, rules map[string]rule) []string {
	var problems []string
	if rules[reportPublisher].role != verifierModel {
		problems = append(problems, fmt.Sprintf("%s must be registered as %s", reportPublisher, verifierModel))
	}
	for _, imported := range packages[reportPublisher].Imports {
		if dependency := packages[imported]; dependency.Module != nil && dependency.Module.Main && dependency.production() {
			problems = append(problems, fmt.Sprintf("forbidden report publication import: %s -> %s", reportPublisher, imported))
		}
	}
	consumers := []string{releaseVerifier, linuxVerifier, retainedBench, capacityEvidenceVerifier}
	for _, consumer := range consumers {
		if !slices.Contains(packages[consumer].Imports, reportPublisher) {
			problems = append(problems, fmt.Sprintf("%s must directly import %s", consumer, reportPublisher))
		}
	}
	for _, path := range slices.Sorted(maps.Keys(packages)) {
		pkg := packages[path]
		if path == reportPublisher || slices.Contains(consumers, path) ||
			pkg.Module == nil || !pkg.Module.Main || !pkg.production() {
			continue
		}
		if reachableImports(packages, path)[reportPublisher] {
			problems = append(problems, fmt.Sprintf("forbidden report publication dependency: %s reaches %s", path, reportPublisher))
		}
	}
	return problems
}

func TestReportPublicationBoundaryFixtures(t *testing.T) {
	t.Parallel()
	const (
		library  = "bog/internal/library"
		relay    = "bog/verify/relay"
		external = "example.org/relay"
	)
	fixture := func() map[string]listedPackage {
		pkgs := make(map[string]listedPackage)
		for _, path := range []string{server, operatorCommand, exportCommand, library, relay, reportPublisher} {
			pkgs[path] = productionPackage("fixture")
		}
		for _, path := range []string{releaseVerifier, linuxVerifier, retainedBench, capacityEvidenceVerifier} {
			pkgs[path] = productionPackage("fixture", reportPublisher)
		}
		return pkgs
	}
	require := func(t *testing.T, pkgs map[string]listedPackage, rules map[string]rule, want ...string) {
		t.Helper()
		if got := reportPublicationBoundaryProblems(pkgs, rules); !slices.Equal(got, want) {
			t.Errorf("report publication boundary = %q, want %q", got, want)
		}
	}
	forbidden := func(path string) string {
		return "forbidden report publication dependency: " + path + " reaches " + reportPublisher
	}
	rules := map[string]rule{reportPublisher: {role: verifierModel}, relay: {role: verifierModel}}
	require(t, fixture(), rules)
	for _, consumer := range []string{releaseVerifier, linuxVerifier, retainedBench, capacityEvidenceVerifier} {
		t.Run(consumer+" missing direct import", func(t *testing.T) {
			t.Parallel()
			pkgs := fixture()
			pkgs[consumer] = productionPackage("fixture", external)
			pkgs[external] = listedPackage{GoFiles: []string{"relay.go"}, Imports: []string{reportPublisher}}
			require(t, pkgs, rules, consumer+" must directly import "+reportPublisher)
		})
	}
	for _, path := range []string{server, operatorCommand, exportCommand, library} {
		t.Run(path, func(t *testing.T) {
			t.Parallel()
			pkgs := fixture()
			pkgs[path] = productionPackage("fixture", reportPublisher)
			require(t, pkgs, rules, forbidden(path))
			pkgs[path] = productionPackage("fixture", relay)
			pkgs[relay] = productionPackage("fixture", reportPublisher)
			want := []string{forbidden(path), forbidden(relay)}
			slices.Sort(want)
			require(t, pkgs, rules, want...)
			pkgs[relay] = productionPackage("fixture")
			pkgs[path] = productionPackage("fixture", external)
			pkgs[external] = listedPackage{GoFiles: []string{"relay.go"}, Imports: []string{capacityEvidenceVerifier}}
			require(t, pkgs, rules, forbidden(path))
		})
	}
	pkgs := fixture()
	pkgs[reportPublisher] = productionPackage("fixture", library)
	require(t, pkgs, nil, reportPublisher+" must be registered as verifier support",
		"forbidden report publication import: "+reportPublisher+" -> "+library)
}
