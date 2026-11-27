package archtest

import (
	"fmt"
	"maps"
	"slices"
	"testing"
)

const (
	releaseVerifier = "bog/verify/release"
	linuxVerifier   = "bog/verify/linux"
	retainedBench   = "bog/bench/retained-history"
)

func TestReleaseEvidenceBoundary(t *testing.T) {
	t.Parallel()
	for _, problem := range releaseEvidenceBoundaryProblems(registry) {
		t.Error(problem)
	}
}

// TestLinuxArchitecture checks the registry against Linux production imports
// on both architectures. Test imports stay outside that graph, so release
// tests can construct WAL, HTTP, and retained storage fixtures in process.
func releaseEvidenceBoundaryProblems(rules map[string]rule) []string {
	var problems []string
	for _, root := range []string{releaseVerifier, linuxVerifier, retainedBench} {
		if entry, ok := rules[root]; !ok || entry.role != verifier {
			problems = append(problems, fmt.Sprintf("%s must be registered as a %s", root, verifier))
		}
		reached := registeredReachability(rules, root)
		for _, path := range []string{
			"bog/internal/backup",
			"bog/internal/checkpoint",
			"bog/internal/history",
			"bog/internal/httpapi",
			"bog/internal/readiness",
			"bog/internal/wal",
		} {
			if reached[path] {
				problems = append(problems, fmt.Sprintf("forbidden release evidence dependency: %s reaches %s", root, path))
			}
		}
		if root == linuxVerifier && reached[retainedBench] {
			problems = append(problems, fmt.Sprintf("forbidden release evidence dependency: %s reaches %s", root, retainedBench))
		}
		if root == retainedBench && reached[linuxVerifier] {
			problems = append(problems, fmt.Sprintf("forbidden release evidence dependency: %s reaches %s", root, linuxVerifier))
		}
	}
	return problems
}

func TestDelegatedCapacityProcessBoundary(t *testing.T) {
	t.Parallel()
	const externalRelay = "example.com/capacity-relay"
	for _, pair := range [][2]string{{linuxVerifier, retainedBench}, {retainedBench, linuxVerifier}} {
		root, target := pair[0], pair[1]
		for _, relay := range []string{"", "bog/verify/evidencehelper", externalRelay} {
			name := relay
			if name == "" {
				name = "direct"
			}
			t.Run(root+"/"+name, func(t *testing.T) {
				t.Parallel()
				rules := map[string]rule{
					server:               {role: serverCommand},
					operatorCommand:      {verifier, []string{"bog/internal/backup"}},
					releaseVerifier:      {role: verifier},
					linuxVerifier:        {role: verifier},
					retainedBench:        {role: verifier},
					streamPolicyVerifier: {role: verifierModel},
				}
				imported := target
				if relay != "" {
					imported = relay
					rules[relay] = rule{verifierModel, []string{target}}
				}
				rules[root] = rule{verifier, []string{imported}}
				want := []string{"forbidden release evidence dependency: " + root + " reaches " + target}
				if relay != externalRelay {
					if got := releaseEvidenceBoundaryProblems(rules); !slices.Equal(got, want) {
						t.Errorf("declared boundary problems = %q, want %q", got, want)
					}
				}
				packages := make(map[string]listedPackage, len(rules))
				for path, entry := range rules {
					packages[path] = listedPackage{GoFiles: []string{"package.go"}, Imports: entry.imports}
				}
				if relay == externalRelay {
					delete(rules, relay)
				}
				if got := measuredCutoverBoundaryProblems(packages, rules); !slices.Equal(got, want) {
					t.Errorf("measured boundary problems = %q, want %q", got, want)
				}
			})
		}
	}
}

func TestReleaseEvidenceBoundaryFixtures(t *testing.T) {
	t.Parallel()
	const (
		backup     = "bog/internal/backup"
		checkpoint = "bog/internal/checkpoint"
		history    = "bog/internal/history"
		httpapi    = "bog/internal/httpapi"
		readiness  = "bog/internal/readiness"
		store      = "bog/internal/store"
		wal        = "bog/internal/wal"
		relay      = "bog/verify/evidencehelper"
	)
	// Model the composed server after the retained libraries are promoted.
	base := map[string]rule{
		server:          {serverCommand, []string{backup, history, httpapi, readiness, wal}},
		operatorCommand: {verifier, []string{backup, goClient, store}},
		backup:          {runtimeLibrary, []string{store}},
		checkpoint:      {runtimeLibrary, []string{store}},
		history:         {runtimeLibrary, []string{checkpoint, store}},
		httpapi:         {runtimeLibrary, []string{store, wal}},
		readiness:       {runtimeLibrary, []string{store, wal}},
		store:           {runtimeLibrary, nil},
		wal:             {runtimeLibrary, []string{store}},
		goClient:        {standalone, []string{store}},
		releaseVerifier: {verifier, []string{goClient, store}},
		linuxVerifier:   {verifier, nil},
		retainedBench:   {verifier, nil},
	}
	t.Run("production client and shared rows alongside retained server and offline reader", func(t *testing.T) {
		t.Parallel()
		requireValidCommandFixture(t, base)
		if got := releaseEvidenceBoundaryProblems(base); len(got) != 0 {
			t.Errorf("releaseEvidenceBoundaryProblems() = %q, want no problems", got)
		}
		if got := operatorBoundaryProblems(base); len(got) != 0 {
			t.Errorf("operatorBoundaryProblems() = %q, want no problems", got)
		}
	})

	paths := []struct {
		destination string
		reached     []string
	}{
		{backup, []string{backup}},
		{checkpoint, []string{checkpoint}},
		{history, []string{checkpoint, history}},
		{httpapi, []string{httpapi, wal}},
		{readiness, []string{readiness, wal}},
		{wal, []string{wal}},
	}
	for _, root := range []string{releaseVerifier, linuxVerifier, retainedBench} {
		for _, path := range paths {
			for _, mode := range []string{"direct", "transitive"} {
				t.Run(root+"/"+mode+"/"+path.destination, func(t *testing.T) {
					t.Parallel()
					rules := maps.Clone(base)
					imported := path.destination
					if mode == "transitive" {
						rules[relay] = rule{verifierModel, []string{imported}}
						imported = relay
					}
					entry := rules[root]
					entry.imports = append(slices.Clone(entry.imports), imported)
					rules[root] = entry
					requireValidCommandFixture(t, rules)

					var want []string
					for _, destination := range path.reached {
						want = append(want, "forbidden release evidence dependency: "+root+" reaches "+destination)
					}
					if got := releaseEvidenceBoundaryProblems(rules); !slices.Equal(got, want) {
						t.Errorf("releaseEvidenceBoundaryProblems() = %q, want %q", got, want)
					}
				})
			}
		}
	}
}

func TestReleaseEvidenceRootRegistration(t *testing.T) {
	t.Parallel()
	for _, root := range []string{releaseVerifier, linuxVerifier, retainedBench} {
		for _, tt := range []struct {
			name    string
			present bool
		}{
			{name: "missing"},
			{name: "reclassified as library", present: true},
		} {
			t.Run(root+"/"+tt.name, func(t *testing.T) {
				t.Parallel()
				rules := map[string]rule{
					releaseVerifier: {role: verifier},
					linuxVerifier:   {role: verifier},
					retainedBench:   {role: verifier},
				}
				if tt.present {
					rules[root] = rule{role: standalone}
				} else {
					delete(rules, root)
				}
				want := []string{root + " must be registered as a verifier command"}
				if got := releaseEvidenceBoundaryProblems(rules); !slices.Equal(got, want) {
					t.Errorf("releaseEvidenceBoundaryProblems() = %q, want %q", got, want)
				}
			})
		}
	}
}
