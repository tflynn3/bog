package archtest

import (
	"slices"
	"testing"
)

func TestRecoveryViewBoundaryFixtures(t *testing.T) {
	t.Parallel()
	const (
		backup        = "bog/internal/backup"
		checkpoint    = "bog/internal/checkpoint"
		history       = "bog/internal/history"
		store         = "bog/internal/store"
		wal           = "bog/internal/wal"
		localRelay    = "bog/internal/relay"
		externalRelay = "example.com/relay"
		forbidden     = "forbidden recovery view dependency: "
	)

	check := func(t *testing.T, libraryRole role, from, to, relay string, want []string) {
		t.Helper()
		rules := map[string]rule{
			server:               {serverCommand, []string{wal}},
			operatorCommand:      {verifier, []string{backup, goClient, store}},
			backup:               {libraryRole, []string{store}},
			checkpoint:           {libraryRole, []string{store}},
			history:              {libraryRole, []string{checkpoint, store}},
			store:                {runtimeLibrary, nil},
			wal:                  {runtimeLibrary, []string{store}},
			goClient:             {standalone, []string{store}},
			releaseVerifier:      {verifier, []string{goClient}},
			linuxVerifier:        {role: verifier},
			retainedBench:        {role: verifier},
			streamPolicyVerifier: {role: verifierModel},
		}
		if libraryRole == runtimeLibrary {
			rules[server] = rule{serverCommand, []string{wal, backup, history}}
		}
		if from != "" && relay != externalRelay {
			target := to
			if relay == localRelay {
				rules[relay] = rule{libraryRole, []string{to}}
				target = relay
			}
			entry := rules[from]
			entry.imports = append(entry.imports, target)
			rules[from] = entry
		}

		packages := make(map[string]listedPackage, len(rules)+1)
		for path, entry := range rules {
			name := "library"
			if entry.role == serverCommand || entry.role == verifier {
				name = "main"
			}
			packages[path] = productionPackage(name, entry.imports...)
		}
		registryWant := want
		if relay == externalRelay {
			consumer := packages[from]
			consumer.Imports = append(slices.Clone(consumer.Imports), relay)
			packages[from] = consumer
			packages[relay] = listedPackage{GoFiles: []string{"relay.go"}, Imports: []string{to}}
			registryWant = nil // The registry contains only root-module edges.
		}
		if problems := architectureProblems(packages, rules); len(problems) != 0 {
			t.Fatalf("fixture violates the production registry: %q", problems)
		}
		if got := storageAuthorityProblems(rules); !slices.Equal(got, registryWant) {
			t.Errorf("storageAuthorityProblems() = %q, want %q", got, registryWant)
		}
		if got := measuredCutoverBoundaryProblems(packages, rules); !slices.Equal(got, want) {
			t.Errorf("measuredCutoverBoundaryProblems() = %q, want %q", got, want)
		}
	}

	t.Run("command composes independent views with history using checkpoint", func(t *testing.T) {
		t.Parallel()
		check(t, runtimeLibrary, "", "", "", nil)
	})
	t.Run("offline operator reads staged backup", func(t *testing.T) {
		t.Parallel()
		check(t, staged, "", "", "", nil)
	})

	for _, edge := range []struct {
		from, to string
		want     []string
	}{
		{backup, checkpoint, []string{forbidden + backup + " reaches " + checkpoint}},
		{backup, history, []string{forbidden + backup + " reaches " + checkpoint, forbidden + backup + " reaches " + history}},
		{checkpoint, backup, []string{forbidden + checkpoint + " reaches " + backup, forbidden + history + " reaches " + backup}},
		{history, backup, []string{forbidden + history + " reaches " + backup}},
	} {
		t.Run(edge.from+" -> "+edge.to, func(t *testing.T) {
			for _, route := range []struct{ name, relay string }{
				{"direct", ""},
				{"registered relay", localRelay},
				{"external relay", externalRelay},
			} {
				t.Run(route.name, func(t *testing.T) {
					t.Parallel()
					check(t, runtimeLibrary, edge.from, edge.to, route.relay, edge.want)
				})
			}
		})
	}
}
