package archtest

import (
	"slices"
	"testing"
)

func TestStoreDependencyDirection(t *testing.T) {
	t.Parallel()
	const (
		store         = "bog/internal/store"
		localRelay    = "bog/internal/relay"
		localTarget   = "bog/internal/rowhelper"
		externalRelay = "example.com/relay"
		forbidden     = "forbidden store dependency: " + store + " reaches "
	)
	tests := []struct {
		name         string
		localImports map[string][]string
		relayImports []string
		wantDeclared []string
		wantMeasured []string
	}{
		{name: "store has no root-module production dependencies"},
		{
			name:         "store can reach external production packages",
			relayImports: []string{"time"},
		},
		{
			name:         "direct registered dependency",
			localImports: map[string][]string{store: {localTarget}, localTarget: nil},
			wantDeclared: []string{forbidden + localTarget},
			wantMeasured: []string{forbidden + localTarget},
		},
		{
			name: "dependency through a registered local relay",
			localImports: map[string][]string{
				store:       {localRelay},
				localRelay:  {localTarget},
				localTarget: nil,
			},
			wantDeclared: []string{forbidden + localRelay, forbidden + localTarget},
			wantMeasured: []string{forbidden + localRelay, forbidden + localTarget},
		},
		{
			name:         "dependency through an external relay",
			localImports: map[string][]string{localTarget: nil},
			relayImports: []string{localTarget},
			wantMeasured: []string{forbidden + localTarget},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			rules := map[string]rule{
				server:                {serverCommand, []string{store}},
				store:                 {role: runtimeLibrary},
				operatorCommand:       {verifier, []string{"bog/internal/backup"}},
				"bog/internal/backup": {role: staged},
				releaseVerifier:       {role: verifier},
				linuxVerifier:         {role: verifier},
				retainedBench:         {role: verifier},
				streamPolicyVerifier:  {role: verifierModel},
			}
			for path, imports := range tt.localImports {
				rules[path] = rule{runtimeLibrary, imports}
			}
			packages := make(map[string]listedPackage, len(rules)+2)
			for path, entry := range rules {
				name := "library"
				if entry.role == serverCommand || entry.role == verifier {
					name = "main"
				}
				packages[path] = productionPackage(name, entry.imports...)
			}
			packages["time"] = listedPackage{GoFiles: []string{"time.go"}}
			storePackage := packages[store]
			storePackage.Imports = append(slices.Clone(storePackage.Imports), "time")
			if len(tt.relayImports) > 0 {
				packages[externalRelay] = listedPackage{
					Module:  &struct{ Main bool }{},
					GoFiles: []string{"relay.go"},
					Imports: tt.relayImports,
				}
				storePackage.Imports = append(storePackage.Imports, externalRelay)
			}
			packages[store] = storePackage

			if got := architectureProblems(packages, rules); len(got) != 0 {
				t.Fatalf("fixture violates the production registry: %q", got)
			}
			if got := storageAuthorityProblems(rules); !slices.Equal(got, tt.wantDeclared) {
				t.Errorf("storageAuthorityProblems() = %q, want %q", got, tt.wantDeclared)
			}
			if got := measuredCutoverBoundaryProblems(packages, rules); !slices.Equal(got, tt.wantMeasured) {
				t.Errorf("measuredCutoverBoundaryProblems() = %q, want %q", got, tt.wantMeasured)
			}
		})
	}
}
