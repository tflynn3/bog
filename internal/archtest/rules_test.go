package archtest

import (
	"slices"
	"strings"
	"testing"
)

func TestArchitectureRules(t *testing.T) {
	const (
		ingest   = "bog/internal/ingest"
		release  = "bog/verify/release"
		extra    = "bog/internal/extra"
		external = "example.com/dependency"
	)
	tests := []struct {
		name   string
		change func(map[string]listedPackage, map[string]rule)
		want   []string
	}{
		{name: "registered graph with offline backup and test-only ingest"},
		{
			name: "offline backup cannot import the live writer",
			change: func(pkgs map[string]listedPackage, _ map[string]rule) {
				pkgs[backup] = productionPackage("backup", store, wal)
			},
			want: []string{"forbidden production import: " + backup + " -> " + wal},
		},
		{
			name: "server wiring requires promoting offline backup",
			change: func(pkgs map[string]listedPackage, rules map[string]rule) {
				pkgs[server] = productionPackage("main", store, wal, backup)
				rules[server] = rule{serverCommand, []string{store, wal, backup}}
			},
			want: []string{backup + " is registered as staged but reachability from " + server + " is true"},
		},
		{
			name: "unregistered unreachable production package",
			change: func(pkgs map[string]listedPackage, _ map[string]rule) {
				pkgs[extra] = productionPackage("extra")
			},
			want: []string{"unregistered unreachable production package " + extra},
		},
		{
			name: "unregistered reachable production package",
			change: func(pkgs map[string]listedPackage, _ map[string]rule) {
				pkgs[extra] = productionPackage("extra")
				pkgs[server] = productionPackage("main", store, wal, extra)
			},
			want: []string{
				"forbidden production import: " + server + " -> " + extra,
				"unregistered reachable production package " + extra,
			},
		},
		{
			name: "removed staging entry",
			change: func(pkgs map[string]listedPackage, _ map[string]rule) {
				delete(pkgs, history)
			},
			want: []string{"stale staged registration: " + history + " is absent from the Linux inventory"},
		},
		{
			name: "staged package has only tests",
			change: func(pkgs map[string]listedPackage, _ map[string]rule) {
				pkg := pkgs[history]
				pkg.GoFiles = nil
				pkg.TestGoFiles = []string{"history_test.go"}
				pkgs[history] = pkg
			},
			want: []string{"stale staged registration: " + history + " has no Linux production files"},
		},
		{
			name: "staged packages become transitively reachable",
			change: func(pkgs map[string]listedPackage, _ map[string]rule) {
				pkgs[server] = productionPackage("main", store, wal, external)
				pkgs[external] = listedPackage{Imports: []string{history}}
			},
			want: []string{
				checkpoint + " is registered as staged but reachability from " + server + " is true",
				history + " is registered as staged but reachability from " + server + " is true",
			},
		},
		{
			name: "runtime package detached",
			change: func(pkgs map[string]listedPackage, _ map[string]rule) {
				pkgs[server] = productionPackage("main", wal)
			},
			want: []string{
				store + " is registered as runtime library but reachability from " + server + " is false",
				"stale production import registration: " + server + " -> " + store,
			},
		},
		{
			name: "test harness gains production files",
			change: func(pkgs map[string]listedPackage, _ map[string]rule) {
				pkgs[ingest] = productionPackage("ingest")
			},
			want: []string{ingest + " must contain Linux test files only"},
		},
		{
			name: "verifier cannot import runtime storage",
			change: func(pkgs map[string]listedPackage, _ map[string]rule) {
				pkgs[release] = productionPackage("main", store)
			},
			want: []string{"forbidden production import: " + release + " -> " + store},
		},
		{
			name: "verifier command changed to library",
			change: func(pkgs map[string]listedPackage, _ map[string]rule) {
				pkgs[release] = productionPackage("release")
			},
			want: []string{release + " has package name \"release\" incompatible with its verifier command role"},
		},
		{
			name: "cycle outside runtime even if imports are allowed",
			change: func(pkgs map[string]listedPackage, rules map[string]rule) {
				pkgs[checkpoint] = productionPackage("checkpoint", history)
				rules[checkpoint] = rule{staged, []string{history}}
			},
			want: []string{"production import cycle: " + checkpoint + " -> " + history + " -> " + checkpoint},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ingestPackage := productionPackage("ingest")
			ingestPackage.GoFiles = nil
			ingestPackage.XTestGoFiles = []string{"reference_test.go"}
			pkgs := map[string]listedPackage{
				server:     productionPackage("main", store, wal),
				store:      productionPackage("store"),
				wal:        productionPackage("wal"),
				checkpoint: productionPackage("checkpoint", store),
				history:    productionPackage("history", checkpoint, store),
				backup:     productionPackage("backup", store),
				ingest:     ingestPackage,
				release:    productionPackage("main"),
			}
			rules := map[string]rule{
				server:     {serverCommand, []string{store, wal}},
				store:      {runtimeLibrary, nil},
				wal:        {runtimeLibrary, nil},
				checkpoint: {staged, []string{store}},
				history:    {staged, []string{checkpoint, store}},
				backup:     {staged, []string{store}},
				ingest:     {testOnly, nil},
				release:    {verifier, nil},
			}
			if tt.change != nil {
				tt.change(pkgs, rules)
			}
			got := architectureProblems(pkgs, rules)
			if !slices.Equal(got, tt.want) {
				t.Errorf("architectureProblems() = [%s], want [%s]", strings.Join(got, "\n"), strings.Join(tt.want, "\n"))
			}
		})
	}
}

// This fixture exercises the retained boundaries independently of whether
// the command wiring is present in the checkout.
func TestComposedCutoverRules(t *testing.T) {
	tests := []struct {
		name   string
		change func(map[string]listedPackage, map[string]rule)
		want   []string
	}{
		{name: "retained wiring with promoted storage modules"},
		{
			name: "wiring without role promotions",
			change: func(_ map[string]listedPackage, rules map[string]rule) {
				for _, path := range []string{backup, checkpoint, history} {
					r := rules[path]
					r.role = staged
					rules[path] = r
				}
			},
			want: []string{
				backup + " is registered as staged but reachability from " + server + " is true",
				checkpoint + " is registered as staged but reachability from " + server + " is true",
				history + " is registered as staged but reachability from " + server + " is true",
			},
		},
		{
			name: "retained wiring without an adapter registration",
			change: func(_ map[string]listedPackage, rules map[string]rule) {
				delete(rules, retained)
			},
			want: []string{
				"unregistered reachable production package " + retained,
				"forbidden production import: " + retained + " -> " + backup,
				"forbidden production import: " + retained + " -> " + history,
				"forbidden production import: " + retained + " -> " + wal,
			},
		},
		{
			name: "retired direct command edge remains registered",
			change: func(_ map[string]listedPackage, rules map[string]rule) {
				r := rules[server]
				r.imports = append(r.imports, backup)
				rules[server] = r
			},
			want: []string{"stale production import registration: " + server + " -> " + backup},
		},
		{
			name: "registered storage cycle still fails",
			change: func(pkgs map[string]listedPackage, rules map[string]rule) {
				pkgs[checkpoint] = productionPackage("checkpoint", store, history)
				rules[checkpoint] = rule{runtimeLibrary, []string{store, history}}
			},
			want: []string{"production import cycle: " + history + " -> " + checkpoint + " -> " + history},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			pkgs, rules := composedCutoverFixture()
			if tt.change != nil {
				tt.change(pkgs, rules)
			}
			if got := architectureProblems(pkgs, rules); !slices.Equal(got, tt.want) {
				t.Errorf("architectureProblems() = [%s], want [%s]", strings.Join(got, "\n"), strings.Join(tt.want, "\n"))
			}
		})
	}
}

func TestCutoverForbiddenDirections(t *testing.T) {
	tests := []struct {
		from, to string
	}{
		{httpapi, retained},
		{httpapi, history},
		{readiness, backup},
		{checkpoint, httpapi},
		{history, readiness},
		{backup, httpapi},
		{retained, httpapi},
		{wal, retained},
	}
	for _, tt := range tests {
		t.Run(tt.from+" to "+tt.to, func(t *testing.T) {
			t.Parallel()
			pkgs, rules := composedCutoverFixture()
			pkg, r := pkgs[tt.from], rules[tt.from]
			pkg.Imports = append(pkg.Imports, tt.to)
			r.imports = append(r.imports, tt.to)
			pkgs[tt.from], rules[tt.from] = pkg, r

			want := "forbidden production import: " + tt.from + " -> " + tt.to
			if got := architectureProblems(pkgs, rules); !slices.Contains(got, want) {
				t.Errorf("architectureProblems() = %q, missing %q", got, want)
			}
		})
	}
}

func composedCutoverFixture() (map[string]listedPackage, map[string]rule) {
	pkgs := map[string]listedPackage{
		server:     productionPackage("main", httpapi, readiness, retained, store, wal),
		backup:     productionPackage("backup", store),
		checkpoint: productionPackage("checkpoint", store),
		history:    productionPackage("history", checkpoint, store),
		httpapi:    productionPackage("httpapi", store, wal),
		readiness:  productionPackage("readiness", store, wal),
		retained:   productionPackage("retained", backup, history, wal),
		store:      productionPackage("store"),
		wal:        productionPackage("wal", store),
	}
	rules := map[string]rule{
		server:     {serverCommand, []string{httpapi, readiness, retained, store, wal}},
		backup:     {runtimeLibrary, []string{store}},
		checkpoint: {runtimeLibrary, []string{store}},
		history:    {runtimeLibrary, []string{checkpoint, store}},
		httpapi:    {runtimeLibrary, []string{store, wal}},
		readiness:  {runtimeLibrary, []string{store, wal}},
		retained:   {runtimeLibrary, []string{backup, history, wal}},
		store:      {runtimeLibrary, nil},
		wal:        {runtimeLibrary, []string{store}},
	}
	return pkgs, rules
}

func productionPackage(name string, imports ...string) listedPackage {
	return listedPackage{
		Name:    name,
		Module:  &struct{ Main bool }{Main: true},
		GoFiles: []string{"package.go"},
		Imports: imports,
	}
}

func TestImportCycle(t *testing.T) {
	tests := []struct {
		name  string
		graph map[string][]string
		want  []string
	}{
		{
			name:  "diamond with external leaf",
			graph: map[string][]string{"a": {"b", "c"}, "b": {"d"}, "c": {"d"}, "d": {"external"}},
		},
		{
			name:  "self import",
			graph: map[string][]string{"a": {"a"}},
			want:  []string{"a", "a"},
		},
		{
			name:  "cycle excludes the path to it",
			graph: map[string][]string{"a": {"b"}, "b": {"c"}, "c": {"b"}},
			want:  []string{"b", "c", "b"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := importCycle(tt.graph); !slices.Equal(got, tt.want) {
				t.Errorf("importCycle() = %v, want %v", got, tt.want)
			}
		})
	}
}
