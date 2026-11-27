package archtest

import (
	"slices"
	"strings"
	"testing"
)

func TestArchitectureRules(t *testing.T) {
	const (
		store      = "bog/internal/store"
		checkpoint = "bog/internal/checkpoint"
		history    = "bog/internal/history"
		backup     = "bog/internal/backup"
		wal        = "bog/internal/wal"
		ingest     = "bog/internal/ingest"
		release    = "bog/verify/release"
		extra      = "bog/internal/extra"
		external   = "example.com/dependency"
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
			name: "unregistered test-only package",
			change: func(pkgs map[string]listedPackage, _ map[string]rule) {
				pkgs[extra] = pkgs[ingest]
			},
			want: []string{"unregistered test-only package " + extra},
		},
		{
			name: "test import cannot be registered as a production import",
			change: func(_ map[string]listedPackage, rules map[string]rule) {
				rules[ingest] = rule{testOnly, []string{store}}
			},
			want: []string{ingest + " must register no production imports"},
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

func TestComposedCutoverRules(t *testing.T) {
	const (
		httpapi    = "bog/internal/httpapi"
		readiness  = "bog/internal/readiness"
		wal        = "bog/internal/wal"
		store      = "bog/internal/store"
		checkpoint = "bog/internal/checkpoint"
		history    = "bog/internal/history"
		backup     = "bog/internal/backup"
		adapter    = "bog/internal/retained"
	)
	tests := []struct {
		name   string
		change func(map[string]listedPackage, map[string]rule)
		want   []string
	}{
		{name: "command composes retained storage and readiness independently"},
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
			name: "one Linux inventory lacks the command's backup wiring",
			change: func(pkgs map[string]listedPackage, _ map[string]rule) {
				pkgs[server] = productionPackage("main", history, httpapi, readiness, store, wal)
			},
			want: []string{
				backup + " is registered as runtime library but reachability from " + server + " is false",
				"stale production import registration: " + server + " -> " + backup,
			},
		},
		{
			name: "registered retained dependencies form a cycle",
			change: func(pkgs map[string]listedPackage, rules map[string]rule) {
				pkgs[checkpoint] = productionPackage("checkpoint", store, history)
				rules[checkpoint] = rule{runtimeLibrary, []string{store, history}}
			},
			want: []string{"production import cycle: " + history + " -> " + checkpoint + " -> " + history},
		},
		{
			name: "registered checkpoint import still violates storage direction",
			change: func(pkgs map[string]listedPackage, rules map[string]rule) {
				pkgs[checkpoint] = productionPackage("checkpoint", store, httpapi)
				rules[checkpoint] = rule{runtimeLibrary, []string{store, httpapi}}
			},
			want: []string{
				"forbidden storage dependency: " + checkpoint + " -> " + httpapi,
				"forbidden storage dependency: " + history + " -> " + httpapi,
			},
		},
		{
			name: "registered history import still cannot depend on readiness",
			change: func(pkgs map[string]listedPackage, rules map[string]rule) {
				pkgs[history] = productionPackage("history", checkpoint, store, readiness)
				rules[history] = rule{runtimeLibrary, []string{checkpoint, store, readiness}}
			},
			want: []string{"forbidden storage dependency: " + history + " -> " + readiness},
		},
		{
			name: "backup cannot reach HTTP through a registered adapter",
			change: func(pkgs map[string]listedPackage, rules map[string]rule) {
				pkgs[backup] = productionPackage("backup", store, adapter)
				pkgs[adapter] = productionPackage("retained", httpapi)
				rules[backup] = rule{runtimeLibrary, []string{store, adapter}}
				rules[adapter] = rule{runtimeLibrary, []string{httpapi}}
			},
			want: []string{"forbidden storage dependency: " + backup + " -> " + httpapi},
		},
		{
			name: "unregistered backup import violates both rules",
			change: func(pkgs map[string]listedPackage, _ map[string]rule) {
				pkgs[backup] = productionPackage("backup", store, httpapi)
			},
			want: []string{
				"forbidden production import: " + backup + " -> " + httpapi,
				"forbidden storage dependency: " + backup + " -> " + httpapi,
			},
		},
		{
			name: "registered HTTP import still cannot bypass the consumer seam",
			change: func(pkgs map[string]listedPackage, rules map[string]rule) {
				pkgs[httpapi] = productionPackage("httpapi", store, wal, history)
				rules[httpapi] = rule{runtimeLibrary, []string{store, wal, history}}
			},
			want: []string{
				"HTTP must use the page source seam: " + httpapi + " reaches " + checkpoint,
				"HTTP must use the page source seam: " + httpapi + " reaches " + history,
			},
		},
		{
			name: "registered readiness import still cannot reach backup",
			change: func(pkgs map[string]listedPackage, rules map[string]rule) {
				pkgs[readiness] = productionPackage("readiness", store, wal, backup)
				rules[readiness] = rule{runtimeLibrary, []string{store, wal, backup}}
			},
			want: []string{"forbidden readiness dependency: " + readiness + " reaches " + backup},
		},
		{
			name: "readiness cannot reach retained history through the observed WAL",
			change: func(pkgs map[string]listedPackage, rules map[string]rule) {
				pkgs[httpapi] = productionPackage("httpapi", store)
				rules[httpapi] = rule{runtimeLibrary, []string{store}}
				pkgs[wal] = productionPackage("wal", store, history)
				rules[wal] = rule{runtimeLibrary, []string{store, history}}
			},
			want: []string{
				"forbidden readiness dependency: " + readiness + " reaches " + checkpoint,
				"forbidden readiness dependency: " + readiness + " reaches " + history,
			},
		},
		{
			name: "extracted runtime adapter requires registration",
			change: func(pkgs map[string]listedPackage, rules map[string]rule) {
				pkgs[server] = productionPackage("main", httpapi, readiness, store, wal, adapter)
				pkgs[adapter] = productionPackage("retained", backup, history, wal)
				rules[server] = rule{serverCommand, []string{httpapi, readiness, store, wal, adapter}}
			},
			want: []string{
				"unregistered reachable production package " + adapter,
				"forbidden production import: " + adapter + " -> " + backup,
				"forbidden production import: " + adapter + " -> " + history,
				"forbidden production import: " + adapter + " -> " + wal,
			},
		},
		{
			name: "retired direct edge remains in the registry",
			change: func(pkgs map[string]listedPackage, _ map[string]rule) {
				pkgs[httpapi] = productionPackage("httpapi", store)
			},
			want: []string{"stale production import registration: " + httpapi + " -> " + wal},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			// Model an adapter in cmd/bog. History brings checkpoint in
			// transitively, so no speculative direct command edge is needed.
			pkgs := map[string]listedPackage{
				server:     productionPackage("main", backup, history, httpapi, readiness, store, wal),
				backup:     productionPackage("backup", store),
				checkpoint: productionPackage("checkpoint", store),
				history:    productionPackage("history", checkpoint, store),
				httpapi:    productionPackage("httpapi", store, wal),
				readiness:  productionPackage("readiness", store, wal),
				store:      productionPackage("store"),
				wal:        productionPackage("wal", store),
			}
			rules := map[string]rule{
				server:     {serverCommand, []string{backup, history, httpapi, readiness, store, wal}},
				backup:     {runtimeLibrary, []string{store}},
				checkpoint: {runtimeLibrary, []string{store}},
				history:    {runtimeLibrary, []string{checkpoint, store}},
				httpapi:    {runtimeLibrary, []string{store, wal}},
				readiness:  {runtimeLibrary, []string{store, wal}},
				store:      {runtimeLibrary, nil},
				wal:        {runtimeLibrary, []string{store}},
			}
			if tt.change != nil {
				tt.change(pkgs, rules)
			}
			if got := architectureProblems(pkgs, rules); !slices.Equal(got, tt.want) {
				t.Errorf("architectureProblems() = [%s], want [%s]", strings.Join(got, "\n"), strings.Join(tt.want, "\n"))
			}
		})
	}
}

func TestStorageDirection(t *testing.T) {
	const (
		httpapi   = "bog/internal/httpapi"
		readiness = "bog/internal/readiness"
		client    = "bog/internal/client"
		external  = "example.com/bridge"
	)
	for _, from := range []string{
		"bog/internal/backup",
		"bog/internal/checkpoint",
		"bog/internal/history",
		"bog/internal/idempotency",
		"bog/internal/store",
		"bog/internal/wal",
	} {
		for _, to := range []string{httpapi, readiness, client} {
			t.Run(from+" -> "+to, func(t *testing.T) {
				t.Parallel()
				// All local edges and roles agree with the registry. A
				// staged or detached library still cannot reach transport,
				// even when an external dependency provides the path.
				pkgs := map[string]listedPackage{
					server:   productionPackage("main"),
					from:     productionPackage("storage", external),
					to:       productionPackage("service"),
					external: {Imports: []string{to}},
				}
				rules := map[string]rule{
					server: {serverCommand, nil},
					from:   {staged, nil},
					to:     {standalone, nil},
				}
				want := []string{"forbidden storage dependency: " + from + " -> " + to}
				if got := architectureProblems(pkgs, rules); !slices.Equal(got, want) {
					t.Errorf("architectureProblems() = %q, want %q", got, want)
				}
			})
		}
	}
}

func TestLinuxRegistryCorruptionHarness(t *testing.T) {
	t.Parallel()
	entry, present := registry[fuzzVerifier]
	if !present || entry.role != testOnly || len(entry.imports) != 0 {
		t.Fatalf("corruption harness registration = %+v, present = %t", entry, present)
	}
	harness := listedPackage{
		Name:        "fuzz",
		Module:      &struct{ Main bool }{Main: true},
		TestGoFiles: []string{"harness_linux_test.go"},
	}
	rules := map[string]rule{fuzzVerifier: entry}
	tests := []struct {
		name   string
		change func(map[string]listedPackage)
		want   []string
	}{
		{name: "Linux test files only"},
		{
			name: "harness absent from Linux inventory",
			change: func(pkgs map[string]listedPackage) {
				delete(pkgs, fuzzVerifier)
			},
			want: []string{"stale test-only registration: " + fuzzVerifier + " is absent from the Linux inventory"},
		},
		{
			name: "production files added alongside tests",
			change: func(pkgs map[string]listedPackage) {
				pkg := pkgs[fuzzVerifier]
				pkg.GoFiles = []string{"harness_linux.go"}
				pkgs[fuzzVerifier] = pkg
			},
			want: []string{fuzzVerifier + " must contain Linux test files only"},
		},
		{
			name: "converted to production only",
			change: func(pkgs map[string]listedPackage) {
				pkgs[fuzzVerifier] = productionPackage("fuzz")
			},
			want: []string{fuzzVerifier + " must contain Linux test files only"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			pkgs := map[string]listedPackage{fuzzVerifier: harness}
			if tt.change != nil {
				tt.change(pkgs)
			}
			if got := architectureProblems(pkgs, rules); !slices.Equal(got, tt.want) {
				t.Errorf("architectureProblems() = %q, want %q", got, tt.want)
			}
		})
	}
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
