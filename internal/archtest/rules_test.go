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
			want: []string{store + " is registered as runtime library but reachability from " + server + " is false"},
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
