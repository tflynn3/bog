// Package archtest checks the Linux server's package boundaries from any Go host.
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

const (
	server       = "bog/cmd/bog"
	fuzzVerifier = "bog/verify/fuzz"
)

type role string

const (
	runtimeLibrary role = "runtime library"
	serverCommand  role = "server command"
	staged         role = "staged"
	standalone     role = "standalone"
	testOnly       role = "test-only"
	verifier       role = "verifier command"
	verifierModel  role = "verifier support"
)

type rule struct {
	role    role
	imports []string
}

// Record the observed direct production imports within the root module.
// Update roles and edges in the same change as the wiring that requires them.
// The retained command composes checkpoint and history with the WAL owner.
// The operator retains its direct backup import.
var registry = map[string]rule{
	exportCommand:                {verifier, []string{goClient}},
	operatorCommand:              {verifier, []string{"bog/internal/backup", goClient, "bog/internal/store"}},
	server:                       {serverCommand, []string{"bog/internal/checkpoint", "bog/internal/history", "bog/internal/httpapi", "bog/internal/readiness", "bog/internal/store", "bog/internal/wal"}},
	"bog/internal/httpapi":       {runtimeLibrary, []string{"bog/internal/store", "bog/internal/wal"}},
	"bog/internal/readiness":     {runtimeLibrary, []string{"bog/internal/store", "bog/internal/wal"}},
	"bog/internal/wal":           {runtimeLibrary, []string{"bog/internal/store"}},
	"bog/internal/store":         {runtimeLibrary, nil},
	"bog/internal/checkpoint":    {runtimeLibrary, []string{"bog/internal/store"}},
	"bog/internal/history":       {runtimeLibrary, []string{"bog/internal/checkpoint", "bog/internal/store"}},
	"bog/internal/backup":        {staged, []string{"bog/internal/store"}},
	"bog/internal/idempotency":   {staged, nil},
	streamPolicyEngine:           {staged, nil},
	goClient:                     {standalone, []string{"bog/internal/store"}},
	"bog/internal/ingest":        {testOnly, nil},
	"bog/internal/archtest":      {testOnly, nil},
	fuzzVerifier:                 {testOnly, nil},
	"bog/verify/release":         {verifier, []string{goClient, capacityEvidenceVerifier, listenerVerifier, reportPublisher}},
	"bog/verify/linux":           {verifier, []string{reportPublisher}},
	"bog/bench/retained-history": {verifier, []string{capacityEvidenceVerifier, listenerVerifier, reportPublisher}},
	capacityEvidenceVerifier:     {verifierModel, []string{reportPublisher}},
	reportPublisher:              {verifierModel, nil},
	listenerVerifier:             {verifierModel, nil},
	"bog/verify/query-model":     {verifierModel, nil},
	streamPolicyVerifier:         {verifierModel, nil},
}

// Operator roots use the verifier role for package kind and reachability.
func isOperatorCommand(path string) bool {
	return path == operatorCommand || path == exportCommand
}

type listedPackage struct {
	ImportPath   string
	Name         string
	Module       *struct{ Main bool }
	GoFiles      []string
	CgoFiles     []string
	TestGoFiles  []string
	XTestGoFiles []string
	Imports      []string
}

func (p listedPackage) production() bool {
	return len(p.GoFiles)+len(p.CgoFiles) > 0
}

func TestLinuxArchitecture(t *testing.T) {
	for _, arch := range []string{"amd64", "arm64"} {
		t.Run(arch, func(t *testing.T) {
			t.Parallel()
			checkLinuxArchitecture(t, arch)
		})
	}
}

func linuxProductionList(ctx context.Context, root, arch string, args ...string) *exec.Cmd {
	cmd := exec.CommandContext(ctx, "go", append([]string{"list", "-deps"}, args...)...)
	cmd.Dir = root
	cmd.Env = append(cmd.Environ(),
		"GOOS=linux", "GOARCH="+arch, "GOAMD64=v1", "GOARM64=v8.0", "CGO_ENABLED=0",
		"GOENV=off", "GOFLAGS=", "GO111MODULE=on", "GOWORK=off")
	cmd.WaitDelay = 2 * time.Second
	return cmd
}

func checkLinuxArchitecture(t *testing.T, arch string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	cmd := linuxProductionList(ctx, "../..", arch,
		"-json=ImportPath,Name,Module,GoFiles,CgoFiles,TestGoFiles,XTestGoFiles,Imports", "./...")

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
		packages[pkg.ImportPath] = pkg
	}
	for _, problem := range listenerBoundaryProblems(packages, registry) {
		t.Error(problem)
	}
	for _, problem := range capacityEvidenceBoundaryProblems(packages, registry) {
		t.Error(problem)
	}
	for _, problem := range reportPublicationBoundaryProblems(packages, registry) {
		t.Error(problem)
	}
	for _, problem := range architectureProblems(packages, registry) {
		t.Error(problem)
	}
	for _, problem := range measuredCutoverBoundaryProblems(packages, registry) {
		t.Error(problem)
	}
	t.Run("RetainedRuntimeWiring", func(t *testing.T) {
		required := retainedRuntimeConstructors(registry, reachableImports(packages, server))
		for _, problem := range retainedRuntimeWiringProblems(t, "../..", arch, required) {
			t.Error(problem)
		}
	})
}

// Check the cutover seams against the full production graph. The root-module
// edge registry cannot describe a path that traverses an external dependency.
func measuredCutoverBoundaryProblems(packages map[string]listedPackage, rules map[string]rule) []string {
	measured := make(map[string]rule, len(packages))
	for path, pkg := range packages {
		if pkg.production() {
			measured[path] = rule{role: rules[path].role, imports: pkg.Imports}
		}
	}
	problems := pagingBoundaryProblems(measured)
	problems = append(problems, storageAuthorityProblems(measured)...)
	problems = append(problems, serverCommandBoundaryProblems(measured)...)
	problems = append(problems, operatorBoundaryProblems(measured)...)
	// The exporter registry checks exact direct local imports. Follow the
	// full graph here to catch forbidden destinations via external packages.
	for _, path := range slices.Sorted(maps.Keys(registeredReachability(measured, exportCommand))) {
		if forbiddenExportDependency(path) {
			problems = append(problems, fmt.Sprintf("forbidden operator export dependency: %s reaches %s", exportCommand, path))
		}
	}
	problems = append(problems, releaseEvidenceBoundaryProblems(measured)...)
	problems = append(problems, streamPolicyProblems(measured)...)
	return append(problems, queryOracleProblems(measured)...)
}

func TestMeasuredCutoverBoundaries(t *testing.T) {
	t.Parallel()
	const (
		backup     = "bog/internal/backup"
		checkpoint = "bog/internal/checkpoint"
		history    = "bog/internal/history"
		httpapi    = "bog/internal/httpapi"
		readiness  = "bog/internal/readiness"
		store      = "bog/internal/store"
		wal        = "bog/internal/wal"
		relay      = "example.com/relay"
	)
	tests := []struct {
		name, consumer, target string
		testOnly               bool
		want                   []string
	}{
		{name: "command composes retained paging and WAL while the operator reads backup", consumer: server},
		{
			name:     "HTTP reaches history and checkpoint through an external relay",
			consumer: httpapi, target: history,
			want: []string{
				"HTTP must use the page source seam: " + httpapi + " reaches " + checkpoint,
				"HTTP must use the page source seam: " + httpapi + " reaches " + history,
				"forbidden retained storage composition: " + httpapi + " reaches " + wal + " and " + checkpoint + ", " + history + ". Compose in " + server,
			},
		},
		{
			name:     "backup reaches WAL through an external relay",
			consumer: backup, target: wal,
			want: []string{
				"forbidden storage authority dependency: " + backup + " reaches " + wal,
				"forbidden retained storage composition: " + operatorCommand + " reaches " + wal + " and " + backup + ". Compose in " + server,
				"forbidden operator verification dependency: " + operatorCommand + " reaches " + wal,
			},
		},
		{
			name:     "WAL reaches backup through an external relay",
			consumer: wal, target: backup,
			want: []string{
				"HTTP must use the page source seam: " + httpapi + " reaches " + backup,
				"forbidden storage authority dependency: " + wal + " reaches " + backup,
				"forbidden retained storage composition: " + httpapi + " reaches " + wal + " and " + backup + ". Compose in " + server,
				"forbidden retained storage composition: " + readiness + " reaches " + wal + " and " + backup + ". Compose in " + server,
			},
		},
		{
			name:     "release admission reaches history through an external relay",
			consumer: releaseVerifier, target: history,
			want: []string{
				"forbidden release evidence dependency: " + releaseVerifier + " reaches " + checkpoint,
				"forbidden release evidence dependency: " + releaseVerifier + " reaches " + history,
			},
		},
		{
			name:     "Linux durability producer reaches backup through an external relay",
			consumer: linuxVerifier, target: backup,
			want: []string{"forbidden release evidence dependency: " + linuxVerifier + " reaches " + backup},
		},
		{
			name:     "capacity producer reaches WAL through an external relay",
			consumer: retainedBench, target: wal,
			want: []string{"forbidden release evidence dependency: " + retainedBench + " reaches " + wal},
		},
		{
			name:     "operator reaches WAL through an external relay",
			consumer: operatorCommand, target: wal,
			want: []string{
				"forbidden retained storage composition: " + operatorCommand + " reaches " + wal + " and " + backup + ". Compose in " + server,
				"forbidden operator verification dependency: " + operatorCommand + " reaches " + wal,
			},
		},
		{
			name:     "export client reaches history through an external relay",
			consumer: goClient, target: history,
			want: []string{
				"forbidden operator export dependency: " + exportCommand + " reaches " + checkpoint,
				"forbidden operator export dependency: " + exportCommand + " reaches " + history,
				"forbidden release evidence dependency: " + releaseVerifier + " reaches " + checkpoint,
				"forbidden release evidence dependency: " + releaseVerifier + " reaches " + history,
			},
		},
		{
			name:     "operator reaches stream policy through an external relay",
			consumer: operatorCommand, target: streamPolicyVerifier,
			want: []string{"forbidden command dependency: " + operatorCommand + " reaches " + streamPolicyVerifier},
		},
		{
			name:     "stream policy reaches shared rows through an external relay",
			consumer: streamPolicyVerifier, target: store,
			want: []string{"forbidden stream policy dependency: " + streamPolicyVerifier + " reaches " + store},
		},
		{
			name:     "server reaches the oracle through an external relay",
			consumer: server, target: queryModel,
			want: []string{"forbidden server command dependency: " + server + " reaches " + queryModel},
		},
		{
			name:     "oracle reaches shared runtime rows through an external relay",
			consumer: queryModel, target: store,
			want: []string{"forbidden query oracle dependency: " + queryModel + " reaches " + store},
		},
		{
			name:     "release can use HTTP client and shared row types through an external relay",
			consumer: releaseVerifier, target: goClient,
		},
		{
			name:     "test-only relay supplies no production edge",
			consumer: httpapi, target: history, testOnly: true,
		},
		{
			name:     "release fixtures supply no production edge",
			consumer: releaseVerifier, target: backup, testOnly: true,
		},
		{
			name:     "export fixtures supply no production edge",
			consumer: exportCommand, target: history, testOnly: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			rules := map[string]rule{
				server:               {serverCommand, []string{backup, history, httpapi, readiness, wal}},
				operatorCommand:      {verifier, []string{backup, goClient, store}},
				exportCommand:        {verifier, []string{goClient}},
				releaseVerifier:      {verifier, []string{goClient}},
				linuxVerifier:        {role: verifier},
				retainedBench:        {role: verifier},
				queryModel:           {role: verifierModel},
				streamPolicyVerifier: {role: verifierModel},
				backup:               {runtimeLibrary, []string{store}},
				checkpoint:           {runtimeLibrary, []string{store}},
				history:              {runtimeLibrary, []string{checkpoint, store}},
				httpapi:              {runtimeLibrary, []string{store, wal}},
				readiness:            {runtimeLibrary, []string{store, wal}},
				wal:                  {runtimeLibrary, []string{store}},
				store:                {role: runtimeLibrary},
				goClient:             {standalone, []string{store}},
			}

			packages := make(map[string]listedPackage, len(rules)+1)
			for path, entry := range rules {
				packages[path] = listedPackage{GoFiles: []string{"package.go"}, Imports: entry.imports}
			}
			consumer := packages[tt.consumer]
			consumer.Imports = append(consumer.Imports, relay)
			packages[tt.consumer] = consumer
			pkg := listedPackage{GoFiles: []string{"relay.go"}}
			if tt.target != "" {
				pkg.Imports = []string{tt.target}
			}
			if tt.testOnly {
				pkg.GoFiles = nil
				pkg.TestGoFiles = []string{"relay_test.go"}
			}
			packages[relay] = pkg
			if got := measuredCutoverBoundaryProblems(packages, rules); !slices.Equal(got, tt.want) {
				t.Errorf("measuredCutoverBoundaryProblems() = %q, want %q", got, tt.want)
			}
		})
	}
}

func architectureProblems(packages map[string]listedPackage, rules map[string]rule) []string {
	var problems []string
	production := make(map[string][]string)
	local := make(map[string]listedPackage)
	for path, pkg := range packages {
		if pkg.Module == nil || !pkg.Module.Main {
			continue
		}
		local[path] = pkg
		if pkg.production() {
			production[path] = pkg.Imports
		}
	}

	reachable := reachableImports(packages, server)

	for _, path := range slices.Sorted(maps.Keys(rules)) {
		r := rules[path]
		pkg, exists := local[path]
		switch {
		case !exists:
			problems = append(problems, fmt.Sprintf("stale %s registration: %s is absent from the Linux inventory", r.role, path))
		case r.role == testOnly:
			if pkg.production() || len(pkg.TestGoFiles)+len(pkg.XTestGoFiles) == 0 {
				problems = append(problems, fmt.Sprintf("%s must contain Linux test files only", path))
			}
			if len(r.imports) != 0 {
				problems = append(problems, fmt.Sprintf("%s must register no production imports", path))
			}
		case !pkg.production():
			problems = append(problems, fmt.Sprintf("stale %s registration: %s has no Linux production files", r.role, path))
		default:
			wantMain := r.role == serverCommand || r.role == verifier
			if (pkg.Name == "main") != wantMain {
				problems = append(problems, fmt.Sprintf("%s has package name %q incompatible with its %s role", path, pkg.Name, r.role))
			}
			wantReachable := r.role == serverCommand || r.role == runtimeLibrary
			if reachable[path] != wantReachable {
				problems = append(problems, fmt.Sprintf("%s is registered as %s but reachability from %s is %t", path, r.role, server, reachable[path]))
			}
		}
	}

	for _, path := range slices.Sorted(maps.Keys(local)) {
		pkg := local[path]
		if _, registered := rules[path]; !registered && !pkg.production() && len(pkg.TestGoFiles)+len(pkg.XTestGoFiles) > 0 {
			problems = append(problems, "unregistered test-only package "+path)
		}
	}

	for _, path := range slices.Sorted(maps.Keys(production)) {
		r, registered := rules[path]
		if !registered {
			reach := "unreachable"
			if reachable[path] {
				reach = "reachable"
			}
			problems = append(problems, fmt.Sprintf("unregistered %s production package %s", reach, path))
		}
		imports := production[path]
		for _, imported := range imports {
			if _, isLocal := production[imported]; isLocal && !slices.Contains(r.imports, imported) {
				problems = append(problems, fmt.Sprintf("forbidden production import: %s -> %s", path, imported))
			}
		}
		registeredImports := make(map[string]bool, len(r.imports))
		for _, imported := range r.imports {
			if registeredImports[imported] {
				problems = append(problems, fmt.Sprintf("duplicate production import registration: %s -> %s", path, imported))
				continue
			}
			registeredImports[imported] = true
			if _, isLocal := production[imported]; !isLocal || !slices.Contains(imports, imported) {
				problems = append(problems, fmt.Sprintf("stale production import registration: %s -> %s", path, imported))
			}
		}
	}

	if cycle := importCycle(production); len(cycle) > 0 {
		problems = append(problems, "production import cycle: "+strings.Join(cycle, " -> "))
	}
	problems = append(problems, storageDirectionProblems(packages, production)...)
	return problems
}

func TestRetainedCommandImportRegistrations(t *testing.T) {
	t.Parallel()
	const (
		backup     = "bog/internal/backup"
		checkpoint = "bog/internal/checkpoint"
		history    = "bog/internal/history"
		store      = "bog/internal/store"
		wal        = "bog/internal/wal"
	)
	for _, tt := range []struct {
		name, importer string
		imports        []string
		want           []string
	}{
		{name: "checkpoint reaches the server through history"},
		{
			name:     "registry adds an unmeasured direct checkpoint edge",
			importer: server,
			imports:  []string{backup, checkpoint, history, wal},
			want:     []string{"stale production import registration: " + server + " -> " + checkpoint},
		},
		{
			name:     "registry omits a measured server import of backup",
			importer: server,
			imports:  []string{history, wal},
			want:     []string{"forbidden production import: " + server + " -> " + backup},
		},
		{
			name:     "registry duplicates the server backup edge",
			importer: server,
			imports:  []string{backup, history, wal, backup},
			want:     []string{"duplicate production import registration: " + server + " -> " + backup},
		},
		{
			name:     "registry duplicates the offline backup edge",
			importer: operatorCommand,
			imports:  []string{backup, goClient, store, backup},
			want:     []string{"duplicate production import registration: " + operatorCommand + " -> " + backup},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			rules := map[string]rule{
				server:          {serverCommand, []string{backup, history, wal}},
				operatorCommand: {verifier, []string{backup, goClient, store}},
				backup:          {runtimeLibrary, []string{store}},
				checkpoint:      {runtimeLibrary, []string{store}},
				history:         {runtimeLibrary, []string{checkpoint, store}},
				store:           {role: runtimeLibrary},
				wal:             {runtimeLibrary, []string{store}},
				goClient:        {standalone, []string{store}},
			}
			packages := make(map[string]listedPackage, len(rules))
			for path, entry := range rules {
				name := "library"
				if path == server || path == operatorCommand {
					name = "main"
				}
				packages[path] = productionPackage(name, entry.imports...)
			}
			if tt.importer != "" {
				entry := rules[tt.importer]
				entry.imports = tt.imports
				rules[tt.importer] = entry
			}
			if got := architectureProblems(packages, rules); !slices.Equal(got, tt.want) {
				t.Errorf("architectureProblems() = %q, want %q", got, tt.want)
			}
		})
	}
}

// Follow production imports through dependencies outside the root module too.
func reachableImports(packages map[string]listedPackage, root string) map[string]bool {
	reached := make(map[string]bool)
	var visit func(string)
	visit = func(path string) {
		if reached[path] {
			return
		}
		reached[path] = true
		for _, imported := range packages[path].Imports {
			visit(imported)
		}
	}
	visit(root)
	return reached
}

// Direction is an architectural invariant independent of the observed registry.
func storageDirectionProblems(packages map[string]listedPackage, production map[string][]string) []string {
	const (
		httpapi   = "bog/internal/httpapi"
		readiness = "bog/internal/readiness"
	)
	var problems []string
	for _, from := range []string{
		"bog/internal/backup",
		"bog/internal/checkpoint",
		"bog/internal/history",
		"bog/internal/idempotency",
		"bog/internal/store",
		"bog/internal/wal",
	} {
		if _, exists := production[from]; !exists {
			continue
		}
		reached := reachableImports(packages, from)
		for _, to := range []string{httpapi, readiness, goClient} {
			if reached[to] {
				problems = append(problems, fmt.Sprintf("forbidden storage dependency: %s -> %s", from, to))
			}
		}
	}
	// Readiness observes the active owner and its filesystem. The command
	// supplies the probe handle and owns retained storage composition.
	readyReachable := reachableImports(packages, readiness)
	for _, to := range []string{"bog/internal/backup", "bog/internal/checkpoint", "bog/internal/history"} {
		if slices.Contains(production[httpapi], to) {
			problems = append(problems, fmt.Sprintf("HTTP must use the page source seam: %s -> %s", httpapi, to))
		}
		if readyReachable[to] {
			problems = append(problems, fmt.Sprintf("forbidden readiness dependency: %s reaches %s", readiness, to))
		}
	}
	return problems
}

// go list also rejects cycles during loading. Keep the invariant explicit so it
// still applies to constructed graphs and covers packages outside the server.
func importCycle(graph map[string][]string) []string {
	active := make(map[string]int)
	done := make(map[string]bool)
	var stack []string
	var visit func(string) []string
	visit = func(path string) []string {
		if index, ok := active[path]; ok {
			return append(slices.Clone(stack[index:]), path)
		}
		imports, local := graph[path]
		if !local || done[path] {
			return nil
		}
		active[path] = len(stack)
		stack = append(stack, path)
		for _, imported := range imports {
			if cycle := visit(imported); cycle != nil {
				return cycle
			}
		}
		stack = stack[:len(stack)-1]
		delete(active, path)
		done[path] = true
		return nil
	}
	for _, path := range slices.Sorted(maps.Keys(graph)) {
		if cycle := visit(path); cycle != nil {
			return cycle
		}
	}
	return nil
}
