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
// Promote a retained library and remove its staging declaration once the
// composed command reaches it on both Linux targets and its criteria are met.
var registry = map[string]rule{
	operatorCommand:              {verifier, []string{"bog/internal/backup", goClient, "bog/internal/store"}},
	server:                       {serverCommand, []string{"bog/internal/httpapi", "bog/internal/readiness", "bog/internal/store", "bog/internal/wal"}},
	"bog/internal/httpapi":       {runtimeLibrary, []string{"bog/internal/store", "bog/internal/wal"}},
	"bog/internal/readiness":     {runtimeLibrary, []string{"bog/internal/store", "bog/internal/wal"}},
	"bog/internal/wal":           {runtimeLibrary, []string{"bog/internal/store"}},
	"bog/internal/store":         {runtimeLibrary, nil},
	"bog/internal/checkpoint":    {staged, []string{"bog/internal/store"}},
	"bog/internal/history":       {staged, []string{"bog/internal/checkpoint", "bog/internal/store"}},
	"bog/internal/backup":        {staged, []string{"bog/internal/store"}},
	"bog/internal/idempotency":   {staged, nil},
	goClient:                     {standalone, []string{"bog/internal/store"}},
	"bog/internal/ingest":        {testOnly, nil},
	"bog/internal/archtest":      {testOnly, nil},
	fuzzVerifier:                 {testOnly, nil},
	"bog/verify/release":         {verifier, []string{goClient, capacityEvidenceVerifier, listenerVerifier}},
	"bog/verify/linux":           {verifier, nil},
	"bog/bench/retained-history": {verifier, []string{capacityEvidenceVerifier, listenerVerifier}},
	capacityEvidenceVerifier:     {verifierModel, nil},
	listenerVerifier:             {verifierModel, nil},
	"bog/verify/query-model":     {verifierModel, nil},
	streamPolicyVerifier:         {verifierModel, nil},
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

func checkLinuxArchitecture(t *testing.T, arch string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, "go", "list", "-deps",
		"-json=ImportPath,Name,Module,GoFiles,CgoFiles,TestGoFiles,XTestGoFiles,Imports", "./...")
	cmd.Dir = "../.."
	cmd.Env = append(cmd.Environ(),
		"GOOS=linux", "GOARCH="+arch, "GOAMD64=v1", "GOARM64=v8.0", "CGO_ENABLED=0",
		"GOENV=off", "GOFLAGS=", "GO111MODULE=on", "GOWORK=off")
	cmd.WaitDelay = 2 * time.Second

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
	for _, problem := range architectureProblems(packages, registry) {
		t.Error(problem)
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
		for _, imported := range r.imports {
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
	// The command supplies the page source and the readiness directory handle.
	problems = append(problems, pagingBoundaryProblems(packages)...)
	readyReachable := reachableImports(packages, readiness)
	for _, to := range []string{"bog/internal/backup", "bog/internal/checkpoint", "bog/internal/history"} {
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
