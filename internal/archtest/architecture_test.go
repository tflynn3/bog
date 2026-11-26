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
	server     = "bog/cmd/bog"
	httpapi    = "bog/internal/httpapi"
	readiness  = "bog/internal/readiness"
	store      = "bog/internal/store"
	wal        = "bog/internal/wal"
	checkpoint = "bog/internal/checkpoint"
	history    = "bog/internal/history"
	backup     = "bog/internal/backup"
	retained   = "bog/internal/retained"
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

// Record observed Linux production imports within the root module. The
// retained composition fixture covers the cutover contract until the command
// wiring lands. Promote roles and update edges in the wiring change.
var registry = map[string]rule{
	server:                       {serverCommand, []string{httpapi, readiness, store, wal}},
	httpapi:                      {runtimeLibrary, []string{store, wal}},
	readiness:                    {runtimeLibrary, []string{store, wal}},
	wal:                          {runtimeLibrary, []string{store}},
	store:                        {runtimeLibrary, nil},
	checkpoint:                   {staged, []string{store}},
	history:                      {staged, []string{checkpoint, store}},
	backup:                       {staged, []string{store}},
	"bog/internal/idempotency":   {staged, nil},
	"bog/internal/client":        {standalone, []string{"bog/internal/store"}},
	"bog/internal/ingest":        {testOnly, nil},
	"bog/internal/archtest":      {testOnly, nil},
	"bog/verify/release":         {verifier, nil},
	"bog/verify/linux":           {verifier, nil},
	"bog/bench/retained-history": {verifier, nil},
	"bog/verify/query-model":     {verifierModel, nil},
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
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, "go", "list", "-deps",
		"-json=ImportPath,Name,Module,GoFiles,CgoFiles,TestGoFiles,XTestGoFiles,Imports", "./...")
	cmd.Dir = "../.."
	cmd.Env = append(cmd.Environ(),
		"GOOS=linux", "GOARCH=amd64", "GOAMD64=v1", "CGO_ENABLED=0",
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

	// Walk all dependencies for reachability, even through packages outside
	// the root module. Only root-module packages need registry entries.
	reachable := make(map[string]bool)
	var visit func(string)
	visit = func(path string) {
		if reachable[path] {
			return
		}
		reachable[path] = true
		for _, imported := range packages[path].Imports {
			visit(imported)
		}
	}
	visit(server)

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
			if _, isLocal := production[imported]; !isLocal {
				continue
			}
			if forbiddenDirection(path, imported) || !slices.Contains(r.imports, imported) {
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
	return problems
}

// These boundaries survive registry updates. cmd/bog bridges the HTTP
// consumer interface to retained storage. The storage modules stay usable
// independently of the transport and the retained coordinator.
func forbiddenDirection(from, to string) bool {
	switch from {
	case httpapi, readiness:
		return to == retained || to == history || to == checkpoint || to == backup
	case store, wal, history, checkpoint, backup:
		return to == httpapi || to == readiness || to == retained
	case retained:
		return to == httpapi || to == readiness
	default:
		return false
	}
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
