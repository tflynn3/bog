package archtest

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"go/ast"
	"go/importer"
	"go/parser"
	"go/token"
	"go/types"
	"io"
	"maps"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"testing"
	"time"
)

func TestRetainedRuntimeWiring(t *testing.T) {
	t.Parallel()
	for _, arch := range []string{"amd64", "arm64"} {
		t.Run("linux_"+arch, func(t *testing.T) {
			t.Parallel()
			for _, problem := range retainedRuntimeWiringProblems(t, "../..", arch, registry) {
				t.Error(problem)
			}
		})
	}
}

func retainedRuntimeWiringProblems(t *testing.T, root, arch string, rules map[string]rule) []string {
	t.Helper()
	for _, path := range []string{"bog/internal/checkpoint", "bog/internal/history", "bog/internal/backup"} {
		if rules[path].role != runtimeLibrary {
			return nil
		}
	}
	root, err := filepath.Abs(root)
	if err != nil {
		t.Fatalf("resolve repository path: %v", err)
	}

	// Match TestLinuxArchitecture's target settings. Export data lets go/types
	// resolve the real import bindings without running or linking a Linux binary.
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, "go", "list", "-deps", "-export",
		"-json=ImportPath,Dir,GoFiles,Export", "./cmd/bog")
	cmd.Dir = root
	cmd.Env = append(cmd.Environ(),
		"GOOS=linux", "GOARCH="+arch, "GOAMD64=v1", "GOARM64=v8.0", "CGO_ENABLED=0",
		"GOENV=off", "GOFLAGS=", "GO111MODULE=on", "GOWORK=off")
	cmd.WaitDelay = 2 * time.Second
	data, err := cmd.Output()
	if err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			t.Fatalf("list %s Linux production sources: %v\n%s", server, err, exitErr.Stderr)
		}
		t.Fatalf("list %s Linux production sources: %v", server, err)
	}

	var commandSources []string
	exports := make(map[string]string)
	decoder := json.NewDecoder(bytes.NewReader(data))
	for {
		var pkg struct {
			ImportPath string
			Dir        string
			GoFiles    []string
			Export     string
		}
		err := decoder.Decode(&pkg)
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("decode Linux production sources: %v", err)
		}
		if pkg.ImportPath == server {
			for _, name := range pkg.GoFiles {
				commandSources = append(commandSources, filepath.Join(pkg.Dir, name))
			}
		}
		exports[pkg.ImportPath] = pkg.Export
	}
	if len(commandSources) == 0 {
		t.Fatalf("%s has no Linux production Go files", server)
	}

	fset := token.NewFileSet()
	files := make([]*ast.File, 0, len(commandSources))
	for _, name := range commandSources {
		file, err := parser.ParseFile(fset, name, nil, parser.SkipObjectResolution)
		if err != nil {
			t.Fatalf("parse production source: %v", err)
		}
		files = append(files, file)
	}

	cfg := types.Config{
		Sizes: types.SizesFor("gc", arch),
		Importer: importer.ForCompiler(fset, "gc", func(path string) (io.ReadCloser, error) {
			if exports[path] == "" {
				return nil, fmt.Errorf("missing Linux export data for %s", path)
			}
			return os.Open(exports[path])
		}),
	}
	uses := make(map[*ast.Ident]types.Object)
	if _, err := cfg.Check(server, fset, files, &types.Info{Uses: uses}); err != nil {
		t.Fatalf("resolve %s constructor references: %v", server, err)
	}
	var problems []string
	for _, ident := range slices.SortedFunc(maps.Keys(uses), func(a, b *ast.Ident) int {
		return int(a.Pos() - b.Pos())
	}) {
		constructor, ok := uses[ident].(*types.Func)
		if !ok || constructor.Pkg() == nil || constructor.Parent() != constructor.Pkg().Scope() {
			continue
		}
		replacement := retainedConstructorReplacement(constructor.Pkg().Path(), constructor.Name())
		if replacement == "" {
			continue
		}
		pos := fset.PositionFor(ident.Pos(), false)
		name, err := filepath.Rel(root, pos.Filename)
		if err != nil {
			t.Fatalf("resolve constructor source path: %v", err)
		}
		problems = append(problems, fmt.Sprintf("%s:%d:%d: retained runtime must use %s instead of %s.%s",
			name, pos.Line, pos.Column, replacement, constructor.Pkg().Name(), constructor.Name()))
	}
	return problems
}

func retainedConstructorReplacement(path, name string) string {
	switch path {
	case "bog/internal/httpapi":
		switch name {
		case "New", "NewWithQueryMaxResponseBytes", "NewWithQueryLimits":
			return "httpapi.NewWithPageSource"
		}
	case "bog/internal/readiness":
		if name == "NewHandler" {
			return "readiness.NewHandlerForDirectory"
		}
	}
	return ""
}

func TestRetainedRuntimeWiringFixtures(t *testing.T) {
	t.Parallel()
	const (
		checkpoint = "bog/internal/checkpoint"
		history    = "bog/internal/history"
		backup     = "bog/internal/backup"
	)
	legacyWiring := map[string]string{"legacy.go": `package main
import "bog/internal/httpapi"
import "bog/internal/readiness"
var _ = httpapi.NewWithQueryLimits
var _ = readiness.NewHandler
`}
	for _, tt := range []struct {
		name   string
		arch   string
		staged []string
		legacy map[string]string
		want   []string
	}{
		{
			name:   "staged baseline permits legacy constructors",
			arch:   "amd64",
			staged: []string{checkpoint, history, backup},
			legacy: legacyWiring,
		},
		{
			name:   "staged checkpoint delays activation",
			arch:   "arm64",
			staged: []string{checkpoint},
			legacy: legacyWiring,
		},
		{
			name:   "staged history delays activation",
			arch:   "amd64",
			staged: []string{history},
			legacy: legacyWiring,
		},
		{
			name:   "staged backup delays activation",
			arch:   "arm64",
			staged: []string{backup},
			legacy: legacyWiring,
		},
		{
			name: "retained constructors and selected production sources",
			arch: "amd64",
			legacy: map[string]string{
				"legacy_linux_arm64.go": `package main
import "bog/internal/httpapi"
var _ = httpapi.New
`,
				"legacy_linux_test.go": `package main
import "bog/internal/readiness"
var _ = readiness.NewHandler
`,
				"legacy_darwin_amd64.go": `package main
import "bog/internal/readiness"
var _ = readiness.NewHandler
`,
				"legacy_tagged.go": `//go:build retained_wiring_fixture

package main
import "bog/internal/readiness"
var _ = readiness.NewHandler
`,
			},
		},
		{
			name: "aliases and dot imports on Linux arm64",
			arch: "arm64",
			legacy: map[string]string{"legacy_linux_arm64.go": `package main
import api "bog/internal/httpapi"
import . "bog/internal/readiness"
var _ = api.New
var _ = api.NewWithQueryMaxResponseBytes()
var _ = api.NewWithQueryLimits
var _ = NewHandler
`},
			want: []string{
				"cmd/bog/legacy_linux_arm64.go:4:13: retained runtime must use httpapi.NewWithPageSource instead of httpapi.New",
				"cmd/bog/legacy_linux_arm64.go:5:13: retained runtime must use httpapi.NewWithPageSource instead of httpapi.NewWithQueryMaxResponseBytes",
				"cmd/bog/legacy_linux_arm64.go:6:13: retained runtime must use httpapi.NewWithPageSource instead of httpapi.NewWithQueryLimits",
				"cmd/bog/legacy_linux_arm64.go:7:9: retained runtime must use readiness.NewHandlerForDirectory instead of readiness.NewHandler",
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			fixture := map[string]string{
				"go.mod": "module bog\n\ngo 1.27\n",
				"cmd/bog/main.go": `package main
import (
	"bog/internal/httpapi"
	probe "bog/internal/readiness"
)
func main() {
	httpapi.NewWithPageSource()
	probe.NewHandlerForDirectory()
	httpapi := struct{ New func() }{}
	_ = httpapi.New
}
`,
				"internal/httpapi/http.go": `package httpapi
func New() {}
func NewWithQueryMaxResponseBytes() int { return 0 }
func NewWithQueryLimits() {}
func NewWithPageSource() {}
`,
				"internal/readiness/ready.go": `package readiness
func NewHandler() {}
func NewHandlerForDirectory() {}
`,
			}
			for name, source := range tt.legacy {
				fixture[filepath.Join("cmd/bog", name)] = source
			}
			for path, source := range fixture {
				name := filepath.Join(root, path)
				if err := os.MkdirAll(filepath.Dir(name), 0o755); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(name, []byte(source), 0o644); err != nil {
					t.Fatal(err)
				}
			}
			rules := map[string]rule{
				checkpoint: {role: runtimeLibrary},
				history:    {role: runtimeLibrary},
				backup:     {role: runtimeLibrary},
			}
			for _, path := range tt.staged {
				rules[path] = rule{role: staged}
			}
			if got := retainedRuntimeWiringProblems(t, root, tt.arch, rules); !slices.Equal(got, tt.want) {
				t.Errorf("constructor diagnostics = %q, want %q", got, tt.want)
			}
		})
	}
}
