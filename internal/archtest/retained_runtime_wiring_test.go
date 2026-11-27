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
	"strings"
	"testing"
	"time"
)

type retainedConstructors struct {
	directoryReadiness bool
	retainedPaging     bool
}

const retainedPagingConstructor = "bog/internal/httpapi.NewWithPageSourceAndQueryResources"

// Apply the guard even if the command wiring and role changes disagree.
// The inventory check separately reports the missing or premature promotion.
func retainedRuntimeConstructors(rules map[string]rule, reachable map[string]bool) retainedConstructors {
	wired := func(path string) bool {
		return rules[path].role == runtimeLibrary || reachable[path]
	}
	historyWired := wired("bog/internal/history")
	return retainedConstructors{
		directoryReadiness: historyWired || wired("bog/internal/checkpoint") || wired("bog/internal/backup"),
		retainedPaging:     historyWired,
	}
}

func retainedRuntimeWiringProblems(t *testing.T, root, arch string, required retainedConstructors) []string {
	t.Helper()
	if !required.directoryReadiness {
		return nil
	}
	root, err := filepath.Abs(root)
	if err != nil {
		t.Fatalf("resolve repository path: %v", err)
	}

	// Match TestLinuxArchitecture's target settings. Export data lets go/types
	// resolve the real import bindings without running or linking a Linux binary.
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	cmd := linuxProductionList(ctx, root, arch, "-export",
		"-json=ImportPath,Dir,GoFiles,Export", "./cmd/bog")
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
	info := types.Info{
		Uses:  make(map[*ast.Ident]types.Object),
		Types: make(map[ast.Expr]types.TypeAndValue),
	}
	if _, err := cfg.Check(server, fset, files, &info); err != nil {
		t.Fatalf("resolve %s constructor references: %v", server, err)
	}
	// Required constructors need a direct call. Keep scanning all typed uses
	// below so storing or passing a legacy constructor still fails the guard.
	called := make(map[*ast.Ident]*ast.CallExpr)
	for _, file := range files {
		for node := range ast.Preorder(file) {
			call, ok := node.(*ast.CallExpr)
			if !ok {
				continue
			}
			switch fun := ast.Unparen(call.Fun).(type) {
			case *ast.Ident:
				called[fun] = call
			case *ast.SelectorExpr:
				called[fun.Sel] = call
			}
		}
	}
	missing := map[string]struct{}{
		"bog/internal/readiness.NewHandlerForDirectory": {},
	}
	if required.retainedPaging {
		missing[retainedPagingConstructor] = struct{}{}
	}
	var problems []string
	for _, ident := range slices.SortedFunc(maps.Keys(info.Uses), func(a, b *ast.Ident) int {
		return int(a.Pos() - b.Pos())
	}) {
		constructor, ok := info.Uses[ident].(*types.Func)
		if !ok || constructor.Pkg() == nil || constructor.Parent() != constructor.Pkg().Scope() {
			continue
		}
		path, name := constructor.Pkg().Path(), constructor.Name()
		if path == "bog/internal/httpapi" && !required.retainedPaging {
			continue
		}
		qualifiedName := path + "." + name
		call := called[ident]
		if call != nil {
			delete(missing, qualifiedName)
		}
		var problem string
		pos := ident.Pos()
		switch {
		case call != nil && qualifiedName == retainedPagingConstructor:
			arg := call.Args[0]
			source := info.TypeOf(arg)
			// A multi-result call can supply all of the constructor's arguments.
			if tuple, ok := source.(*types.Tuple); ok {
				source = tuple.At(0).Type()
			}
			if !implementsRetainedPageSource(t, constructor.Pkg(), source) {
				pos = arg.Pos()
				problem = fmt.Sprintf("retained runtime source passed to httpapi.%s must statically implement both httpapi.PageSource and httpapi.PageSourceScanBound (got %s)",
					name, types.TypeString(source, (*types.Package).Name))
			}
		default:
			if replacement := retainedConstructorReplacement(path, name); replacement != "" {
				problem = fmt.Sprintf("retained runtime must use %s instead of %s.%s",
					replacement, constructor.Pkg().Name(), name)
			}
		}
		if problem == "" {
			continue
		}
		position := fset.PositionFor(pos, false)
		file, err := filepath.Rel(root, position.Filename)
		if err != nil {
			t.Fatalf("resolve constructor source path: %v", err)
		}
		problems = append(problems, fmt.Sprintf("%s:%d:%d: %s", file, position.Line, position.Column, problem))
	}
	for _, constructor := range slices.Sorted(maps.Keys(missing)) {
		problems = append(problems, fmt.Sprintf("cmd/bog (linux/%s): retained runtime must call %s",
			arch, strings.TrimPrefix(constructor, "bog/internal/")))
	}
	return problems
}

func implementsRetainedPageSource(t *testing.T, httpapi *types.Package, source types.Type) bool {
	t.Helper()
	for _, name := range []string{"PageSource", "PageSourceScanBound"} {
		decl, ok := httpapi.Scope().Lookup(name).(*types.TypeName)
		if !ok {
			t.Fatalf("missing type httpapi.%s", name)
		}
		iface, ok := decl.Type().Underlying().(*types.Interface)
		if !ok {
			t.Fatalf("httpapi.%s must be an interface", name)
		}
		if !types.Implements(source, iface) {
			return false
		}
	}
	return true
}

func retainedConstructorReplacement(path, name string) string {
	switch path {
	case "bog/internal/httpapi":
		switch name {
		case "New", "NewWithQueryMaxResponseBytes", "NewWithQueryLimits", "NewWithQueryResources", "NewWithPageSource":
			return "httpapi.NewWithPageSourceAndQueryResources"
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
	productionInternal, err := filepath.Abs("..")
	if err != nil {
		t.Fatal(err)
	}
	const (
		checkpoint = "bog/internal/checkpoint"
		history    = "bog/internal/history"
		backup     = "bog/internal/backup"
		pageSource = `package main
import "context"
import api "bog/internal/httpapi"
import "bog/internal/store"

type retainedSource struct{}
func (retainedSource) Page(context.Context, api.PageRequest, *store.QueryWork) (api.PageResult, error) {
	return api.PageResult{}, nil
}
func (retainedSource) Release(string) error { return nil }
func (retainedSource) PageScanBytes(api.PageRequest) uint64 { return 4096 }

var _ api.PageSourceScanBound = retainedSource{}

func retainedAPI() *api.API {
	return (api.NewWithPageSourceAndQueryResources)(retainedSource{}, nil, 1024, 2, 100,
		api.QueryResourceLimits{ResponseBytes: 2048, ScanBytes: 8192, Captures: 2})
}
`
		dotImportedPageSource = `package main
import . "bog/internal/httpapi"

type boundedSource interface {
	PageSource
	PageSourceScanBound
}

func newBoundedSource() boundedSource { return retainedSource{} }

func factoryAPI() *API {
	return NewWithPageSourceAndQueryResources(newBoundedSource(), nil, 1024, 2, 100,
		QueryResourceLimits{ResponseBytes: 2048, ScanBytes: 8192, Captures: 2})
}
`
		unboundedPageSource = `package main
import . "bog/internal/httpapi"
import "bog/internal/store"

type unboundedSource struct{ PageSource }

func unboundedAPI() *API {
	return NewWithPageSourceAndQueryResources(unboundedSource{}, nil, 1024, 2, 100, QueryResourceLimits{})
}

func pagingArgs() (PageSource, interface{ Append([]byte, []store.Row) error }, int, int, uint64, QueryResourceLimits) {
	return retainedSource{}, nil, 1024, 2, 100, QueryResourceLimits{}
}

func erasedAPI() *API {
	return NewWithPageSourceAndQueryResources(pagingArgs())
}
`
		directoryHandler = `package main
import "context"
import "net/http"
import "os"
import ready "bog/internal/readiness"
func directoryProbe(dir *os.File, log ready.Appender, newAPI func(ready.Appender) http.Handler) http.Handler {
	return ready.NewHandlerForDirectory(context.Background(), dir, log, newAPI)
}
`
		dotImportedDirectoryHandler = `package main
import "context"
import "net/http"
import "os"
import . "bog/internal/readiness"
func directoryProbe(dir *os.File, log Appender, newAPI func(Appender) http.Handler) http.Handler {
	return NewHandlerForDirectory(context.Background(), dir, log, newAPI)
}
`
		pagingReference = `package main
import api "bog/internal/httpapi"
var _ = api.NewWithPageSourceAndQueryResources
`
		readinessReference = `package main
import ready "bog/internal/readiness"
var _ = ready.NewHandlerForDirectory
`
		compatibilityPaging = `package main
import . "bog/internal/httpapi"
func compatibilityAPI() *API {
	return NewWithPageSource(nil, nil, 1024, 2, 100)
}
`
	)
	legacyWiring := map[string]string{"legacy.go": `package main
import "bog/internal/httpapi"
import "bog/internal/readiness"
var _ = httpapi.NewWithQueryLimits
var _ = readiness.NewHandler
`}
	pinnedReadinessWithLegacyPaging := map[string]string{
		"readiness.go": directoryHandler,
		"paging.go": `package main
import "bog/internal/httpapi"
var _ = httpapi.NewWithQueryResources
`,
		"compatibility.go": compatibilityPaging,
	}
	retainedWiring := map[string]string{
		"readiness.go": directoryHandler,
		"paging.go":    pageSource,
	}
	const (
		legacyReadiness = "cmd/bog/legacy.go:5:19: retained runtime must use readiness.NewHandlerForDirectory instead of readiness.NewHandler"
		legacyPaging    = "cmd/bog/legacy.go:4:17: retained runtime must use httpapi.NewWithPageSourceAndQueryResources instead of httpapi.NewWithQueryLimits"
	)
	allRetained := []string{checkpoint, history, backup}
	splitWiring := map[string]string{
		"paging_linux_amd64.go":        pageSource,
		"paging_ref_linux_arm64.go":    pagingReference,
		"readiness_linux_arm64.go":     dotImportedDirectoryHandler,
		"readiness_ref_linux_amd64.go": readinessReference,
		"readiness_linux_test.go":      directoryHandler,
		"readiness_darwin.go":          directoryHandler,
		"paging_tagged.go":             "//go:build retained_wiring_fixture\n\n" + pageSource,
	}
	mixedScanBoundWiring := map[string]string{
		"paging.go":             pageSource,
		"paging_linux_arm64.go": unboundedPageSource,
		"readiness.go":          directoryHandler,
	}
	for _, tt := range []struct {
		name      string
		arch      string
		wired     []string
		reachable []string
		sources   map[string]string
		want      []string
	}{
		{
			name:    "staged baseline permits legacy constructors",
			arch:    "amd64",
			sources: legacyWiring,
		},
		{
			name:      "observed backup wiring requires directory readiness only",
			arch:      "amd64",
			reachable: []string{backup},
			sources:   legacyWiring,
			want: []string{
				legacyReadiness,
				"cmd/bog (linux/amd64): retained runtime must call readiness.NewHandlerForDirectory",
			},
		},
		{
			name:    "checkpoint only rejects path readiness",
			arch:    "arm64",
			wired:   []string{checkpoint},
			sources: legacyWiring,
			want: []string{
				legacyReadiness,
				"cmd/bog (linux/arm64): retained runtime must call readiness.NewHandlerForDirectory",
			},
		},
		{
			name:    "checkpoint only permits legacy paging with pinned readiness",
			arch:    "arm64",
			wired:   []string{checkpoint},
			sources: pinnedReadinessWithLegacyPaging,
		},
		{
			name:    "backup only rejects path readiness",
			arch:    "amd64",
			wired:   []string{backup},
			sources: legacyWiring,
			want: []string{
				legacyReadiness,
				"cmd/bog (linux/amd64): retained runtime must call readiness.NewHandlerForDirectory",
			},
		},
		{
			name:    "backup only permits legacy paging with pinned readiness",
			arch:    "amd64",
			wired:   []string{backup},
			sources: pinnedReadinessWithLegacyPaging,
		},
		{
			name:    "history activates pinned readiness on its own",
			arch:    "amd64",
			wired:   []string{history},
			sources: map[string]string{"paging.go": pageSource},
			want:    []string{"cmd/bog (linux/amd64): retained runtime must call readiness.NewHandlerForDirectory"},
		},
		{
			name:    "history and checkpoint reject legacy constructors with backup staged",
			arch:    "arm64",
			wired:   []string{history, checkpoint},
			sources: legacyWiring,
			want: []string{
				legacyPaging,
				legacyReadiness,
				"cmd/bog (linux/arm64): retained runtime must call httpapi.NewWithPageSourceAndQueryResources",
				"cmd/bog (linux/arm64): retained runtime must call readiness.NewHandlerForDirectory",
			},
		},
		{
			name:      "observed history wiring activates both guards with stale staged roles",
			arch:      "arm64",
			reachable: []string{history, checkpoint},
			sources:   legacyWiring,
			want: []string{
				legacyPaging,
				legacyReadiness,
				"cmd/bog (linux/arm64): retained runtime must call httpapi.NewWithPageSourceAndQueryResources",
				"cmd/bog (linux/arm64): retained runtime must call readiness.NewHandlerForDirectory",
			},
		},
		{
			name:    "history and checkpoint accept aliased constructor calls in command wrappers",
			arch:    "arm64",
			wired:   []string{history, checkpoint},
			sources: retainedWiring,
		},
		{
			name:      "observed history accepts a dot-imported call with a composed factory result",
			arch:      "amd64",
			reachable: []string{history},
			sources: map[string]string{
				"paging.go":    pageSource,
				"factory.go":   dotImportedPageSource,
				"readiness.go": dotImportedDirectoryHandler,
			},
		},
		{
			name:    "amd64 ignores arm64 sources missing the scan bound",
			arch:    "amd64",
			wired:   []string{history},
			sources: mixedScanBoundWiring,
		},
		{
			name:      "observed history checks every arm64 call including multi-result arguments",
			arch:      "arm64",
			reachable: []string{history},
			sources:   mixedScanBoundWiring,
			want: []string{
				"cmd/bog/paging_linux_arm64.go:8:44: retained runtime source passed to httpapi.NewWithPageSourceAndQueryResources must statically implement both httpapi.PageSource and httpapi.PageSourceScanBound (got main.unboundedSource)",
				"cmd/bog/paging_linux_arm64.go:16:44: retained runtime source passed to httpapi.NewWithPageSourceAndQueryResources must statically implement both httpapi.PageSource and httpapi.PageSourceScanBound (got httpapi.PageSource)",
			},
		},
		{
			name:  "compatibility paging call leaves the explicit constructor missing",
			arch:  "arm64",
			wired: []string{history},
			sources: map[string]string{
				"paging.go":    compatibilityPaging,
				"readiness.go": directoryHandler,
			},
			want: []string{
				"cmd/bog/paging.go:4:9: retained runtime must use httpapi.NewWithPageSourceAndQueryResources instead of httpapi.NewWithPageSource",
				"cmd/bog (linux/arm64): retained runtime must call httpapi.NewWithPageSourceAndQueryResources",
			},
		},
		{
			name:    "amd64 requires directory readiness in selected production sources",
			arch:    "amd64",
			wired:   allRetained,
			sources: splitWiring,
			want:    []string{"cmd/bog (linux/amd64): retained runtime must call readiness.NewHandlerForDirectory"},
		},
		{
			name:    "arm64 requires retained paging in selected production sources",
			arch:    "arm64",
			wired:   allRetained,
			sources: splitWiring,
			want:    []string{"cmd/bog (linux/arm64): retained runtime must call httpapi.NewWithPageSourceAndQueryResources"},
		},
		{
			name:  "unused required constructor references leave both calls missing",
			arch:  "amd64",
			wired: allRetained,
			sources: map[string]string{
				"paging.go":    pagingReference,
				"readiness.go": readinessReference,
			},
			want: []string{
				"cmd/bog (linux/amd64): retained runtime must call httpapi.NewWithPageSourceAndQueryResources",
				"cmd/bog (linux/amd64): retained runtime must call readiness.NewHandlerForDirectory",
			},
		},
		{
			name:  "calls through function variables do not count as direct calls",
			arch:  "arm64",
			wired: allRetained,
			sources: map[string]string{"indirect.go": `package main
import "context"
import api "bog/internal/httpapi"
import ready "bog/internal/readiness"
func pass[T any](constructor T) T { return constructor }
func handlers() {
	paging := api.NewWithPageSourceAndQueryResources
	probe := ready.NewHandlerForDirectory
	paging(nil, nil, 1024, 2, 100, api.QueryResourceLimits{})
	pass(api.NewWithPageSourceAndQueryResources)(nil, nil, 1024, 2, 100, api.QueryResourceLimits{})
	probe(context.Background(), nil, nil, nil)
}
`},
			want: []string{
				"cmd/bog (linux/arm64): retained runtime must call httpapi.NewWithPageSourceAndQueryResources",
				"cmd/bog (linux/arm64): retained runtime must call readiness.NewHandlerForDirectory",
			},
		},
		{
			name:  "same-named functions and methods leave both constructors missing",
			arch:  "arm64",
			wired: allRetained,
			sources: map[string]string{"lookalikes.go": `package main
import httpapi "bog/cmd/bog/lookalike"
type probe struct{}
func (probe) NewHandlerForDirectory() {}
func lookalikes() {
	httpapi.NewWithPageSourceAndQueryResources()
	probe.NewHandlerForDirectory(probe{})
}
`},
			want: []string{
				"cmd/bog (linux/arm64): retained runtime must call httpapi.NewWithPageSourceAndQueryResources",
				"cmd/bog (linux/arm64): retained runtime must call readiness.NewHandlerForDirectory",
			},
		},
		{
			name:  "retained constructors and selected production sources",
			arch:  "amd64",
			wired: allRetained,
			sources: map[string]string{
				"paging_linux_amd64.go":    pageSource,
				"readiness_linux_amd64.go": directoryHandler,
				"legacy_linux_arm64.go": `package main
import "bog/internal/httpapi"
var _ = httpapi.NewWithQueryResources
`,
				"legacy_linux_test.go": `package main
import "bog/internal/httpapi"
var _ = httpapi.NewWithPageSource
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
			name:  "legacy aliases and dot imports still fail when both required constructors are present",
			arch:  "arm64",
			wired: allRetained,
			sources: map[string]string{
				"paging_linux_arm64.go":    pageSource,
				"readiness_linux_arm64.go": directoryHandler,
				"legacy_linux_arm64.go": `package main
import api "bog/internal/httpapi"
import . "bog/internal/readiness"
var _ = api.New
var _ = api.NewWithQueryMaxResponseBytes(nil, nil, 1024)
var _ = api.NewWithQueryLimits
var _ = NewHandler
var _ = api.NewWithQueryResources(nil, nil, 1024, 2, 100, api.QueryResourceLimits{})
var _ = api.NewWithPageSource
`,
			},
			want: []string{
				"cmd/bog/legacy_linux_arm64.go:4:13: retained runtime must use httpapi.NewWithPageSourceAndQueryResources instead of httpapi.New",
				"cmd/bog/legacy_linux_arm64.go:5:13: retained runtime must use httpapi.NewWithPageSourceAndQueryResources instead of httpapi.NewWithQueryMaxResponseBytes",
				"cmd/bog/legacy_linux_arm64.go:6:13: retained runtime must use httpapi.NewWithPageSourceAndQueryResources instead of httpapi.NewWithQueryLimits",
				"cmd/bog/legacy_linux_arm64.go:7:9: retained runtime must use readiness.NewHandlerForDirectory instead of readiness.NewHandler",
				"cmd/bog/legacy_linux_arm64.go:8:13: retained runtime must use httpapi.NewWithPageSourceAndQueryResources instead of httpapi.NewWithQueryResources",
				"cmd/bog/legacy_linux_arm64.go:9:13: retained runtime must use httpapi.NewWithPageSourceAndQueryResources instead of httpapi.NewWithPageSource",
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			// Compile against the production packages so the command fixtures
			// exercise the real constructor signatures for the selected target.
			if err := os.Symlink(productionInternal, filepath.Join(root, "internal")); err != nil {
				t.Fatal(err)
			}
			fixture := map[string]string{
				"go.mod": "module bog\n\ngo 1.27\n",
				"cmd/bog/main.go": `package main
func main() {
	httpapi := struct{ New func() }{}
	_ = httpapi.New
}
`,
				"cmd/bog/lookalike/http.go": `package httpapi
func NewWithPageSourceAndQueryResources() {}
`,
			}
			for name, source := range tt.sources {
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
				checkpoint: {role: staged},
				history:    {role: staged},
				backup:     {role: staged},
			}
			for _, path := range tt.wired {
				rules[path] = rule{role: runtimeLibrary}
			}
			reachable := make(map[string]bool, len(tt.reachable))
			for _, path := range tt.reachable {
				reachable[path] = true
			}
			required := retainedRuntimeConstructors(rules, reachable)
			if got := retainedRuntimeWiringProblems(t, root, tt.arch, required); !slices.Equal(got, tt.want) {
				t.Errorf("constructor diagnostics = %q, want %q", got, tt.want)
			}
		})
	}
}
