package archtest

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"maps"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"
	"unicode"
	"unicode/utf8"
)

type verifierTestConsumer struct {
	packagePath string
	testName    string // When set, the support package provides this in-package test.
}

// Test ownership is separate from the production registry.
var verifierTestConsumers = map[string]verifierTestConsumer{
	queryModel:           {packagePath: releaseVerifier},
	streamPolicyVerifier: {packagePath: streamPolicyVerifier, testName: "TestReferenceScenario"},
}

type verifierTestPackage struct {
	ImportPath   string
	Dir          string
	Name         string
	TestGoFiles  []string
	TestImports  []string
	XTestImports []string
}

func TestVerifierSupportConsumers(t *testing.T) {
	t.Parallel()
	for _, arch := range []string{"amd64", "arm64"} {
		t.Run("linux_"+arch, func(t *testing.T) {
			t.Parallel()
			packages := listVerifierTestPackages(t, arch)
			for _, problem := range verifierConsumerProblems(registry, verifierTestConsumers, packages) {
				t.Error(problem)
			}
		})
	}
}

func listVerifierTestPackages(t *testing.T, arch string) map[string]verifierTestPackage {
	t.Helper()
	// Match TestLinuxArchitecture's build settings. Request test metadata
	// directly from the target packages without constructing test binaries.
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, "go", "list",
		"-json=ImportPath,Dir,Name,TestGoFiles,TestImports,XTestImports", "./...")
	cmd.Dir = "../.."
	cmd.Env = append(cmd.Environ(),
		"GOOS=linux", "GOARCH="+arch, "GOAMD64=v1", "GOARM64=v8.0", "CGO_ENABLED=0",
		"GOENV=off", "GOFLAGS=", "GO111MODULE=on", "GOWORK=off")
	cmd.WaitDelay = 2 * time.Second

	data, err := cmd.Output()
	if err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			t.Fatalf("list Linux test packages: %v\n%s", err, exitErr.Stderr)
		}
		t.Fatalf("list Linux test packages: %v", err)
	}

	packages := make(map[string]verifierTestPackage)
	decoder := json.NewDecoder(bytes.NewReader(data))
	for {
		var pkg verifierTestPackage
		err := decoder.Decode(&pkg)
		if err == io.EOF {
			return packages
		}
		if err != nil {
			t.Fatalf("decode Linux test package list: %v", err)
		}
		packages[pkg.ImportPath] = pkg
	}
}

// TestLinuxArchitecture validates the registered production edges on both
// Linux targets. Only evidence command roots establish production ownership.
func verifierConsumerProblems(rules map[string]rule, consumers map[string]verifierTestConsumer, packages map[string]verifierTestPackage) []string {
	reached := make(map[string]bool)
	for path, entry := range rules {
		if entry.role == verifier && path != operatorCommand {
			maps.Copy(reached, registeredReachability(rules, path))
		}
	}

	var problems []string
	for _, path := range slices.Sorted(maps.Keys(rules)) {
		if rules[path].role != verifierModel {
			continue
		}
		if _, named := consumers[path]; !reached[path] && !named {
			problems = append(problems, fmt.Sprintf("verifier support %s has no evidence command or named test consumer", path))
		}
	}
	for _, path := range slices.Sorted(maps.Keys(consumers)) {
		consumer := consumers[path]
		pkg, listed := packages[consumer.packagePath]
		_, supportListed := packages[path]
		prefix := "stale test consumer for verifier support " + path
		switch {
		case rules[path].role != verifierModel || !supportListed:
			problems = append(problems, prefix+": support package is absent or no longer registered as verifier support")
		case reached[path]:
			problems = append(problems, prefix+": an evidence command reaches the package in production")
		case !listed:
			problems = append(problems, fmt.Sprintf("%s: %s is absent from the Linux inventory", prefix, consumer.packagePath))
		case consumer.testName == "":
			if consumer.packagePath == path || !(slices.Contains(pkg.TestImports, path) || slices.Contains(pkg.XTestImports, path)) {
				problems = append(problems, fmt.Sprintf("%s: %s must be a separate package that imports it directly in tests", prefix, consumer.packagePath))
			}
		case consumer.packagePath != path:
			problems = append(problems, fmt.Sprintf("%s: %s must be declared in the support package", prefix, consumer.testName))
		default:
			found, err := hasInPackageVerifierTest(pkg, consumer.testName)
			if err != nil {
				problems = append(problems, fmt.Sprintf("%s: %v", prefix, err))
			} else if !found {
				problems = append(problems, fmt.Sprintf("%s: %s must declare runnable in-package %s", prefix, path, consumer.testName))
			}
		}
	}
	return problems
}

func hasInPackageVerifierTest(pkg verifierTestPackage, testName string) (bool, error) {
	fset := token.NewFileSet()
	for _, name := range pkg.TestGoFiles {
		file, err := parser.ParseFile(fset, filepath.Join(pkg.Dir, name), nil, parser.SkipObjectResolution)
		if err != nil {
			return false, fmt.Errorf("parse selected test source: %w", err)
		}
		if declaresVerifierTest(file, pkg.Name, testName) {
			return true, nil
		}
	}
	return false, nil
}

func declaresVerifierTest(file *ast.File, packageName, testName string) bool {
	if file.Name.Name != packageName {
		return false
	}
	// Match the Go test naming rule, including its Unicode handling.
	suffix, isTest := strings.CutPrefix(testName, "Test")
	first, _ := utf8.DecodeRuneInString(suffix)
	if !isTest || unicode.IsLower(first) {
		return false
	}

	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Name.Name != testName || fn.Recv != nil || fn.Body == nil ||
			fn.Type.TypeParams.NumFields() != 0 || fn.Type.Results.NumFields() != 0 ||
			fn.Type.Params.NumFields() != 1 {
			continue
		}
		ptr, ok := fn.Type.Params.List[0].Type.(*ast.StarExpr)
		if !ok {
			continue
		}
		for _, spec := range file.Imports {
			// Import paths can be quoted or raw string literals.
			path, err := strconv.Unquote(spec.Path.Value)
			if err != nil || path != "testing" {
				continue
			}
			name := "testing"
			if spec.Name != nil {
				name = spec.Name.Name
			}
			switch typ := ptr.X.(type) {
			case *ast.Ident:
				if name == "." && typ.Name == "T" {
					return true
				}
			case *ast.SelectorExpr:
				qualifier, ok := typ.X.(*ast.Ident)
				if ok && name != "_" && qualifier.Name == name && typ.Sel.Name == "T" {
					return true
				}
			}
		}
	}
	return false
}

func TestVerifierConsumerFixtures(t *testing.T) {
	t.Parallel()
	const (
		helper  = "bog/verify/helper"
		missing = "verifier support " + queryModel + " has no evidence command or named test consumer"
		stale   = "stale test consumer for verifier support " + queryModel + ": "
	)
	for _, tt := range []struct {
		name   string
		change func(map[string]rule, map[string]verifierTestConsumer, map[string]verifierTestPackage)
		want   []string
	}{
		{name: "release tests import the model"},
		{
			name: "external release tests import the model",
			change: func(_ map[string]rule, _ map[string]verifierTestConsumer, pkgs map[string]verifierTestPackage) {
				pkgs[releaseVerifier] = verifierTestPackage{XTestImports: []string{queryModel}}
			},
		},
		{
			name: "direct production consumer",
			change: func(rules map[string]rule, consumers map[string]verifierTestConsumer, _ map[string]verifierTestPackage) {
				delete(consumers, queryModel)
				rules[releaseVerifier] = rule{verifier, []string{queryModel}}
			},
		},
		{
			name: "transitive production consumer",
			change: func(rules map[string]rule, consumers map[string]verifierTestConsumer, _ map[string]verifierTestPackage) {
				delete(consumers, queryModel)
				rules[retainedBench] = rule{verifier, []string{helper}}
				rules[helper] = rule{verifierModel, []string{queryModel}}
			},
		},
		{
			name: "undeclared test consumer",
			change: func(_ map[string]rule, consumers map[string]verifierTestConsumer, _ map[string]verifierTestPackage) {
				delete(consumers, queryModel)
			},
			want: []string{missing},
		},
		{
			name: "unrooted production importer",
			change: func(rules map[string]rule, consumers map[string]verifierTestConsumer, _ map[string]verifierTestPackage) {
				delete(consumers, queryModel)
				rules[helper] = rule{verifierModel, []string{queryModel}}
			},
			want: []string{"verifier support " + helper + " has no evidence command or named test consumer", missing},
		},
		{
			name: "operator is outside the evidence roots",
			change: func(rules map[string]rule, consumers map[string]verifierTestConsumer, _ map[string]verifierTestPackage) {
				delete(consumers, queryModel)
				rules[operatorCommand] = rule{verifier, []string{queryModel}}
			},
			want: []string{missing},
		},
		{
			name: "declared test package disappeared",
			change: func(_ map[string]rule, _ map[string]verifierTestConsumer, pkgs map[string]verifierTestPackage) {
				delete(pkgs, releaseVerifier)
			},
			want: []string{stale + releaseVerifier + " is absent from the Linux inventory"},
		},
		{
			name: "only an unnamed test package imports the model",
			change: func(_ map[string]rule, _ map[string]verifierTestConsumer, pkgs map[string]verifierTestPackage) {
				pkgs[helper] = pkgs[releaseVerifier]
				pkgs[releaseVerifier] = verifierTestPackage{}
			},
			want: []string{stale + releaseVerifier + " must be a separate package that imports it directly in tests"},
		},
		{
			name: "removed support registration leaves stale test metadata",
			change: func(rules map[string]rule, _ map[string]verifierTestConsumer, _ map[string]verifierTestPackage) {
				delete(rules, queryModel)
			},
			want: []string{stale + "support package is absent or no longer registered as verifier support"},
		},
		{
			name: "new production consumer leaves stale test metadata",
			change: func(rules map[string]rule, _ map[string]verifierTestConsumer, _ map[string]verifierTestPackage) {
				rules[linuxVerifier] = rule{verifier, []string{queryModel}}
			},
			want: []string{stale + "an evidence command reaches the package in production"},
		},
		{
			name: "named scenario has no selected in-package test",
			change: func(rules map[string]rule, consumers map[string]verifierTestConsumer, pkgs map[string]verifierTestPackage) {
				rules[streamPolicyVerifier] = rule{role: verifierModel}
				consumers[streamPolicyVerifier] = verifierTestConsumers[streamPolicyVerifier]
				pkgs[streamPolicyVerifier] = verifierTestPackage{}
			},
			want: []string{"stale test consumer for verifier support " + streamPolicyVerifier + ": " +
				streamPolicyVerifier + " must declare runnable in-package TestReferenceScenario"},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			rules := map[string]rule{
				server:          {role: serverCommand},
				releaseVerifier: {role: verifier},
				queryModel:      {role: verifierModel},
			}
			consumers := map[string]verifierTestConsumer{queryModel: verifierTestConsumers[queryModel]}
			pkgs := map[string]verifierTestPackage{
				queryModel:      {},
				releaseVerifier: {TestImports: []string{queryModel}},
			}
			if tt.change != nil {
				tt.change(rules, consumers, pkgs)
			}
			requireValidCommandFixture(t, rules)
			if got := verifierConsumerProblems(rules, consumers, pkgs); !slices.Equal(got, tt.want) {
				t.Errorf("verifierConsumerProblems() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestInPackageVerifierTestDeclarations(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name, body string
		want       bool
	}{
		{"standard test", "package policy; import \"testing\"; func TestReferenceScenario(t *testing.T) {}", true},
		{"aliased import", "package policy; import ts `testing`; func TestReferenceScenario(*ts.T) {}", true},
		{"dot import", "package policy; import . \"testing\"; func TestReferenceScenario(*T) {}", true},
		{"external package", "package policy_test; import \"testing\"; func TestReferenceScenario(*testing.T) {}", false},
		{"renamed test", "package policy; import \"testing\"; func TestOther(*testing.T) {}", false},
		{"method", "package policy; import \"testing\"; func (fixture) TestReferenceScenario(*testing.T) {}", false},
		{"function variable", "package policy; import \"testing\"; var TestReferenceScenario = func(*testing.T) {}", false},
		{"wrong import", "package policy; import testing \"example.org/fake\"; func TestReferenceScenario(*testing.T) {}", false},
		{"wrong testing type", "package policy; import \"testing\"; func TestReferenceScenario(*testing.B) {}", false},
		{"non-pointer", "package policy; import \"testing\"; func TestReferenceScenario(testing.T) {}", false},
		{"extra argument", "package policy; import \"testing\"; func TestReferenceScenario(a, b *testing.T) {}", false},
		{"return value", "package policy; import \"testing\"; func TestReferenceScenario(*testing.T) int { return 0 }", false},
		{"generic test", "package policy; import \"testing\"; func TestReferenceScenario[A any](*testing.T) {}", false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			file, err := parser.ParseFile(token.NewFileSet(), "runner_test.go", tt.body, parser.SkipObjectResolution)
			if err != nil {
				t.Fatal(err)
			}
			if got := declaresVerifierTest(file, "policy", "TestReferenceScenario"); got != tt.want {
				t.Errorf("declaresVerifierTest() = %t, want %t", got, tt.want)
			}
		})
	}
}
