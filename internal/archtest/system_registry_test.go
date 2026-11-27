package archtest

import (
	"fmt"
	"maps"
	"os"
	"regexp"
	"slices"
	"strings"
	"testing"
)

func TestSystemPackageRegistry(t *testing.T) {
	t.Parallel()
	const path = "../../docs/architecture/SYSTEM.md"
	doc, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, problem := range systemRegistryProblems(string(doc), registry, stagedPackages) {
		t.Errorf("%s: %s", path, problem)
	}
}

const packageRegistryHeading = "## Package registry"

var (
	packageRegistryHeader    = regexp.MustCompile(`^\|\s*Package or entry point\s*\|\s*Role\s*\|\s*Responsibility\s*\|$`)
	packageRegistryDelimiter = regexp.MustCompile(`^\|\s*:?-+:?\s*\|\s*:?-+:?\s*\|\s*:?-+:?\s*\|$`)
)

// Read just the package registry table. Other tables, diagrams, and prose
// can discuss packages without declaring their current roles.
func systemPackageRoles(doc string) (map[string][]string, error) {
	lines := strings.Split(doc, "\n")
	section := slices.IndexFunc(lines, func(line string) bool {
		return strings.TrimSpace(line) == packageRegistryHeading
	})
	if section < 0 {
		return nil, fmt.Errorf("missing %s heading", packageRegistryHeading)
	}
	for i := section + 1; i < len(lines); i++ {
		line := strings.TrimSpace(lines[i])
		if strings.HasPrefix(line, "# ") || strings.HasPrefix(line, "## ") {
			break
		}
		if !packageRegistryHeader.MatchString(line) {
			continue
		}
		if i+1 == len(lines) || !packageRegistryDelimiter.MatchString(strings.TrimSpace(lines[i+1])) {
			return nil, fmt.Errorf("package registry table at line %d has no valid delimiter row", i+1)
		}
		roles := make(map[string][]string)
		for j := i + 2; j < len(lines); j++ {
			line := strings.TrimSpace(lines[j])
			if !strings.HasPrefix(line, "|") {
				break
			}
			cells := strings.SplitN(strings.TrimSuffix(line[1:], "|"), "|", 3)
			if len(cells) != 3 {
				return nil, fmt.Errorf("package registry row at line %d must have package, role, and responsibility columns", j+1)
			}
			path := strings.TrimSpace(cells[0])
			if after, ok := strings.CutPrefix(path, "`"); ok {
				if inner, ok := strings.CutSuffix(after, "`"); ok {
					path = strings.TrimSpace(inner)
				}
			}
			roles["bog/"+path] = append(roles["bog/"+path], strings.TrimSpace(cells[1]))
		}
		return roles, nil
	}
	return nil, fmt.Errorf("missing package registry table under %s", packageRegistryHeading)
}

func systemRegistryProblems(doc string, rules map[string]rule, declarations map[string]stagingDeclaration) []string {
	roles, err := systemPackageRoles(doc)
	if err != nil {
		return []string{err.Error()}
	}
	problems := stagedPackageProblems(rules, declarations)
	for _, path := range slices.Sorted(maps.Keys(rules)) {
		if len(roles[path]) == 0 {
			problems = append(problems, fmt.Sprintf("missing row for %s in the package registry table", path))
		}
	}
	for _, path := range slices.Sorted(maps.Keys(roles)) {
		if len(roles[path]) > 1 {
			problems = append(problems, fmt.Sprintf("duplicate rows for %s in the package registry table", path))
			continue
		}

		entry, exists := rules[path]
		var want string
		if exists {
			want = systemRoleLabel(path, entry.role)
			if want == "" {
				problems = append(problems, fmt.Sprintf("no SYSTEM.md role label for %s registered as %q", path, entry.role))
				continue
			}
		} else {
			// The table also documents two entry points outside the Go registry.
			switch path {
			case "bog/bench/segment-layout":
				want = "Nested-module benchmark"
			case "bog/.github/workflows/linux-release.yml":
				want = "Release workflow"
			default:
				problems = append(problems, fmt.Sprintf("unregistered entry %s in the package registry table", path))
				continue
			}
		}
		if got := roles[path][0]; got != want {
			problems = append(problems, fmt.Sprintf("role mismatch for %s in the package registry table: got %q, want %q", path, got, want))
		}
	}
	return problems
}

func systemRoleLabel(path string, registeredRole role) string {
	switch registeredRole {
	case serverCommand:
		return "Server command"
	case runtimeLibrary:
		return "Wired"
	case staged:
		return "Staged"
	case standalone:
		return "Standalone"
	case testOnly:
		return "Test-only"
	case verifier:
		if path == operatorCommand {
			return "Operator command"
		}
		return "Verifier command"
	case verifierModel:
		return "Verifier support"
	default:
		return ""
	}
}

func TestSystemPackageRegistryFixtures(t *testing.T) {
	t.Parallel()
	const (
		backup      = "bog/internal/backup"
		release     = "bog/verify/release"
		serverRow   = "| `cmd/bog` | Server command | Server lifecycle. |\n"
		operatorRow = "| `cmd/bog-verify` | Operator command | Read-only verification. |\n"
		releaseRow  = "| `verify/release` | Verifier command | Release evidence. |\n"
		stagedRow   = "| `internal/backup` | Staged | Pinned export and read-only verification. |\n"
		wiredRow    = "| `internal/backup` | Wired | Verified writable recovery. |\n"
		removedRow  = "| `internal/removed` | Staged | Retired package. |\n"
		workflowRow = "| `.github/workflows/linux-release.yml` | Release workflow | Native jobs. |\n"
		layoutRow   = "| `bench/segment-layout` | Nested-module benchmark | Layout experiment. |\n"
		tableHeader = "| Package or entry point | Role | Responsibility |\n| --- | --- | --- |\n"
		section     = packageRegistryHeading + "\n\nStaged roles describe server activation.\n\n"
		before      = "## Before\n\n" + tableHeader + stagedRow + releaseRow + "\n"
		after       = "\n| Package | Future role | Notes |\n| --- | --- | --- |\n" + stagedRow
	)
	tests := []struct {
		name        string
		rows        string
		promoted    bool
		declaration bool
		want        []string
	}{
		{name: "operator-only backup remains staged", rows: stagedRow + releaseRow, declaration: true},
		{name: "workflow and nested-module rows", rows: stagedRow + releaseRow + workflowRow + layoutRow, declaration: true},
		{
			name: "missing staged row even when mentioned outside the table", rows: releaseRow, declaration: true,
			want: []string{"missing row for " + backup + " in the package registry table"},
		},
		{
			name: "missing verifier row even when mentioned outside the table", rows: stagedRow, declaration: true,
			want: []string{"missing row for " + release + " in the package registry table"},
		},
		{
			name: "stale wired role for a staged package", rows: wiredRow + releaseRow, declaration: true,
			want: []string{`role mismatch for ` + backup + ` in the package registry table: got "Wired", want "Staged"`},
		},
		{
			name:     "wired package still labeled standalone",
			rows:     strings.Replace(wiredRow, "Wired", "Standalone", 1) + releaseRow,
			promoted: true,
			want:     []string{`role mismatch for ` + backup + ` in the package registry table: got "Standalone", want "Wired"`},
		},
		{
			name:        "evidence verifier labeled as the operator",
			rows:        stagedRow + strings.Replace(releaseRow, "Verifier command", "Operator command", 1),
			declaration: true,
			want:        []string{`role mismatch for ` + release + ` in the package registry table: got "Operator command", want "Verifier command"`},
		},
		{
			name: "unregistered staged row", rows: stagedRow + releaseRow + removedRow, declaration: true,
			want: []string{"unregistered entry bog/internal/removed in the package registry table"},
		},
		{
			name: "unregistered wired row", rows: stagedRow + releaseRow + strings.Replace(removedRow, "Staged", "Wired", 1), declaration: true,
			want: []string{"unregistered entry bog/internal/removed in the package registry table"},
		},
		{
			name: "duplicate staged row", rows: stagedRow + releaseRow + stagedRow, declaration: true,
			want: []string{"duplicate rows for " + backup + " in the package registry table"},
		},
		{
			name: "conflicting roles for a staged package", rows: wiredRow + stagedRow + releaseRow, declaration: true,
			want: []string{"duplicate rows for " + backup + " in the package registry table"},
		},
		{
			name: "duplicate verifier row", rows: stagedRow + releaseRow + releaseRow, declaration: true,
			want: []string{"duplicate rows for " + release + " in the package registry table"},
		},
		{
			name: "duplicate workflow row", rows: stagedRow + releaseRow + workflowRow + workflowRow, declaration: true,
			want: []string{"duplicate rows for bog/.github/workflows/linux-release.yml in the package registry table"},
		},
		{
			name: "nested-module row has wrong role", rows: stagedRow + releaseRow + strings.Replace(layoutRow, "Nested-module benchmark", "Verifier command", 1), declaration: true,
			want: []string{`role mismatch for bog/bench/segment-layout in the package registry table: got "Verifier command", want "Nested-module benchmark"`},
		},
		{
			name: "staged row and role require a declaration", rows: stagedRow + releaseRow,
			want: []string{"staged package " + backup + " has no staging declaration"},
		},
		{
			name: "promotion leaves a stale row", rows: stagedRow + releaseRow, promoted: true,
			want: []string{`role mismatch for ` + backup + ` in the package registry table: got "Staged", want "Wired"`},
		},
		{
			name: "promotion leaves a stale declaration", rows: wiredRow + releaseRow, promoted: true, declaration: true,
			want: []string{"stale staging declaration for " + backup + ": package is registered as runtime library"},
		},
		{name: "server promotion clears the declaration and updates the role", rows: wiredRow + releaseRow, promoted: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			rules := map[string]rule{
				server:          {role: serverCommand},
				operatorCommand: {verifier, []string{backup}},
				backup:          {role: staged},
				release:         {role: verifier},
			}
			if tt.promoted {
				rules[server] = rule{serverCommand, []string{backup}}
				rules[backup] = rule{role: runtimeLibrary}
			}
			requireValidCommandFixture(t, rules)
			declarations := make(map[string]stagingDeclaration)
			if tt.declaration {
				declarations[backup] = stagingDeclaration{
					owner: "bog-storage", activation: "Prove verified writable restore and restart parity.",
				}
			}
			doc := before + section + tableHeader + serverRow + operatorRow + tt.rows + after
			if got := systemRegistryProblems(doc, rules, declarations); !slices.Equal(got, tt.want) {
				t.Errorf("systemRegistryProblems() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestSystemPackageRegistryTableRequired(t *testing.T) {
	t.Parallel()
	const heading = packageRegistryHeading + "\n\n"
	const header = "| Package or entry point | Role | Responsibility |\n"
	const delimiter = "| --- | --- | --- |\n"
	tests := []struct {
		name string
		doc  string
		want string
	}{
		{"missing section", header + delimiter, "missing ## Package registry heading"},
		{"table in the next section", heading + "## Other\n\n" + header + delimiter, "missing package registry table under ## Package registry"},
		{"missing delimiter", heading + header, "package registry table at line 3 has no valid delimiter row"},
		{"malformed row", heading + header + delimiter + "| `internal/backup` | Staged |\n", "package registry row at line 5 must have package, role, and responsibility columns"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := systemRegistryProblems(tt.doc, nil, nil); !slices.Equal(got, []string{tt.want}) {
				t.Errorf("systemRegistryProblems() = %q, want %q", got, tt.want)
			}
		})
	}
}
