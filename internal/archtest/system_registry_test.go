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
// can discuss staged packages without declaring their current roles.
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
		if rules[path].role == staged && !slices.Contains(roles[path], "Staged") {
			problems = append(problems, fmt.Sprintf("missing Staged row for %s in the package registry table", path))
		}
	}
	for _, path := range slices.Sorted(maps.Keys(roles)) {
		if !slices.Contains(roles[path], "Staged") {
			continue
		}
		if len(roles[path]) > 1 {
			problems = append(problems, fmt.Sprintf("duplicate rows for %s in the package registry table", path))
		}
		entry, exists := rules[path]
		switch {
		case !exists:
			problems = append(problems, fmt.Sprintf("stale Staged row for %s: package is unregistered", path))
		case entry.role != staged:
			problems = append(problems, fmt.Sprintf("stale Staged row for %s: package is registered as %s", path, entry.role))
		}
	}
	return problems
}

func TestSystemPackageRegistryFixtures(t *testing.T) {
	t.Parallel()
	const (
		backup      = "bog/internal/backup"
		stagedRow   = "| `internal/backup` | Staged | Pinned export and read-only verification. |\n"
		wiredRow    = "| `internal/backup` | Wired | Verified writable recovery. |\n"
		removedRow  = "| `internal/removed` | Staged | Retired package. |\n"
		tableHeader = "| Package or entry point | Role | Responsibility |\n| --- | --- | --- |\n"
		section     = packageRegistryHeading + "\n\nStaged roles describe server activation.\n\n"
		before      = "## Before\n\n" + tableHeader + stagedRow + "\n"
		after       = "\n| Package | Future role | Notes |\n| --- | --- | --- |\n" + stagedRow
	)
	tests := []struct {
		name        string
		rows        string
		promoted    bool
		declaration bool
		want        []string
	}{
		{name: "operator-only backup remains staged", rows: stagedRow, declaration: true},
		{
			name: "missing staged row even when mentioned outside the table", rows: wiredRow, declaration: true,
			want: []string{"missing Staged row for " + backup + " in the package registry table"},
		},
		{
			name: "unregistered staged row", rows: stagedRow + removedRow, declaration: true,
			want: []string{"stale Staged row for bog/internal/removed: package is unregistered"},
		},
		{
			name: "duplicate staged row", rows: stagedRow + stagedRow, declaration: true,
			want: []string{"duplicate rows for " + backup + " in the package registry table"},
		},
		{
			name: "conflicting roles for a staged package", rows: wiredRow + stagedRow, declaration: true,
			want: []string{"duplicate rows for " + backup + " in the package registry table"},
		},
		{
			name: "staged row and role require a declaration", rows: stagedRow,
			want: []string{"staged package " + backup + " has no staging declaration"},
		},
		{
			name: "promotion leaves a stale row", rows: stagedRow, promoted: true,
			want: []string{"stale Staged row for " + backup + ": package is registered as runtime library"},
		},
		{
			name: "promotion leaves a stale declaration", rows: wiredRow, promoted: true, declaration: true,
			want: []string{"stale staging declaration for " + backup + ": package is registered as runtime library"},
		},
		{name: "server promotion clears the declaration and updates the role", rows: wiredRow, promoted: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			rules := map[string]rule{
				server:          {role: serverCommand},
				operatorCommand: {verifier, []string{backup}},
				backup:          {role: staged},
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
			doc := before + section + tableHeader + tt.rows + after
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
