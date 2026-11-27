package archtest

import (
	"fmt"
	"os"
	"regexp"
	"slices"
	"strings"
	"testing"
)

const (
	systemDiagramHeading = "## Declared Linux production import graph"
	runtimeSubgraph      = "Runtime"
	stagedSubgraph       = "Staged"
)

func TestSystemImportDiagram(t *testing.T) {
	t.Parallel()
	const path = "../../docs/architecture/SYSTEM.md"
	doc, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, problem := range systemDiagramProblems(string(doc), registry) {
		t.Errorf("%s: %s", path, problem)
	}
}

type diagramEdge struct {
	from, to string
}

type systemImportGraph struct {
	nodes map[string]string // Package path to subgraph ID, or empty at top level.
	edges map[diagramEdge]bool
}

func systemDiagramProblems(doc string, rules map[string]rule) []string {
	graph, err := parseSystemImportGraph(doc)
	if err != nil {
		return []string{err.Error()}
	}

	var problems []string
	serverClosure := registeredReachability(rules, server)
	expectedEdges := make(map[diagramEdge]bool)
	for path, entry := range rules {
		if entry.role == testOnly {
			continue
		}
		if group, ok := graph.nodes[path]; !ok {
			problems = append(problems, "missing production node "+path)
		} else {
			for _, membership := range []struct {
				group string
				want  bool
			}{
				{runtimeSubgraph, serverClosure[path]},
				{stagedSubgraph, entry.role == staged},
			} {
				switch {
				case membership.want && group != membership.group:
					problems = append(problems, fmt.Sprintf("missing %s subgraph member %s", membership.group, path))
				case !membership.want && group == membership.group:
					problems = append(problems, fmt.Sprintf("unexpected %s subgraph member %s", membership.group, path))
				}
			}
		}
		for _, dependency := range entry.imports {
			edge := diagramEdge{path, dependency}
			expectedEdges[edge] = true
			if !graph.edges[edge] {
				problems = append(problems, fmt.Sprintf("missing production edge %s -> %s", edge.from, edge.to))
			}
		}
	}
	for path := range graph.nodes {
		if entry, ok := rules[path]; !ok || entry.role == testOnly {
			problems = append(problems, "unexpected production node "+path)
		}
	}
	for edge := range graph.edges {
		if !expectedEdges[edge] {
			problems = append(problems, fmt.Sprintf("unexpected production edge %s -> %s", edge.from, edge.to))
		}
	}
	slices.Sort(problems)
	return problems
}

var (
	diagramDirection = regexp.MustCompile(`^flowchart\s+(TD|TB|BT|LR|RL)$`)
	diagramSubgraph  = regexp.MustCompile(`^subgraph\s+([A-Za-z][A-Za-z0-9_]*)\s*\["[^"]+"\]$`)
	diagramNode      = regexp.MustCompile(`^([A-Za-z][A-Za-z0-9_]*)\s*\["([A-Za-z0-9_./-]+)( \([^"]+\))?"\]$`)
	diagramArrow     = regexp.MustCompile(`^([A-Za-z][A-Za-z0-9_]*)\s*-->\s*([A-Za-z][A-Za-z0-9_]*)$`)
)

// Read the Mermaid block in the declared graph section. Accept the small
// flowchart subset used by SYSTEM.md. Edges inside a subgraph must have
// both endpoints declared there so they cannot introduce implicit members.
func parseSystemImportGraph(doc string) (systemImportGraph, error) {
	lines := strings.Split(doc, "\n")
	section := slices.IndexFunc(lines, func(line string) bool {
		return strings.TrimSpace(line) == systemDiagramHeading
	})
	if section == -1 {
		return systemImportGraph{}, fmt.Errorf("missing %s heading", systemDiagramHeading)
	}

	start, end := -1, -1
	for i := section + 1; i < len(lines); i++ {
		line := strings.TrimSpace(lines[i])
		if strings.HasPrefix(line, "#") {
			break
		}
		if line == "```mermaid" {
			if start != -1 {
				return systemImportGraph{}, fmt.Errorf("multiple Mermaid openings at line %d", i+1)
			}
			start = i + 1
		}
		if line == "```" && start != -1 && end == -1 {
			end = i
		}
	}
	if start == -1 || end == -1 {
		return systemImportGraph{}, fmt.Errorf("missing complete Mermaid block under %s", systemDiagramHeading)
	}
	if start == end || !diagramDirection.MatchString(strings.TrimSpace(lines[start])) {
		return systemImportGraph{}, fmt.Errorf("expected Mermaid flowchart direction at line %d", start+1)
	}

	graph := systemImportGraph{nodes: make(map[string]string), edges: make(map[diagramEdge]bool)}
	aliases := make(map[string]string)
	subgraphs := make(map[string]bool)
	type diagramArrowLine struct {
		diagramEdge
		line  int
		group string
	}
	var arrows []diagramArrowLine
	group := ""
	for i := start + 1; i < end; i++ {
		line := strings.TrimSpace(lines[i])
		subgraph := diagramSubgraph.FindStringSubmatch(line)
		switch {
		case line == "":
		case subgraph != nil && group == "":
			group = subgraph[1]
			if _, isNode := aliases[group]; subgraphs[group] || isNode {
				return systemImportGraph{}, fmt.Errorf("duplicate diagram alias %s at line %d", group, i+1)
			}
			subgraphs[group] = true
		case line == "end" && group != "":
			group = ""
		default:
			if node := diagramNode.FindStringSubmatch(line); node != nil {
				alias, path := node[1], "bog/"+node[2]
				if _, exists := aliases[alias]; exists || subgraphs[alias] {
					return systemImportGraph{}, fmt.Errorf("duplicate diagram alias %s at line %d", alias, i+1)
				}
				if _, exists := graph.nodes[path]; exists {
					return systemImportGraph{}, fmt.Errorf("duplicate production node %s at line %d", path, i+1)
				}
				aliases[alias] = path
				graph.nodes[path] = group
			} else if arrow := diagramArrow.FindStringSubmatch(line); arrow != nil {
				arrows = append(arrows, diagramArrowLine{diagramEdge{arrow[1], arrow[2]}, i + 1, group})
			} else {
				return systemImportGraph{}, fmt.Errorf("unsupported Mermaid statement at line %d: %q", i+1, line)
			}
		}
	}
	if group != "" {
		return systemImportGraph{}, fmt.Errorf("unclosed diagram subgraph before line %d", end+1)
	}

	for _, arrow := range arrows {
		from, to := aliases[arrow.from], aliases[arrow.to]
		if from == "" || to == "" {
			return systemImportGraph{}, fmt.Errorf("undeclared diagram endpoint at line %d: %s -> %s", arrow.line, arrow.from, arrow.to)
		}
		if arrow.group != "" && (graph.nodes[from] != arrow.group || graph.nodes[to] != arrow.group) {
			return systemImportGraph{}, fmt.Errorf("edge inside %s subgraph at line %d has an endpoint declared outside it: %s -> %s", arrow.group, arrow.line, from, to)
		}
		edge := diagramEdge{from, to}
		if graph.edges[edge] {
			return systemImportGraph{}, fmt.Errorf("duplicate production edge %s -> %s at line %d", from, to, arrow.line)
		}
		graph.edges[edge] = true
	}
	return graph, nil
}

func TestSystemImportDiagramFixtures(t *testing.T) {
	t.Parallel()
	const (
		httpapi    = "bog/internal/httpapi"
		store      = "bog/internal/store"
		backup     = "bog/internal/backup"
		leaf       = "bog/verify/query-model"
		arrow      = "    Cmd --> HTTP"
		storeNode  = `        Store["internal/store"]`
		backupNode = `        Backup["internal/backup"]`
		stagedOpen = `    subgraph Staged["Awaiting activation"]`
	)
	rules := map[string]rule{
		server:            {serverCommand, []string{httpapi}},
		httpapi:           {runtimeLibrary, []string{store}},
		store:             {runtimeLibrary, nil},
		backup:            {staged, []string{store}},
		operatorCommand:   {verifier, []string{backup}},
		leaf:              {verifierModel, nil},
		"bog/verify/fuzz": {testOnly, nil},
	}
	const body = `flowchart TD
    subgraph Runtime["Server"]
        Cmd["cmd/bog"]
        HTTP["internal/httpapi"]
` + storeNode + "\n    end\n" + stagedOpen + "\n" + backupNode + `
    end
    Cmd --> HTTP
    HTTP --> Store
    Backup --> Store
    Operator --> Backup
    Operator["cmd/bog-verify"]
    Model["verify/query-model (independent oracle)"]
`
	const open = "```mermaid\n"
	const close = "```\n"
	const doc = "## Other diagram\n" + open + "Ignored content\n" + close +
		systemDiagramHeading + "\n\n" + open + body + close

	tests := []struct {
		name, old, replacement string
		want                   []string
	}{
		{name: "exact graph with operator-only backup, isolated node, and forward reference"},
		{
			name:        "staged backup drawn inside server closure",
			old:         "    end\n" + stagedOpen + "\n" + backupNode,
			replacement: backupNode + "\n    end\n" + stagedOpen,
			want:        []string{"missing Staged subgraph member " + backup, "unexpected Runtime subgraph member " + backup},
		},
		{
			name:        "transitively wired store left outside server closure",
			old:         storeNode + "\n    end",
			replacement: "    end\n" + storeNode,
			want:        []string{"missing Runtime subgraph member " + store},
		},
		{
			name:        "wired store drawn as staged",
			old:         storeNode + "\n    end\n" + stagedOpen,
			replacement: "    end\n" + stagedOpen + "\n" + storeNode,
			want:        []string{"missing Runtime subgraph member " + store, "unexpected Staged subgraph member " + store},
		},
		{
			name: "renamed staged subgraph", old: stagedOpen,
			replacement: `    subgraph Pending["Awaiting activation"]`,
			want:        []string{"missing Staged subgraph member " + backup},
		},
		{
			name: "edge inside Runtime cannot pull in staged backup", old: storeNode,
			replacement: storeNode + "\n        Backup --> Store",
			want:        []string{"edge inside Runtime subgraph at line 13 has an endpoint declared outside it: " + backup + " -> " + store},
		},
		{
			name: "missing edge", old: arrow,
			want: []string{"missing production edge " + server + " -> " + httpapi},
		},
		{
			name: "extra transitive edge", old: arrow, replacement: arrow + "\n    Cmd --> Store",
			want: []string{"unexpected production edge " + server + " -> " + store},
		},
		{
			name: "reversed edge", old: arrow, replacement: "    HTTP --> Cmd",
			want: []string{
				"missing production edge " + server + " -> " + httpapi,
				"unexpected production edge " + httpapi + " -> " + server,
			},
		},
		{
			name: "missing isolated node", old: `    Model["verify/query-model (independent oracle)"]`,
			want: []string{"missing production node " + leaf},
		},
		{
			name: "extra nodes", old: arrow,
			replacement: arrow + "\n    Fuzz[\"verify/fuzz\"]\n    Unknown[\"verify/unknown\"]",
			want:        []string{"unexpected production node bog/verify/fuzz", "unexpected production node bog/verify/unknown"},
		},
		{
			name: "undeclared endpoint", old: arrow, replacement: "    Cmd --> Missing",
			want: []string{"undeclared diagram endpoint at line 17: Cmd -> Missing"},
		},
		{
			name: "duplicate alias", old: arrow, replacement: arrow + "\n    Cmd[\"cmd/other\"]",
			want: []string{"duplicate diagram alias Cmd at line 18"},
		},
		{
			name: "duplicate subgraph alias", old: stagedOpen,
			replacement: `    subgraph Runtime["Awaiting activation"]`,
			want:        []string{"duplicate diagram alias Runtime at line 14"},
		},
		{
			name: "duplicate package", old: arrow, replacement: arrow + "\n    Other[\"cmd/bog\"]",
			want: []string{"duplicate production node bog/cmd/bog at line 18"},
		},
		{
			name: "duplicate edge", old: arrow, replacement: arrow + "\n" + arrow,
			want: []string{"duplicate production edge " + server + " -> " + httpapi + " at line 18"},
		},
		{
			name: "unsupported edge syntax", old: arrow, replacement: arrow + " --> Store",
			want: []string{`unsupported Mermaid statement at line 17: "Cmd --> HTTP --> Store"`},
		},
		{
			name: "missing heading", old: systemDiagramHeading, replacement: "## Elsewhere",
			want: []string{"missing " + systemDiagramHeading + " heading"},
		},
		{
			name: "missing closing fence", old: body + close, replacement: body,
			want: []string{"missing complete Mermaid block under " + systemDiagramHeading},
		},
		{
			name: "Mermaid block in a different section", old: body + close,
			replacement: "\n## Elsewhere\n" + open + body + close,
			want:        []string{"missing complete Mermaid block under " + systemDiagramHeading},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			fixture := strings.Replace(doc, tt.old, tt.replacement, 1)
			if got := systemDiagramProblems(fixture, rules); !slices.Equal(got, tt.want) {
				t.Errorf("systemDiagramProblems() = %q, want %q", got, tt.want)
			}
		})
	}
}
