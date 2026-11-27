package streampolicy

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"unicode/utf8"
)

// Replacement is a case-sensitive literal substitution. Rules run in slice
// order and each rule replaces all occurrences in the result of the previous
// rule. An empty rule list represents disabled masking.
type Replacement struct {
	Find    string
	Replace string
}

type maskingPolicy struct {
	version uint64
	rules   []Replacement
}

func (m maskingPolicy) sameRules(other maskingPolicy) bool {
	return slices.Equal(m.rules, other.rules)
}

func (m maskingPolicy) apply(line string) string {
	return replaceLine(line, m.rules)
}

// NewLiteralRedactor validates and copies a list of literal replacements.
// An empty list explicitly configures identity redaction. The supplied rules
// may be reused after this function returns.
func NewLiteralRedactor(rules []Replacement) (Redactor, error) {
	compiled, err := copyReplacements(rules)
	if err != nil {
		return nil, err
	}
	return func(ctx context.Context, line string) (string, error) {
		if err := ctx.Err(); err != nil {
			return "", err
		}
		return replaceLine(line, compiled), nil
	}, nil
}

func copyReplacements(rules []Replacement) ([]Replacement, error) {
	for _, rule := range rules {
		if rule.Find == "" || !utf8.ValidString(rule.Find) || !utf8.ValidString(rule.Replace) {
			return nil, fmt.Errorf("%w: invalid literal replacement", ErrInvalidRevision)
		}
	}
	return slices.Clone(rules), nil
}

func replaceLine(line string, rules []Replacement) string {
	for _, rule := range rules {
		line = strings.ReplaceAll(line, rule.Find, rule.Replace)
	}
	return line
}

func compileSnapshot(revision Revision) (*snapshot, error) {
	if revision.Number == 0 {
		return nil, fmt.Errorf("%w: zero revision number", ErrInvalidRevision)
	}
	next := &snapshot{
		number:  revision.Number,
		streams: make(map[Scope]streamPolicy, len(revision.Streams)),
		grants:  make(map[subject]Grant, len(revision.Grants)),
	}
	for _, stream := range revision.Streams {
		if !stream.Scope.valid() {
			return nil, fmt.Errorf("%w: incomplete stream scope", ErrInvalidRevision)
		}
		if _, exists := next.streams[stream.Scope]; exists {
			return nil, fmt.Errorf("%w: duplicate stream scope", ErrInvalidRevision)
		}
		if stream.Redactor != nil && stream.RedactionVersion == "" {
			return nil, fmt.Errorf("%w: missing redaction version", ErrInvalidRevision)
		}
		rules, err := copyReplacements(stream.Masks)
		if err != nil {
			return nil, err
		}
		next.streams[stream.Scope] = streamPolicy{
			redactionVersion: stream.RedactionVersion,
			redactor:         stream.Redactor,
			mask: maskingPolicy{
				version: revision.Number,
				rules:   rules,
			},
		}
	}
	for _, grant := range revision.Grants {
		if grant.Principal == "" {
			return nil, fmt.Errorf("%w: missing grant principal", ErrInvalidRevision)
		}
		if _, exists := next.streams[grant.Scope]; !exists {
			return nil, fmt.Errorf("%w: grant references unknown stream", ErrInvalidRevision)
		}
		key := subject{grant.Principal, grant.Scope}
		if _, exists := next.grants[key]; exists {
			return nil, fmt.Errorf("%w: duplicate grant", ErrInvalidRevision)
		}
		next.grants[key] = grant
	}
	return next, nil
}
