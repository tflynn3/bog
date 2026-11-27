package streampolicy

import (
	"context"
	"unicode/utf8"
)

// IngestAdmission captures the current ingest grant and redactor for one
// request. Keyed retries must obtain an admission before consulting prior
// outcomes. The caller owns the retry lookup, serialization, and append.
type IngestAdmission struct {
	revision         uint64
	principal        string
	scope            Scope
	redactionVersion string
	redactor         Redactor
}

// Revision reports the policy revision that admitted the request.
func (a IngestAdmission) Revision() uint64 { return a.revision }

// Principal returns the authenticated principal used for admission.
func (a IngestAdmission) Principal() string { return a.principal }

// Scope returns the canonical stream to which the batch may be appended.
func (a IngestAdmission) Scope() Scope { return a.scope }

// RedactionVersion returns the policy version for a new commit. An exact
// retry returning a stored outcome retains the original commit's version.
func (a IngestAdmission) RedactionVersion() string { return a.redactionVersion }

// AdmitIngest checks current ingest access. Redactor availability is checked
// when applying the batch, so an authorized retry can still return a previous
// committed outcome while redaction is unavailable for new appends.
func (e *Engine) AdmitIngest(principal string, scope Scope) (IngestAdmission, error) {
	revision, stream, err := e.admit(principal, scope, ingest)
	if err != nil {
		return IngestAdmission{}, err
	}
	return IngestAdmission{
		revision:         revision,
		principal:        principal,
		scope:            scope,
		redactionVersion: stream.redactionVersion,
		redactor:         stream.redactor,
	}, nil
}

// RedactBatch applies the admitted redactor to each validated line, preserving
// order and leaving the input untouched. An error always yields nil output.
// Use the returned lines with the corresponding decoded row metadata to build
// and validate the complete durable representation before appending.
func (a IngestAdmission) RedactBatch(ctx context.Context, lines []string) ([]string, error) {
	if a.redactor == nil {
		return nil, ErrRedactionUnavailable
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	redacted := make([]string, len(lines))
	for i, line := range lines {
		result, err := redactLine(ctx, a.redactor, line)
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, ctxErr
		}
		if err != nil || !utf8.ValidString(result) {
			return nil, ErrRedactionUnavailable
		}
		redacted[i] = result
	}
	return redacted, nil
}

func redactLine(ctx context.Context, redactor Redactor, line string) (result string, err error) {
	// Confine recovery to the extension point. Panic values and callback
	// errors may contain raw input, so the batch exposes only a generic error.
	completed := false
	defer func() {
		if !completed {
			recover()
			result, err = "", ErrRedactionUnavailable
		}
	}()
	result, err = redactor(ctx, line)
	completed = true
	return result, err
}
