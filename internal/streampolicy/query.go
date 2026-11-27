package streampolicy

// CursorBinding is the policy-owned portion of a query cursor. The paging
// layer must authenticate it together with the other cursor claims, including
// the query and snapshot position. MaskingVersion is stream-local. It stays
// fixed across unrelated policy changes and changes when masking rules change,
// including enabling, disabling, and restoring earlier rules.
type CursorBinding struct {
	Principal      string
	Scope          Scope
	MaskingVersion uint64
}

// QueryAdmission captures the authorization and masking policy for one page.
// Use the same admission for anchor validation, matching, lookahead, encoding,
// and the next cursor. A subsequent page requires a new admission.
type QueryAdmission struct {
	revision uint64
	binding  CursorBinding
	mask     maskingPolicy
}

// Revision reports the policy revision that admitted this page.
func (a QueryAdmission) Revision() uint64 { return a.revision }

// Binding returns the claims to authenticate when issuing a continuation.
func (a QueryAdmission) Binding() CursorBinding { return a.binding }

// MaskLine applies the captured masking rules before query evaluation.
func (a QueryAdmission) MaskLine(line string) string { return a.mask.apply(line) }

// AdmitQuery checks current query access for a fresh page.
func (e *Engine) AdmitQuery(principal string, scope Scope) (QueryAdmission, error) {
	revision, stream, err := e.admit(principal, scope, query)
	if err != nil {
		return QueryAdmission{}, err
	}
	return QueryAdmission{
		revision: revision,
		binding:  CursorBinding{Principal: principal, Scope: scope, MaskingVersion: stream.mask.version},
		mask:     stream.mask,
	}, nil
}

// ResumeQuery checks current query access before invoking authenticate. The
// callback verifies the token and its other claims, then returns the trusted
// binding. Missing, invalid, and mismatched cursors share ErrInvalidCursor.
func (e *Engine) ResumeQuery(principal string, scope Scope, authenticate func() (CursorBinding, error)) (QueryAdmission, error) {
	admission, err := e.AdmitQuery(principal, scope)
	if err != nil {
		return QueryAdmission{}, err
	}
	if authenticate == nil {
		return QueryAdmission{}, ErrInvalidCursor
	}
	binding, err := authenticate()
	if err != nil || binding != admission.binding {
		return QueryAdmission{}, ErrInvalidCursor
	}
	return admission, nil
}
