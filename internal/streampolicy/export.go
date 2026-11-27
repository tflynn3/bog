package streampolicy

// ExportAdmission binds a pending export to the admitting authority, principal,
// and scope. It authorizes preparing an artifact of already durably redacted
// records from this scope. Query output masking is independent of exports.
type ExportAdmission struct {
	engine    *Engine
	revision  uint64
	principal string
	scope     Scope
}

// Revision reports the policy revision that admitted preparation.
func (a ExportAdmission) Revision() uint64 { return a.revision }

// Principal returns the principal whose grant governs publication.
func (a ExportAdmission) Principal() string { return a.principal }

// Scope returns the only stream permitted in the artifact and its metadata.
func (a ExportAdmission) Scope() Scope { return a.scope }

// AdmitExport checks current access before preparing an export.
func (e *Engine) AdmitExport(principal string, scope Scope) (ExportAdmission, error) {
	revision, _, err := e.admit(principal, scope, export)
	if err != nil {
		return ExportAdmission{}, err
	}
	return ExportAdmission{engine: e, revision: revision, principal: principal, scope: scope}, nil
}

// AuthorizeExportPublication rechecks the original principal and scope against
// current policy. Call it at final publication admission. On denial the caller
// must discard the pending artifact without making it retrievable.
func (e *Engine) AuthorizeExportPublication(admission ExportAdmission) error {
	if admission.engine != e || admission.revision == 0 {
		return ErrDenied
	}
	_, _, err := e.admit(admission.principal, admission.scope, export)
	return err
}

// AuthorizeExportDownload checks a fresh grant before invoking artifactScope.
// The callback looks up the requested artifact and returns its trusted source
// scope. Serve bytes only after success, from the same artifact examined in the
// callback. An export grant for the source scope allows retrieval regardless
// of who created the artifact.
func (e *Engine) AuthorizeExportDownload(principal string, scope Scope, artifactScope func() (Scope, error)) error {
	if _, _, err := e.admit(principal, scope, export); err != nil {
		return err
	}
	if artifactScope == nil {
		return ErrDenied
	}
	source, err := artifactScope()
	if err != nil || source != scope {
		return ErrDenied
	}
	return nil
}
