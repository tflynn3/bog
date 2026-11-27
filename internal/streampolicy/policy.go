// Package streampolicy makes authorization and transformation decisions for
// shared deployments. Callers authenticate principals, resolve selectors to
// trusted canonical scopes, and own cursor authentication and storage.
//
// A trusted policy authority supplies complete revisions. Grant checks and
// transformation selection use the same snapshot. Operations already admitted
// can finish with the captured policy, while revocations govern new admissions.
//
// Ingest callers admit before processing input or looking up a retry outcome.
// For a new append, callers redact the validated lines, then build the durable
// batch and its published rows from that same redacted result. Retry records
// use the admission's stream, principal, and redaction version.
//
// Query callers admit every page. The paging layer authenticates and expires
// cursors and uses the admitted masker before interpreting any row, including
// rows considered for matching, lookahead, and response size.
//
// Export callers restrict the capture and artifact to the admitted scope,
// recheck permission at final publication admission, and authorize each
// retrieval against the trusted source scope of the artifact.
package streampolicy

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
)

var (
	// ErrDenied is shared by absent, unknown, and unauthorized resources.
	ErrDenied = errors.New("stream access denied")

	// ErrInvalidRevision indicates invalid or ambiguous policy configuration.
	ErrInvalidRevision = errors.New("invalid stream policy revision")

	// ErrStaleRevision indicates an activation number that is already current
	// or has been superseded.
	ErrStaleRevision = errors.New("stream policy revision is stale")

	// ErrInvalidCursor is returned for an authorized request with an invalid
	// or mismatched continuation.
	ErrInvalidCursor = errors.New("invalid cursor")

	// ErrRedactionUnavailable is safe to report without exposing input lines
	// or implementation errors.
	ErrRedactionUnavailable = errors.New("stream redaction unavailable")
)

// Scope is the canonical identity assigned by the service to one stream.
type Scope struct {
	TenantID string
	StreamID string
}

func (s Scope) valid() bool {
	return s.TenantID != "" && s.StreamID != ""
}

// Grant assigns independent permissions to a stable authenticated principal
// for exactly one scope. Omitted grants and permissions are denied.
type Grant struct {
	Principal string
	Scope     Scope
	Ingest    bool
	Query     bool
	Export    bool
}

// Redactor transforms one validated line before durability. A redactor must
// be safe for concurrent calls and keep its transformation fixed for the
// lifetime of a revision and its admissions. It must honor context cancellation
// and keep input values out of side effects and diagnostics.
type Redactor func(ctx context.Context, line string) (string, error)

// StreamPolicy configures durable ingest redaction and query output masking
// independently. A nil Redactor marks redaction unavailable. An explicit
// identity transformation is required for intentional pass-through.
type StreamPolicy struct {
	Scope            Scope
	RedactionVersion string
	Redactor         Redactor
	Masks            []Replacement
}

// Revision is a complete desired policy snapshot. Number must be positive and
// increase with each activation. Activation copies the configuration. Callers
// must leave its slices unchanged until Activate returns.
type Revision struct {
	Number  uint64
	Streams []StreamPolicy
	Grants  []Grant
}

type subject struct {
	principal string
	scope     Scope
}

type streamPolicy struct {
	redactionVersion string
	redactor         Redactor
	mask             maskingPolicy
}

type snapshot struct {
	number  uint64
	streams map[Scope]streamPolicy
	grants  map[subject]Grant
}

// Engine is an in-process policy authority. Its zero value denies admissions
// until the first successful activation. An Engine must not be copied after
// first use.
type Engine struct {
	update  sync.Mutex
	current atomic.Pointer[snapshot]
}

// Activate validates and publishes one complete revision. After it returns
// successfully, subsequent admissions see this revision or a newer one.
// A rejected activation leaves the previous snapshot in force.
func (e *Engine) Activate(revision Revision) error {
	next, err := compileSnapshot(revision)
	if err != nil {
		return err
	}

	e.update.Lock()
	defer e.update.Unlock()
	previous := e.current.Load()
	if previous != nil && next.number <= previous.number {
		return ErrStaleRevision
	}
	if previous != nil {
		for scope, stream := range next.streams {
			old, exists := previous.streams[scope]
			if exists && old.mask.sameRules(stream.mask) {
				stream.mask = old.mask
				next.streams[scope] = stream
			}
		}
	}
	e.current.Store(next)
	return nil
}

// CurrentRevision reports the active revision number, or zero before activation.
func (e *Engine) CurrentRevision() uint64 {
	if current := e.current.Load(); current != nil {
		return current.number
	}
	return 0
}

type permission uint8

const (
	ingest permission = iota
	query
	export
)

func (g Grant) permits(needed permission) bool {
	switch needed {
	case ingest:
		return g.Ingest
	case query:
		return g.Query
	case export:
		return g.Export
	default:
		return false
	}
}

// The load is the admission linearization point. Permission and policy come
// from the same immutable snapshot, even when an update races the request.
func (e *Engine) admit(principal string, scope Scope, needed permission) (uint64, streamPolicy, error) {
	current := e.current.Load()
	if current == nil || !current.grants[subject{principal, scope}].permits(needed) {
		return 0, streamPolicy{}, ErrDenied
	}
	return current.number, current.streams[scope], nil
}
