package idempotency

import (
	"crypto/hmac"
	"crypto/sha256"
	"errors"
	"fmt"
)

const (
	// OperationDomain identifies the ingest retry namespace and hash format.
	// Persistent stores shared by multiple operations must include the domain
	// in the key index.
	OperationDomain = "bog-ingest-v1"

	MaxKeyBytes           = 128
	MaxScopeIDBytes       = 128
	MaxPolicyVersionBytes = 128
	MaxBatchBytes         = 4 << 20

	// SecretBytes is the size of the durable server-side fingerprint secret.
	SecretBytes = 32
)

var (
	ErrInvalidKey    = errors.New("invalid ingest retry key")
	ErrInvalidScope  = errors.New("invalid ingest retry scope")
	ErrInvalidBatch  = errors.New("invalid ingest retry batch size")
	ErrInvalidPolicy = errors.New("invalid ingest redaction policy version")
)

// Scope uses canonical server-issued identifiers. Credential rotation and
// stream renaming must preserve these identifiers.
type Scope struct {
	TenantID    string
	StreamID    string
	PrincipalID string
}

// ScopedKey is the index key for OperationDomain. Keep the components separate
// or use an unambiguous encoding when storing them.
type ScopedKey struct {
	Scope Scope
	Key   string
}

// ValidateKey checks a supplied key. Omission of the header is handled by the
// caller's unkeyed ingest path. An explicitly supplied empty key is invalid.
func ValidateKey(key string) error {
	if !validToken(key, MaxKeyBytes) {
		return ErrInvalidKey
	}
	return nil
}

// Validate checks the scope and key for use in the retry index.
func (k ScopedKey) Validate() error {
	if err := ValidateKey(k.Key); err != nil {
		return err
	}
	for _, field := range []struct{ name, value string }{
		{"tenant ID", k.Scope.TenantID},
		{"stream ID", k.Scope.StreamID},
		{"principal ID", k.Scope.PrincipalID},
	} {
		if !validToken(field.value, MaxScopeIDBytes) {
			return fmt.Errorf("%w: %s must be a nonempty identifier of at most %d bytes", ErrInvalidScope, field.name, MaxScopeIDBytes)
		}
	}
	return nil
}

// Request carries the original, fully validated body for fingerprinting and
// the batch already prepared for durable storage. In standalone mode these
// slices may reference the same bytes. In shared mode Batch contains only the
// redacted representation and PolicyVersion identifies the applied policy.
//
// The caller must keep the slices immutable until Submit returns. The
// coordinator never passes Original to the backend.
type Request struct {
	Key           ScopedKey
	Original      []byte
	Batch         []byte
	PolicyVersion string
}

func (r Request) validate() error {
	if err := r.Key.Validate(); err != nil {
		return err
	}
	if len(r.Original) == 0 || len(r.Original) > MaxBatchBytes {
		return fmt.Errorf("%w: original body must contain 1 to %d bytes", ErrInvalidBatch, MaxBatchBytes)
	}
	if len(r.Batch) == 0 || len(r.Batch) > MaxBatchBytes {
		return fmt.Errorf("%w: durable batch must contain 1 to %d bytes", ErrInvalidBatch, MaxBatchBytes)
	}
	if r.PolicyVersion != "" && !validToken(r.PolicyVersion, MaxPolicyVersionBytes) {
		return ErrInvalidPolicy
	}
	return nil
}

func validToken(value string, limit int) bool {
	if len(value) == 0 || len(value) > limit {
		return false
	}
	for i := 0; i < len(value); i++ {
		switch c := value[i]; {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9':
		case c == '-', c == '_', c == '.', c == '~':
		default:
			return false
		}
	}
	return true
}

// Fingerprint is a server-keyed digest of the exact original request bytes.
// Keep it out of client responses, logs, and metric labels.
type Fingerprint [sha256.Size]byte

func fingerprint(secret [SecretBytes]byte, original []byte) Fingerprint {
	// hash.Hash.Write always accepts the full input without error.
	mac := hmac.New(sha256.New, secret[:])
	_, _ = mac.Write([]byte(OperationDomain + "\x00"))
	_, _ = mac.Write(original)
	var digest Fingerprint
	mac.Sum(digest[:0])
	return digest
}
