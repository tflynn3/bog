package idempotency

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestValidateKey(t *testing.T) {
	for _, tt := range []struct {
		name  string
		key   string
		valid bool
	}{
		{"empty", "", false},
		{"valid punctuation", "azAZ09-_.~", true},
		{"at limit", strings.Repeat("k", MaxKeyBytes), true},
		{"over limit", strings.Repeat("k", MaxKeyBytes+1), false},
		{"space", "a b", false},
		{"list", "a,b", false},
		{"newline", "a\nb", false},
		{"non ASCII", "café", false},
		{"control", "a\x00b", false},
		{"slash", "a/b", false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateKey(tt.key)
			if tt.valid && err != nil || !tt.valid && !errors.Is(err, ErrInvalidKey) {
				t.Fatalf("ValidateKey(%q) = %v, valid = %t", tt.key, err, tt.valid)
			}
		})
	}
}

func TestSubmitRejectsInvalidInputBeforeBackend(t *testing.T) {
	oversized := make([]byte, MaxBatchBytes+1)
	for _, tt := range []struct {
		name   string
		modify func(*Request)
		want   error
	}{
		{"empty key", func(r *Request) { r.Key.Key = "" }, ErrInvalidKey},
		{"missing tenant", func(r *Request) { r.Key.Scope.TenantID = "" }, ErrInvalidScope},
		{"oversize stream", func(r *Request) { r.Key.Scope.StreamID = strings.Repeat("s", MaxScopeIDBytes+1) }, ErrInvalidScope},
		{"invalid principal", func(r *Request) { r.Key.Scope.PrincipalID = "someone else" }, ErrInvalidScope},
		{"empty original", func(r *Request) { r.Original = nil }, ErrInvalidBatch},
		{"large original", func(r *Request) { r.Original = oversized }, ErrInvalidBatch},
		{"empty batch", func(r *Request) { r.Batch = nil }, ErrInvalidBatch},
		{"large batch", func(r *Request) { r.Batch = oversized }, ErrInvalidBatch},
		{"invalid policy", func(r *Request) { r.PolicyVersion = "invalid policy" }, ErrInvalidPolicy},
		{"large policy", func(r *Request) { r.PolicyVersion = strings.Repeat("v", MaxPolicyVersionBytes+1) }, ErrInvalidPolicy},
		{"maximum sizes", func(r *Request) {
			r.PolicyVersion = strings.Repeat("v", MaxPolicyVersionBytes)
			r.Key.Scope.StreamID = strings.Repeat("s", MaxScopeIDBytes)
			r.Original = oversized[:MaxBatchBytes]
			r.Batch = oversized[:MaxBatchBytes]
		}, nil},
	} {
		t.Run(tt.name, func(t *testing.T) {
			b := openTestBackend(nil)
			b.beforeCommit = func(context.Context, CommitRequest) commitFault {
				if tt.want != nil {
					t.Fatal("invalid request reached backend")
				}
				return noFault
			}
			r := batchRequest(testScope, "validate", row)
			tt.modify(&r)
			s := submit(coordinatorFor(t, b, newTestClock()), t.Context(), r)
			if tt.want == nil {
				s.check(t, OutcomeCommitted, nil)
			} else {
				s.check(t, OutcomeRejected, tt.want)
			}
		})
	}
}

func TestConfigurationAndRecordedExpiryAcrossRestart(t *testing.T) {
	b := openTestBackend(nil)
	clock := newTestClock()
	for _, tt := range []struct {
		name    string
		backend Backend
		config  Config
	}{
		{"missing backend", nil, Config{FingerprintSecret: testSecret}},
		{"missing secret", b, Config{}},
		{"negative window", b, Config{FingerprintSecret: testSecret, RetryWindow: -time.Second}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if c, err := New(tt.backend, tt.config); c != nil || !errors.Is(err, ErrInvalidConfig) {
				t.Fatalf("New = (%v, %v), want invalid configuration", c, err)
			}
		})
	}

	c, err := New(b, Config{FingerprintSecret: testSecret, Clock: clock.now})
	if err != nil {
		t.Fatal(err)
	}
	req := batchRequest(testScope, "default-window", row)
	submit(c, t.Context(), req).check(t, OutcomeCommitted, nil)
	r, exists := b.record(req.Key)
	if !exists || !r.ExpiresAt.Equal(initialTime.Add(DefaultRetryWindow)) {
		t.Fatalf("default expiry = %v, want %v", r.ExpiresAt, initialTime.Add(DefaultRetryWindow))
	}
	clock.set(initialTime.Add(DefaultRetryWindow - time.Nanosecond))
	restarted := coordinatorFor(t, openTestBackend(b.image()), clock)
	submit(restarted, t.Context(), req).check(t, OutcomeReplayed, nil)
}

func TestFingerprintIsKeyedAndStable(t *testing.T) {
	original := []byte(row)
	digest := fingerprint(testSecret, original)
	if digest == (Fingerprint{}) || digest != fingerprint(testSecret, original) {
		t.Fatal("fingerprint must be nonempty and stable")
	}
	if digest == fingerprint([SecretBytes]byte{2}, original) {
		t.Fatal("a different server secret must produce a different digest")
	}
}
