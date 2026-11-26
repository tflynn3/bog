package main

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const testRevision = "b797128b09c278bc547ef73e25d7d67c97315966"

func TestReportIdentityAndCaseOutcomes(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	binary := filepath.Join(dir, "candidate")
	content := []byte("candidate bytes")
	if err := os.WriteFile(binary, content, 0700); err != nil {
		t.Fatal(err)
	}
	r, err := newReport(binary, testRevision, fixtureID)
	if err != nil {
		t.Fatal(err)
	}
	if err := r.record("sigkill", reconciliation{AcknowledgedRows: 9}, nil); err != nil {
		t.Fatal(err)
	}
	failed := errors.New("case did not recover")
	if err := r.record("recovery", nil, failed); !errors.Is(err, failed) {
		t.Fatalf("record returned %v", err)
	}
	path := filepath.Join(dir, "report.json")
	if err := r.write(path); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var got report
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatal(err)
	}
	if got.SchemaVersion != 1 || got.Revision != testRevision || got.Fixture != fixtureID ||
		got.BinarySHA256 != fmt.Sprintf("%x", sha256.Sum256(content)) {
		t.Fatalf("invalid report identity: %+v", got)
	}
	if got.Cases["sigkill"].Status != "pass" || got.Cases["recovery"].Status != "fail" ||
		got.Cases["recovery"].Detail != failed.Error() {
		t.Fatalf("invalid reports: %+v", got.Cases)
	}
	if err := r.write(filepath.Join(dir, "missing", "report.json")); err == nil {
		t.Fatal("expected report write error")
	}
}

func TestReportRejectsInvalidIdentity(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	binary := filepath.Join(dir, "candidate")
	for _, tc := range []struct{ path, rev, fixture string }{
		{"relative", testRevision, fixtureID},
		{dir, testRevision, fixtureID},
		{binary, "b797128", fixtureID},
		{binary, strings.ToUpper(testRevision), fixtureID},
		{binary, testRevision, "other"},
		{binary, testRevision, fixtureID},
	} {
		if _, err := newReport(tc.path, tc.rev, tc.fixture); err == nil {
			t.Errorf("accepted invalid identity %+v", tc)
		}
	}
	if err := os.WriteFile(binary, []byte("nonexecutable"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := newReport(binary, testRevision, fixtureID); err == nil {
		t.Fatal("accepted nonexecutable file")
	}
}
