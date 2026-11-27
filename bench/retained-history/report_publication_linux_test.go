package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"bog/verify/capacityevidence"
	"bog/verify/reportpublication"
)

type proofPublicationFixture struct {
	reportpublication.DescriptorDirectory
	beforeCommit func() error
	syncErr      error
}

func (d proofPublicationFixture) RenameNoReplace(from, to string) error {
	if err := d.beforeCommit(); err != nil {
		return err
	}
	return d.DescriptorDirectory.RenameNoReplace(from, to)
}

func (d proofPublicationFixture) Sync() error {
	if d.syncErr != nil {
		return d.syncErr
	}
	return d.DescriptorDirectory.Sync()
}

func TestDelegatedProofPublication(t *testing.T) {
	t.Parallel()
	syncErr := errors.New("directory sync failed")
	for _, tc := range []struct {
		name      string
		state     reportpublication.State
		wantErr   error
		competing bool
	}{
		{"success", reportpublication.DurablyPublished, nil, false},
		{"winner after staging", reportpublication.Unpublished, os.ErrExist, true},
		{"directory sync fault", reportpublication.DurabilityUncertain, syncErr, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			parent := t.TempDir()
			const name = "capacity.json"
			path := filepath.Join(parent, name)
			root, err := os.OpenRoot(parent)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := root.Close(); err != nil {
					t.Error(err)
				}
			})
			file, err := root.Open(".")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := file.Close(); err != nil {
					t.Error(err)
				}
			})
			proof := passingCutoverReport()
			staged, err := json.MarshalIndent(proof, "", "  ")
			if err != nil {
				t.Fatal(err)
			}
			staged = append(staged, '\n')
			// These different bytes satisfy the strict schema-v1 validator.
			competing := append([]byte(" "), staged...)
			if err := capacityevidence.ValidateReleaseCutoverReport(competing); err != nil {
				t.Fatal(err)
			}
			ops := proofPublicationFixture{
				DescriptorDirectory: reportpublication.BorrowDirectory(file),
				beforeCommit: func() error {
					entries, err := os.ReadDir(parent)
					if err != nil || len(entries) != 1 || entries[0].Name() == name {
						t.Fatalf("before commit entries = %v, %v", entries, err)
					}
					if !tc.competing {
						return nil
					}
					return root.WriteFile(name, competing, 0600)
				},
			}
			if tc.wantErr == syncErr {
				ops.syncErr = syncErr
			}
			state, err := writeProof(ops, name, proof)
			if state != tc.state || !errors.Is(err, tc.wantErr) {
				t.Fatalf("writeProof = %q, %v, want %q, %v", state, err, tc.state, tc.wantErr)
			}
			if err := file.Sync(); err != nil {
				t.Fatalf("borrowed directory unusable: %v", err)
			}
			want := staged
			if tc.competing {
				want = competing
			}
			if got, err := os.ReadFile(path); err != nil || !bytes.Equal(got, want) {
				t.Fatalf("published proof equals expected bytes = %v, read error = %v", bytes.Equal(got, want), err)
			}
			if entries, err := os.ReadDir(parent); err != nil || len(entries) != 1 || entries[0].Name() != name {
				t.Fatalf("published directory entries = %v, %v", entries, err)
			}
		})
	}
}

// Exercise the production CLI path with the same descriptor handoff as
// the delegated runner. An early workload failure still attempts publication.
func TestCreateOnlyReportFlagUsesInheritedDirectory(t *testing.T) {
	const proofName = "capacity.json"
	if os.Getenv("BOG_TEST_INHERITED_PROOF_CHILD") != "" {
		args := []string{
			"-binary", filepath.Join(os.Getenv("BOG_TEST_INHERITED_PROOF_CHILD"), "missing-bog"),
			"-revision", passingCutoverReport().Revision, "-fixture", capacityevidence.FixtureID,
			"-work-dir", os.Getenv("BOG_TEST_INHERITED_PROOF_CHILD"), "-report", "/proc/self/fd/3/" + proofName,
			"-report-create-only",
		}
		err := run(t.Context(), args, writeReport)
		var outcome *reportpublication.Error
		if !errors.As(err, &outcome) || outcome.State != reportpublication.Unpublished ||
			!errors.Is(err, os.ErrExist) || !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("capacity producer error = %v", err)
		}
		return
	}

	parent := t.TempDir()
	path := filepath.Join(parent, proofName)
	competing, err := json.Marshal(passingCutoverReport())
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, competing, 0600); err != nil {
		t.Fatal(err)
	}
	dir, err := os.Open(parent)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := dir.Close(); err != nil {
			t.Error(err)
		}
	})
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, executable, "-test.run", "^TestCreateOnlyReportFlagUsesInheritedDirectory$")
	cmd.Env = append(os.Environ(), "BOG_TEST_INHERITED_PROOF_CHILD="+t.TempDir())
	cmd.ExtraFiles = []*os.File{dir}
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("capacity producer subprocess: %v\n%s", err, output)
	}
	if err := dir.Sync(); err != nil {
		t.Fatalf("caller-owned descriptor unusable: %v", err)
	}
	if got, err := os.ReadFile(path); err != nil || !bytes.Equal(got, competing) {
		t.Fatalf("competing proof preserved = %v, read error = %v", bytes.Equal(got, competing), err)
	}
	if entries, err := os.ReadDir(parent); err != nil || len(entries) != 1 || entries[0].Name() != proofName {
		t.Fatalf("proof directory entries = %v, %v", entries, err)
	}
}
