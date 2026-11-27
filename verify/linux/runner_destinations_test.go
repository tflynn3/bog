package main

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRunnerDestinationEntries(t *testing.T) {
	base := t.TempDir()
	first, second := filepath.Join(base, "first"), filepath.Join(base, "second")
	for _, dir := range []string{first, second, filepath.Join(second, "child")} {
		if err := os.Mkdir(dir, 0700); err != nil {
			t.Fatal(err)
		}
	}
	alias, traversal := filepath.Join(base, "alias"), filepath.Join(first, "traversal")
	if err := os.Symlink(first, alias); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Join(second, "child"), traversal); err != nil {
		t.Fatal(err)
	}
	t.Chdir(base)
	for _, tc := range []struct {
		name, report, proof, executable, want string
	}{
		{name: "identical absent entry", report: "first/result.json", proof: "first/result.json", want: "separate destinations"},
		{name: "relative and absolute", report: first + "/result.json", proof: "./second/../first//result.json", want: "separate destinations"},
		{name: "symlink parent", report: "first/result.json", proof: "alias/result.json", want: "separate destinations"},
		{name: "kernel resolution across dot dot", report: "second/result.json", proof: "first/traversal/../result.json", want: "separate destinations"},
		{name: "distinct despite lexical clean", report: "first/result.json", proof: "first/traversal/../result.json"},
		{name: "same parent different names", report: "first/result.json", proof: "alias/proof.json"},
		{name: "same name different parents", report: "first/result.json", proof: "second/result.json"},
		{name: "missing proof parent", report: "first/result.json", proof: "missing/result.json", want: "open parent"},
		{name: "invalid proof basename", report: "first/result.json", proof: "second/", want: "must name a file"},
		{name: "missing executable aliases report", report: "first/result.json", proof: "second/result.json",
			executable: alias + "/result.json", want: "separate from executable"},
		{name: "missing executable aliases proof", report: "second/result.json", proof: "first/result.json",
			executable: alias + "/result.json", want: "separate from executable"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var executables []string
			if tc.executable != "" {
				executables = append(executables, tc.executable)
			}
			d, err := openRunnerDestinations(tc.report, tc.proof, executables...)
			if err == nil {
				t.Cleanup(func() {
					if err := errors.Join(d.report.Close(), d.proof.Close()); err != nil {
						t.Error(err)
					}
				})
			}
			if (err == nil) != (tc.want == "") || err != nil && !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("openRunnerDestinations error = %v, want %q", err, tc.want)
			}
			for _, path := range []string{tc.report, tc.proof} {
				if _, err := os.Lstat(path); err == nil && !strings.HasSuffix(path, "/") {
					t.Errorf("preflight created %q", path)
				} else if err != nil && !errors.Is(err, os.ErrNotExist) {
					t.Errorf("inspect %q: %v", path, err)
				}
			}
		})
	}
}

func TestRunnerDestinationsRejectExistingEntries(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		make func(source, output string) error
	}{
		{"hardlink", os.Link},
		{"symlink", os.Symlink},
		{"dangling symlink", func(source, output string) error { return os.Symlink(source+".missing", output) }},
		{"directory", func(_, output string) error { return os.Mkdir(output, 0700) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			proof, report, original := filepath.Join(dir, "proof"), filepath.Join(dir, "report"), filepath.Join(dir, "original")
			if err := os.WriteFile(original, []byte("untouched"), 0600); err != nil {
				t.Fatal(err)
			}
			if err := tc.make(original, proof); err != nil {
				t.Fatal(err)
			}
			before, err := os.Lstat(proof)
			if err != nil {
				t.Fatal(err)
			}
			for _, pair := range []struct{ report, proof, existing string }{
				{report, proof, proof},
				{proof, report, proof},
				{original, proof, original},
			} {
				d, err := openRunnerDestinations(pair.report, pair.proof)
				if err == nil {
					if err := errors.Join(d.report.Close(), d.proof.Close()); err != nil {
						t.Error(err)
					}
					t.Fatal("accepted existing destination")
				}
				if !strings.Contains(err.Error(), "evidence destination already exists: "+pair.existing) {
					t.Fatal(err)
				}
			}
			after, err := os.Lstat(proof)
			if err != nil || !os.SameFile(before, after) {
				t.Fatalf("evidence entry changed: %v, %v", after, err)
			}
			if data, err := os.ReadFile(original); err != nil || string(data) != "untouched" {
				t.Fatalf("original = %q, %v", data, err)
			}
		})
	}
}
