package main

import (
	"debug/buildinfo"
	"debug/elf"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"runtime"
	"strings"
)

const releaseFixture = "linux-release-v1"
const releaseToolchain = "go1.27.0"

var releaseArchitectures = [...]string{"amd64", "arm64"}

func verifyNativeHost(arch string) error {
	if runtime.GOOS != "linux" || runtime.GOARCH != arch {
		return fmt.Errorf("expected a native Linux %s verifier, running on %s/%s", arch, runtime.GOOS, runtime.GOARCH)
	}
	return nil
}

func verifyCandidate(file io.ReaderAt, revision, arch string) error {
	var machine elf.Machine
	var tuningKey, tuningValue string
	switch arch {
	case "amd64":
		machine, tuningKey, tuningValue = elf.EM_X86_64, "GOAMD64", "v1"
	case "arm64":
		machine, tuningKey, tuningValue = elf.EM_AARCH64, "GOARM64", "v8.0"
	default:
		return fmt.Errorf("unsupported release architecture %q", arch)
	}
	header, err := elf.NewFile(file)
	if err != nil {
		return fmt.Errorf("read candidate ELF: %w", err)
	}
	if header.Machine != machine || header.Class != elf.ELFCLASS64 || header.Data != elf.ELFDATA2LSB ||
		(header.Type != elf.ET_EXEC && header.Type != elf.ET_DYN) || header.Entry == 0 {
		return fmt.Errorf("expected a Linux %s executable, got ELF %s/%s/%s/%s", arch, header.Machine, header.Class, header.Data, header.Type)
	}
	info, err := buildinfo.Read(file)
	if err != nil {
		return fmt.Errorf("read candidate Go build metadata: %w", err)
	}
	if info.GoVersion != releaseToolchain || info.Path != "bog/cmd/bog" {
		return fmt.Errorf("expected %s built with %s, got %s built with %s", "bog/cmd/bog", releaseToolchain, info.Path, info.GoVersion)
	}
	settings := make(map[string]string, len(info.Settings))
	for _, setting := range info.Settings {
		settings[setting.Key] = setting.Value
	}
	for _, want := range [][2]string{
		{"GOOS", "linux"}, {"GOARCH", arch}, {tuningKey, tuningValue},
		{"CGO_ENABLED", "0"}, {"vcs", "git"}, {"vcs.revision", revision}, {"vcs.modified", "false"},
	} {
		key, value := want[0], want[1]
		if settings[key] != value {
			return fmt.Errorf("candidate build setting %s=%q, want %q", key, settings[key], value)
		}
	}
	return nil
}

type releaseReports struct {
	admission, durability, capacity, parity string
}

func (p releaseReports) validate(id identity, strict bool) error {
	var err error
	for _, check := range []struct {
		name, path string
		cases      []string
	}{
		{"admission", p.admission, admissionCases},
		{"durability", p.durability, durabilityCases},
		{"capacity", p.capacity, capacityCases},
		{"restored parity", p.parity, []string{restoredParityCase}},
	} {
		if !strict && check.name == "restored parity" && check.path == "" {
			continue
		}
		if checkErr := validateFileMode(check.path, id, check.cases, strict); checkErr != nil {
			err = errors.Join(err, fmt.Errorf("%s report: %w", check.name, checkErr))
		}
	}
	return err
}

func qualifyRelease(root, revision, fixture string) error {
	if root == "" || !fullRevision.MatchString(revision) || fixture != releaseFixture {
		return fmt.Errorf("require an evidence directory, a full lowercase revision and fixture %s", releaseFixture)
	}
	root, err := filepath.Abs(root)
	if err != nil {
		return err
	}
	var failures error
	for _, arch := range releaseArchitectures {
		dir := filepath.Join(root, "linux-release-evidence-"+arch)
		id, err := readCandidateIdentity(filepath.Join(dir, "candidate"), revision, fixture, arch)
		if err != nil {
			failures = errors.Join(failures, fmt.Errorf("%s build identity: %w", arch, err))
			continue
		}
		if err := checkBundleRecords(dir, id); err != nil {
			failures = errors.Join(failures, fmt.Errorf("%s: %w", arch, err))
		}
		reports := releaseReports{
			admission:  filepath.Join(dir, "admission.json"),
			durability: filepath.Join(dir, "durability.json"),
			capacity:   filepath.Join(dir, "capacity.json"),
			parity:     filepath.Join(dir, "parity.json"),
		}
		if err := reports.validate(id, true); err != nil {
			failures = errors.Join(failures, fmt.Errorf("%s: %w", arch, err))
		}
	}
	return failures
}

func checkBundleRecords(dir string, id identity) error {
	var producers strings.Builder
	for _, name := range []string{"admission", "durability", "capacity", "parity"} {
		fmt.Fprintf(&producers, "%s revision=%s exit=0\n", name, id.Revision)
	}
	return errors.Join(
		checkEvidenceRecord(filepath.Join(dir, "binary.sha256"), id.BinarySHA256+"  candidate\n"),
		checkEvidenceRecord(filepath.Join(dir, "producers.txt"), producers.String()),
	)
}

func checkEvidenceRecord(path, expected string) (err error) {
	file, err := openRegular(path)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, file.Close()) }()
	data, err := io.ReadAll(io.LimitReader(file, int64(len(expected))+1))
	if err != nil {
		return err
	}
	if string(data) != expected {
		return fmt.Errorf("%s: evidence does not match the candidate digest or successful producer exits", filepath.Base(path))
	}
	return nil
}
