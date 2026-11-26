package main

import (
	"bytes"
	"debug/elf"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Give the production parser a minimal ELF with a loadable Go metadata
// section. The fixtures are never executed and require no cross toolchain.
func candidateELF(t *testing.T, machine elf.Machine, settings string) []byte {
	t.Helper()
	const offset = 128
	const address = 0x400000
	version := releaseToolchain
	mod := strings.Repeat("x", 16) + "path\tbog/cmd/bog\n" + settings + strings.Repeat("x", 16)
	blob := make([]byte, 32)
	copy(blob, "\xff Go buildinf:")
	blob[14], blob[15] = 8, 2
	blob = append(binary.AppendUvarint(blob, uint64(len(version))), version...)
	blob = append(binary.AppendUvarint(blob, uint64(len(mod))), mod...)
	names := "\x00.shstrtab\x00.go.buildinfo\x00"
	header := elf.Header64{
		Ident: [16]byte{0x7f, 'E', 'L', 'F', byte(elf.ELFCLASS64), byte(elf.ELFDATA2LSB), 1},
		Type:  uint16(elf.ET_EXEC), Machine: uint16(machine), Version: 1, Entry: address,
		Ehsize: 64, Phoff: 64, Phentsize: 56, Phnum: 1,
		Shoff: uint64(offset + len(blob) + len(names)), Shentsize: 64, Shnum: 3, Shstrndx: 1,
	}
	program := elf.Prog64{Type: uint32(elf.PT_LOAD), Off: offset, Vaddr: address, Filesz: uint64(len(blob)), Memsz: uint64(len(blob))}
	var buf bytes.Buffer
	write := func(value any) {
		t.Helper()
		if err := binary.Write(&buf, binary.LittleEndian, value); err != nil {
			t.Fatal(err)
		}
	}
	write(header)
	write(program)
	write(make([]byte, offset-buf.Len()))
	write(blob)
	write([]byte(names))
	write(elf.Section64{})
	write(elf.Section64{Name: 1, Type: uint32(elf.SHT_STRTAB), Off: uint64(offset + len(blob)), Size: uint64(len(names))})
	write(elf.Section64{Name: 11, Type: uint32(elf.SHT_PROGBITS), Addr: address, Off: offset, Size: uint64(len(blob))})
	return buf.Bytes()
}

func nativeSettings(arch string) string {
	tuning := "GOAMD64=v1"
	if arch == "arm64" {
		tuning = "GOARM64=v8.0"
	}
	return fmt.Sprintf("build\tGOOS=linux\nbuild\tGOARCH=%s\nbuild\t%s\nbuild\tCGO_ENABLED=0\nbuild\tvcs=git\nbuild\tvcs.revision=%s\nbuild\tvcs.modified=false\n", arch, tuning, testRevision)
}

func TestVerifyCandidate(t *testing.T) {
	t.Parallel()
	amd := nativeSettings("amd64")
	for _, tc := range []struct {
		name, arch, settings, want string
		machine                    elf.Machine
	}{
		{"amd64", "amd64", amd, "", elf.EM_X86_64},
		{"arm64", "arm64", nativeSettings("arm64"), "", elf.EM_AARCH64},
		{"wrong ELF architecture", "arm64", nativeSettings("arm64"), "expected a Linux arm64 executable", elf.EM_X86_64},
		{"wrong Go architecture", "arm64", amd, "GOARCH", elf.EM_AARCH64},
		{"dirty tree", "amd64", strings.Replace(amd, "modified=false", "modified=true", 1), "vcs.modified", elf.EM_X86_64},
		{"wrong revision", "amd64", strings.Replace(amd, testRevision, strings.Repeat("0", 40), 1), "vcs.revision", elf.EM_X86_64},
		{"wrong tuning", "amd64", strings.Replace(amd, "GOAMD64=v1", "GOAMD64=v3", 1), "GOAMD64", elf.EM_X86_64},
		{"wrong OS", "amd64", strings.Replace(amd, "GOOS=linux", "GOOS=freebsd", 1), "GOOS", elf.EM_X86_64},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := verifyCandidate(bytes.NewReader(candidateELF(t, tc.machine, tc.settings)), testRevision, tc.arch)
			checkProblem(t, err, tc.want)
		})
	}
}

func checkProblem(t *testing.T, err error, problem string) {
	t.Helper()
	if problem == "" && err != nil || problem != "" && (err == nil || !strings.Contains(err.Error(), problem)) {
		t.Fatalf("wanted %q, got %v", problem, err)
	}
}

func writeNativeBundle(t *testing.T, dir string, machine elf.Machine, arch string) {
	t.Helper()
	if err := os.Mkdir(dir, 0700); err != nil {
		t.Fatal(err)
	}
	binary := filepath.Join(dir, "candidate")
	writeFixture(t, binary, candidateELF(t, machine, nativeSettings(arch)))
	id, err := readIdentity(binary, testRevision, releaseFixture)
	if err != nil {
		t.Fatal(err)
	}
	writeFixture(t, filepath.Join(dir, "binary.sha256"), []byte(id.BinarySHA256+"  candidate\n"))
	writeFixture(t, filepath.Join(dir, "producers.txt"), []byte(fmt.Sprintf(
		"admission revision=%[1]s exit=0\ndurability revision=%[1]s exit=0\ncapacity revision=%[1]s exit=0\nparity revision=%[1]s exit=0\n", id.Revision)))
	writeFixture(t, filepath.Join(dir, "capacity.json"), capacityReport(id))
	for _, fixture := range []struct {
		name, caseName, evidence string
		required                 []string
	}{
		{"admission", "work_budget", nativeBudgetJSON, admissionCases},
		{"durability", "enospc", nativeENOSPCJSON, durabilityCases},
		{"parity", restoredParityCase, nativeParityJSON, []string{restoredParityCase}},
	} {
		results := report{id, make(map[string]outcome)}
		for _, name := range fixture.required {
			results.Cases[name] = outcome{Status: "pass"}
		}
		results.Cases[fixture.caseName] = outcome{Status: "pass", Evidence: []byte(fixture.evidence)}
		raw, err := json.Marshal(results)
		if err != nil {
			t.Fatal(err)
		}
		writeFixture(t, filepath.Join(dir, fixture.name+".json"), raw)
	}
}

func TestQualifyRequiresBothNativeBundles(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, source, target, problem string
	}{
		{"both pass", "", "", ""},
		{"missing arm64 parity", "", "parity.json", "arm64: restored parity report"},
		{"swapped binary", "candidate", "candidate", "expected a Linux arm64 executable"},
		{"swapped report", "admission.json", "admission.json", "arm64: admission report: report identity mismatch"},
		{"swapped checksum", "binary.sha256", "binary.sha256", "arm64: binary.sha256"},
		{"failed producer", "", "producers.txt", "arm64: producers.txt"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			amd := filepath.Join(root, "linux-release-evidence-amd64")
			arm := filepath.Join(root, "linux-release-evidence-arm64")
			writeNativeBundle(t, amd, elf.EM_X86_64, "amd64")
			writeNativeBundle(t, arm, elf.EM_AARCH64, "arm64")
			target := filepath.Join(arm, tc.target)
			switch {
			case tc.source != "":
				raw, err := os.ReadFile(filepath.Join(amd, tc.source))
				if err != nil {
					t.Fatal(err)
				}
				writeFixture(t, target, raw)
			case tc.target == "producers.txt":
				raw, err := os.ReadFile(target)
				if err != nil {
					t.Fatal(err)
				}
				writeFixture(t, target, bytes.Replace(raw, []byte("exit=0"), []byte("exit=1"), 1))
			case tc.target != "":
				if err := os.Remove(target); err != nil {
					t.Fatal(err)
				}
			}
			err := run(t.Context(), []string{"qualify", "-evidence", root, "-revision", testRevision, "-fixture", releaseFixture})
			checkProblem(t, err, tc.problem)
		})
	}
}

func TestQualifyReportsBothMissingArchitectures(t *testing.T) {
	t.Parallel()
	err := qualifyRelease(t.TempDir(), testRevision, releaseFixture)
	checkProblem(t, err, "amd64 build identity")
	checkProblem(t, err, "arm64 build identity")
}
