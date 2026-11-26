package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"syscall"
)

const fixtureID = "linux-release-v1"

var revisionPattern = regexp.MustCompile(`^[0-9a-f]{40}$`)

type report struct {
	SchemaVersion int                `json:"schema_version"`
	Revision      string             `json:"revision"`
	BinarySHA256  string             `json:"binary_sha256"`
	Fixture       string             `json:"fixture"`
	Cases         map[string]outcome `json:"cases"`
}

type outcome struct {
	Status   string `json:"status"`
	Detail   string `json:"detail,omitempty"`
	Evidence any    `json:"evidence,omitempty"`
}

func newReport(binary, revision, fixture string) (report, error) {
	if !filepath.IsAbs(binary) || !revisionPattern.MatchString(revision) || fixture != fixtureID {
		return report{}, fmt.Errorf("require absolute -binary path, full lowercase -revision SHA and -fixture %s", fixtureID)
	}
	f, err := os.OpenFile(binary, os.O_RDONLY|syscall.O_NONBLOCK, 0)
	if err != nil {
		return report{}, err
	}
	stat, err := f.Stat()
	if err == nil && (!stat.Mode().IsRegular() || stat.Mode().Perm()&0111 == 0 || stat.Size() == 0) {
		err = errors.New("candidate must be a nonempty executable regular file")
	}
	if err != nil {
		return report{}, errors.Join(err, f.Close())
	}
	hash := sha256.New()
	n, err := io.Copy(hash, f)
	if err := errors.Join(err, f.Close()); err != nil {
		return report{}, err
	}
	if n != stat.Size() {
		return report{}, errors.New("candidate changed while hashing")
	}
	return report{1, revision, hex.EncodeToString(hash.Sum(nil)), fixture, make(map[string]outcome)}, nil
}

func (r report) record(name string, evidence any, err error) error {
	if err != nil {
		r.Cases[name] = outcome{"fail", err.Error(), evidence}
		return fmt.Errorf("%s: %w", name, err)
	}
	r.Cases[name] = outcome{Status: "pass", Evidence: evidence}
	return nil
}

func (r report) write(path string) error {
	data, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(data, '\n'), 0600)
}
