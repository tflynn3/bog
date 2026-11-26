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
	"runtime"

	"bog/internal/checkpoint"
	"github.com/klauspost/compress/zstd"
	"golang.org/x/sys/unix"
)

func measure(path string, size int64, q query, want []checkpoint.BlockRow, totalRows, runs int) (_ measurement, err error) {
	f, err := os.Open(path)
	if err != nil {
		return measurement{}, err
	}
	defer func() { err = errors.Join(err, f.Close()) }()
	fd := int(f.Fd())
	if err := unix.Fadvise(fd, 0, 0, unix.FADV_RANDOM); err != nil {
		return measurement{}, fmt.Errorf("disable readahead: %w", err)
	}
	return measureQuery(f, size, q, want, totalRows, runs, func() error {
		if err := unix.Fadvise(fd, 0, 0, unix.FADV_DONTNEED); err != nil {
			return fmt.Errorf("evict file pages: %w", err)
		}
		return nil
	})
}

func collect(cfg config) (_ report, err error) {
	dir, err := os.MkdirTemp(cfg.dir, "bog-segment-layout-")
	if err != nil {
		return report{}, err
	}
	defer func() { err = errors.Join(err, os.RemoveAll(dir)) }()
	var fs unix.Statfs_t
	if err := unix.Statfs(dir, &fs); err != nil {
		return report{}, err
	}
	var kernel unix.Utsname
	if err := unix.Uname(&kernel); err != nil {
		return report{}, err
	}
	digest, err := executableDigest()
	if err != nil {
		return report{}, fmt.Errorf("fingerprint benchmark binary: %w", err)
	}
	env := reportEnvironment{
		OS: runtime.GOOS, Arch: runtime.GOARCH, Machine: unix.ByteSliceToString(kernel.Machine[:]),
		GoVersion: runtime.Version(), BuildRevision: sourceRevision(), BinarySHA256: digest,
		CPUs: runtime.NumCPU(), GOMAXPROCS: runtime.GOMAXPROCS(0), Kernel: unix.ByteSliceToString(kernel.Release[:]),
		FilesystemMagic: fmt.Sprintf("%#x", fs.Type), FilesystemBlockBytes: fs.Bsize,
		CachePolicy: cachePolicy, WorkDirectory: filepath.Dir(dir),
		ZstdLevel: zstd.SpeedDefault.String(), ZstdWindowBytes: zstdWindow,
	}
	return collectIn(dir, cfg, env, measure)
}

func executableDigest() (_ string, err error) {
	path, err := os.Executable()
	if err != nil {
		return "", err
	}
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer func() { err = errors.Join(err, f.Close()) }()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

func run(cfg config, out io.Writer) error {
	result, err := collect(cfg)
	if err != nil {
		return err
	}
	return json.NewEncoder(out).Encode(result)
}
