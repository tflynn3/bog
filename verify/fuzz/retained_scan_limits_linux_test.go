package fuzz

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"strconv"
	"strings"
	"syscall"
)

const scanMemoryProcBytes = 64 << 10

func scanMemoryAddressLimit(baseline uint64, inherited syscall.Rlimit) (uint64, error) {
	if baseline == 0 || baseline > math.MaxUint64-scanMemoryAddressGrowth {
		return 0, fmt.Errorf("invalid startup address space %d", baseline)
	}
	limit := baseline + scanMemoryAddressGrowth
	if available := min(inherited.Cur, inherited.Max); available < limit {
		return 0, fmt.Errorf("startup address space %d plus %d bytes of mapping growth requires %d, inherited limit is %d",
			baseline, scanMemoryAddressGrowth, limit, available)
	}
	return limit, nil
}

func prepareScanMemoryWorker() (scanMemoryStartup, error) {
	baseline, err := readScanMemoryStatus("VmSize")
	if err != nil {
		return scanMemoryStartup{}, err
	}
	var inherited syscall.Rlimit
	if err := syscall.Getrlimit(syscall.RLIMIT_AS, &inherited); err != nil {
		return scanMemoryStartup{}, fmt.Errorf("get RLIMIT_AS: %w", err)
	}
	limit, err := scanMemoryAddressLimit(baseline, inherited)
	if err != nil {
		return scanMemoryStartup{}, err
	}
	if err := syscall.Setrlimit(syscall.RLIMIT_AS, &syscall.Rlimit{Cur: limit, Max: limit}); err != nil {
		return scanMemoryStartup{}, fmt.Errorf("set RLIMIT_AS: %w", err)
	}
	if err := syscall.Getrlimit(syscall.RLIMIT_FSIZE, &inherited); err != nil {
		return scanMemoryStartup{}, fmt.Errorf("get RLIMIT_FSIZE: %w", err)
	}
	fileLimit := min(inherited.Cur, inherited.Max, scanMemoryFileLimit)
	if fileLimit == 0 {
		return scanMemoryStartup{}, errors.New("inherited RLIMIT_FSIZE prevents fixture writes")
	}
	if err := syscall.Setrlimit(syscall.RLIMIT_FSIZE, &syscall.Rlimit{Cur: fileLimit, Max: fileLimit}); err != nil {
		return scanMemoryStartup{}, fmt.Errorf("set RLIMIT_FSIZE: %w", err)
	}
	return scanMemoryStartup{AddressBytes: baseline, AddressLimit: limit, FileLimit: fileLimit}, nil
}

func readScanMemoryStatus(field string) (_ uint64, err error) {
	f, err := os.Open("/proc/self/status")
	if err != nil {
		return 0, err
	}
	defer func() { err = errors.Join(err, f.Close()) }()
	return parseScanMemoryStatus(f, field)
}

func parseScanMemoryStatus(r io.Reader, field string) (uint64, error) {
	input := &io.LimitedReader{R: r, N: scanMemoryProcBytes + 1}
	scanner := bufio.NewScanner(input)
	scanner.Buffer(make([]byte, 1024), scanMemoryProcBytes)
	prefix := field + ":"
	var result uint64
	for scanner.Scan() {
		value, ok := strings.CutPrefix(scanner.Text(), prefix)
		if !ok {
			continue
		}
		if result != 0 {
			return 0, fmt.Errorf("duplicate %s in worker /proc/self/status", field)
		}
		fields := strings.Fields(value)
		if len(fields) != 2 || fields[1] != "kB" {
			return 0, fmt.Errorf("invalid %s in worker /proc/self/status", field)
		}
		kib, err := strconv.ParseUint(fields[0], 10, 54)
		if err != nil || kib == 0 {
			return 0, fmt.Errorf("invalid %s value in worker /proc/self/status", field)
		}
		result = kib * 1024
	}
	if err := scanner.Err(); err != nil {
		return 0, fmt.Errorf("read worker /proc/self/status: %w", err)
	}
	if input.N == 0 {
		return 0, fmt.Errorf("%w: worker /proc/self/status exceeds %d bytes", errReplayLimit, scanMemoryProcBytes)
	}
	if result == 0 {
		return 0, fmt.Errorf("missing %s in worker /proc/self/status", field)
	}
	return result, nil
}
