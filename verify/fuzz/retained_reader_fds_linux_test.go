package fuzz

import "path/filepath"

func retainedReaderFDs(path string) ([]string, error) {
	canonical, err := filepath.EvalSymlinks(path)
	if err != nil {
		return nil, err
	}
	return scanMemoryFixtureFDs("/proc/self/fd", canonical)
}
