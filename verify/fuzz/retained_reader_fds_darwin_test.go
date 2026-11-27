//go:build cgo

package fuzz

// Darwin runs the instrumented view-descriptor checks. The additional
// inventory of package-internal descriptors uses Linux procfs.
func retainedReaderFDs(string) ([]string, error) { return nil, nil }
