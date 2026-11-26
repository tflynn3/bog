//go:build !linux

package httpapi

// The WAL and its uncertain commit outcome are only available on Linux.
func isUncertainAppend(error) bool { return false }
