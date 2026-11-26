//go:build !linux || (!amd64 && !arm64)

package main

import "os"

func prepareCache(*os.File) string {
	return "warm or unverified (benchmark file written or queried without eviction)"
}
