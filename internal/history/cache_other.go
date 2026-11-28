//go:build !linux || (!amd64 && !arm64)

package history

import "os"

func discardValidationCache(*os.File) {}
