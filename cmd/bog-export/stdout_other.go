//go:build !linux

package main

import "os"

func openStdoutPipe(*os.File) (*os.File, error) {
	return nil, nil
}
