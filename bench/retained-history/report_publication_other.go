//go:build !linux

package main

import (
	"errors"
	"fmt"
)

func openProofDestination(string) (proofDestination, error) {
	return proofDestination{}, fmt.Errorf("inherited create-only report directory: %w", errors.ErrUnsupported)
}
