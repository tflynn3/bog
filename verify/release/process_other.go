//go:build !linux

package main

import "errors"

func candidatePlatform() error {
	return errors.New("candidate lifecycle and listener verification require Linux")
}

func candidateExited(_ int, _ bool) (bool, error) {
	return false, candidatePlatform()
}

func candidateOwnsListener(_, _ int) (bool, error) {
	return false, candidatePlatform()
}
