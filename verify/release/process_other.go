//go:build !linux

package main

import "os/exec"

func waitCandidate(cmd *exec.Cmd, cleanGroup func() error) (error, error) {
	err := cmd.Wait()
	cleanGroup() // The teardown caller receives the cached cleanup result.
	return err, nil
}
