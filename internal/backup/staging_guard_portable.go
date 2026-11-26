//go:build !linux && !darwin

package backup

import "os"

// Permission repair on other hosts relies on the trusted parent namespace.
func repairStagingGuardMode(parent *os.Root, _ *os.File, parentInfo, expected os.FileInfo) error {
	if err := checkStagingGuardEntry(parent, parentInfo, expected); err != nil {
		return err
	}
	if err := parent.Chmod(stagingGuard, stagingGuardMode); err != nil {
		return err
	}
	return checkStagingGuardEntry(parent, parentInfo, expected)
}
