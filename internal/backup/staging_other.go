//go:build !linux

package backup

import "os"

func renameStage(parent *os.Root, _ *os.File, stage, name string, exists bool) error {
	// os.Root.Rename requires an absent directory target on these hosts.
	if exists {
		if err := parent.Remove(name); err != nil {
			return err
		}
	}
	return parent.Rename(stage, name)
}
