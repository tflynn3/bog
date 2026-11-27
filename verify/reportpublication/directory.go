package reportpublication

import (
	"errors"
	"os"
)

// PinnedDirectory owns the directory handles opened by OpenDirectory.
type PinnedDirectory struct {
	directoryOps
	root *os.Root
	file *os.File
}

// OpenDirectory resolves the parent once. The file used for directory sync
// and descriptor handoff is opened relative to that same pinned parent.
func OpenDirectory(path string) (*PinnedDirectory, error) {
	root, err := os.OpenRoot(path)
	if err != nil {
		return nil, err
	}
	file, err := root.Open(".")
	if err != nil {
		return nil, errors.Join(err, root.Close())
	}
	return &PinnedDirectory{newDirectoryOps(root, file), root, file}, nil
}

// File returns the borrowed descriptor for use by a delegated producer.
func (d *PinnedDirectory) File() *os.File { return d.file }

func (d *PinnedDirectory) Lstat(name string) (os.FileInfo, error) { return d.root.Lstat(name) }

func (d *PinnedDirectory) Close() error { return errors.Join(d.file.Close(), d.root.Close()) }
