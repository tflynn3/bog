//go:build !linux

package reportpublication

import "os"

type directoryOps struct {
	root *os.Root
	file *os.File
}

func newDirectoryOps(root *os.Root, file *os.File) directoryOps { return directoryOps{root, file} }

func (d directoryOps) OpenExclusive(name string) (StagingFile, error) {
	return d.root.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
}

func (d directoryOps) Rename(from, to string) error { return d.root.Rename(from, to) }
func (d directoryOps) Remove(name string) error     { return d.root.Remove(name) }
func (d directoryOps) Sync() error                  { return d.file.Sync() }
