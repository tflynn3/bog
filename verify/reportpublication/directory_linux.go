package reportpublication

import (
	"os"
	"runtime"
	"syscall"
	"unsafe"
)

// DescriptorDirectory performs publication relative to a borrowed directory
// descriptor. Its caller keeps the descriptor open throughout publication.
type DescriptorDirectory struct {
	file *os.File
}

func BorrowDirectory(file *os.File) DescriptorDirectory { return DescriptorDirectory{file} }

type directoryOps = DescriptorDirectory

func newDirectoryOps(_ *os.Root, file *os.File) directoryOps { return BorrowDirectory(file) }

func (d DescriptorDirectory) OpenExclusive(name string) (StagingFile, error) {
	fd, err := syscall.Openat(int(d.file.Fd()), name,
		syscall.O_WRONLY|syscall.O_CREAT|syscall.O_EXCL|syscall.O_CLOEXEC, 0600)
	runtime.KeepAlive(d.file)
	if err != nil {
		return nil, &os.PathError{Op: "openat", Path: name, Err: err}
	}
	return os.NewFile(uintptr(fd), name), nil
}

func (d DescriptorDirectory) Rename(from, to string) error {
	fd := int(d.file.Fd())
	err := syscall.Renameat(fd, from, fd, to)
	runtime.KeepAlive(d.file)
	if err != nil {
		return &os.LinkError{Op: "renameat", Old: from, New: to, Err: err}
	}
	return nil
}

func (d DescriptorDirectory) RenameNoReplace(from, to string) error {
	if err := renameNoReplace(d.file, from, to); err != nil {
		return &os.LinkError{Op: "renameat2(RENAME_NOREPLACE)", Old: from, New: to, Err: err}
	}
	return nil
}

func (d DescriptorDirectory) Remove(name string) error {
	err := syscall.Unlinkat(int(d.file.Fd()), name)
	runtime.KeepAlive(d.file)
	if err != nil {
		return &os.PathError{Op: "unlinkat", Path: name, Err: err}
	}
	return nil
}

func (d DescriptorDirectory) Sync() error { return d.file.Sync() }

func renameNoReplace(dir *os.File, old, new string) error {
	oldp, err := syscall.BytePtrFromString(old)
	if err != nil {
		return err
	}
	newp, err := syscall.BytePtrFromString(new)
	if err != nil {
		return err
	}
	// syscall does not export SYS_RENAMEAT2 on every Linux architecture.
	var nr uintptr
	switch runtime.GOARCH {
	case "amd64":
		nr = 316
	case "386":
		nr = 353
	case "arm":
		nr = 382
	case "arm64", "riscv64", "loong64":
		nr = 276
	case "ppc64", "ppc64le":
		nr = 357
	case "s390x":
		nr = 347
	case "mips", "mipsle":
		nr = 4351
	case "mips64", "mips64le":
		nr = 5311
	default:
		return syscall.ENOSYS
	}
	const renameNoReplaceFlag = 1
	fd := dir.Fd()
	_, _, errno := syscall.Syscall6(nr, fd, uintptr(unsafe.Pointer(oldp)), fd, uintptr(unsafe.Pointer(newp)), renameNoReplaceFlag, 0)
	runtime.KeepAlive(dir)
	if errno != 0 {
		return errno
	}
	return nil
}
