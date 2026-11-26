package backup

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"syscall"
)

type directory struct {
	root *os.Root
	fd   *os.File
}

func openDirectory(path string) (*directory, error) {
	root, err := os.OpenRoot(path)
	if err != nil {
		return nil, err
	}
	return directoryFromRoot(root)
}

func directoryFromRoot(root *os.Root) (*directory, error) {
	fd, err := root.Open(".")
	if err != nil {
		return nil, errors.Join(err, root.Close())
	}
	return &directory{root, fd}, nil
}

func (d *directory) Close() error {
	return errors.Join(d.fd.Close(), d.root.Close())
}

func joinClose(err *error, c io.Closer) {
	if e := c.Close(); e != nil {
		*err = errors.Join(*err, e)
	}
}

func (d *directory) openFile(name string) (*os.File, os.FileInfo, error) {
	fd, err := syscall.Openat(int(d.fd.Fd()), name, syscall.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK|syscall.O_CLOEXEC, 0)
	if err != nil {
		return nil, nil, fmt.Errorf("open %s: %w", name, err)
	}
	f := os.NewFile(uintptr(fd), name)
	info, err := f.Stat()
	if err == nil {
		st, ok := info.Sys().(*syscall.Stat_t)
		// A pinned immutable input can be unlinked after a concurrent rename.
		if !info.Mode().IsRegular() || !ok || st.Nlink > 1 || st.Uid != uint32(os.Geteuid()) || info.Mode().Perm()&0022 != 0 {
			err = fmt.Errorf("unsafe input %s", name)
		}
	}
	if err != nil {
		return nil, nil, errors.Join(err, f.Close())
	}
	return f, info, nil
}

func (d *directory) sameFile(name string, captured os.FileInfo) (bool, error) {
	current, err := d.root.Lstat(name)
	if errors.Is(err, os.ErrNotExist) {
		return captured == nil, nil
	}
	if err != nil {
		return false, err
	}
	return captured != nil && os.SameFile(current, captured), nil
}

func (d *directory) read(name string, max int) (_ []byte, err error) {
	f, info, err := d.openFile(name)
	if err != nil {
		return nil, err
	}
	defer joinClose(&err, f)
	return boundedRead(f, info, max)
}

func boundedRead(f *os.File, info os.FileInfo, max int) ([]byte, error) {
	if info.Size() < 0 || info.Size() > int64(max) {
		return nil, fmt.Errorf("invalid size of %s", f.Name())
	}
	b := make([]byte, int(info.Size()))
	if _, err := io.ReadFull(f, b); err != nil {
		return nil, err
	}
	var extra [1]byte
	if _, err := io.ReadFull(f, extra[:]); err != io.EOF {
		if err != nil {
			return nil, err
		}
		return nil, fmt.Errorf("size changed while reading %s", f.Name())
	}
	return b, nil
}

func (d *directory) output(name string, write func(io.Writer) error) (err error) {
	if d == nil {
		return write(io.Discard)
	}
	f, err := d.root.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	defer joinClose(&err, f)
	if err := write(f); err != nil {
		return fmt.Errorf("write %s: %w", name, err)
	}
	return f.Sync()
}

func (d *directory) write(name string, b []byte) error {
	return d.output(name, func(w io.Writer) error {
		_, err := io.Copy(w, bytes.NewReader(b))
		return err
	})
}

func publish(ctx context.Context, destination string, build func(*directory) error) (err error) {
	if err := ctx.Err(); err != nil {
		return err
	}
	path := filepath.Clean(destination)
	name := filepath.Base(path)
	if destination == "" || name == "." || name == ".." || name == "/" {
		return errors.New("destination must name a child directory")
	}
	parent, err := openDirectory(filepath.Dir(path))
	if err != nil {
		return err
	}
	defer joinClose(&err, parent)

	target, err := parent.root.Lstat(name)
	if err == nil {
		if !target.IsDir() {
			return errors.New("destination must be an empty directory")
		}
		root, err := parent.root.OpenRoot(name)
		if err != nil {
			return err
		}
		dir, err := directoryFromRoot(root)
		if err != nil {
			return err
		}
		defer joinClose(&err, dir)
		info, err := dir.fd.Stat()
		if err != nil {
			return err
		}
		if !os.SameFile(info, target) {
			return errors.New("destination changed")
		}
		if err := syscall.Flock(int(dir.fd.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
			return err
		}
		if _, err := dir.fd.Readdirnames(1); err != io.EOF {
			if err != nil {
				return err
			}
			return errors.New("destination directory is not empty")
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}

	var random [16]byte
	if _, err := rand.Read(random[:]); err != nil {
		return err
	}
	temp := ".bog-backup-stage-" + hex.EncodeToString(random[:])
	if err := parent.root.Mkdir(temp, 0700); err != nil {
		return err
	}
	attempted := false
	defer func() {
		if !attempted {
			if e := parent.root.RemoveAll(temp); e != nil {
				err = errors.Join(err, e)
			}
		}
	}()

	if err := buildStage(parent, temp, build); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	same, err := parent.sameFile(name, target)
	if err != nil {
		return err
	}
	if !same {
		return errors.New("destination changed before publication")
	}
	// Rename installs the complete tree in one operation. After an ambiguous
	// failure, leave both names alone and require inspection before retry.
	attempted = true
	return installDirectory(
		func() error {
			fd := int(parent.fd.Fd())
			return syscall.Renameat(fd, temp, fd, name)
		},
		parent.fd.Sync,
	)
}

func buildStage(parent *directory, temp string, build func(*directory) error) (err error) {
	root, err := parent.root.OpenRoot(temp)
	if err != nil {
		return err
	}
	stage, err := directoryFromRoot(root)
	if err != nil {
		return err
	}
	defer joinClose(&err, stage)
	if err := build(stage); err != nil {
		return err
	}
	return stage.fd.Sync()
}

func installDirectory(rename, sync func() error) error {
	if err := rename(); err != nil {
		return fmt.Errorf("%w: rename destination: %w", ErrPublicationUncertain, err)
	}
	if err := sync(); err != nil {
		return fmt.Errorf("%w: sync destination parent: %w", ErrPublicationUncertain, err)
	}
	return nil
}
