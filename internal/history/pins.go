package history

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"syscall"

	"bog/internal/checkpoint"
)

const (
	pinPrefix      = ".bog-history-pin-"
	pinStagePrefix = ".bog-history-pin-stage-"
	pinTokenBytes  = 16
	pinTokenSize   = pinTokenBytes * 2
	pinMagic       = "BOGPIN01"
	pinOverhead    = len(pinMagic) + sha256.Size
	maxPinSize     = pinOverhead + checkpoint.MaxSegments*segmentNameSize
)

// registerPin holds the publication lock shared. The staging name reserves
// the token until rename and lets reclamation recognize interrupted writes.
func (g *generation) registerPin(ctx context.Context) (err error) {
	if err := ctx.Err(); err != nil {
		return err
	}
	var nonce [pinTokenBytes]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return err
	}
	token := hex.EncodeToString(nonce[:])
	name, staged := pinPrefix+token, pinStagePrefix+token
	dirFD := int(g.dir.Fd())
	fd, err := syscall.Openat(dirFD, staged, syscall.O_RDWR|syscall.O_CREAT|syscall.O_EXCL|syscall.O_NOFOLLOW|syscall.O_CLOEXEC, 0600)
	if err != nil {
		return &os.PathError{Op: "create generation pin", Path: staged, Err: err}
	}
	f := os.NewFile(uintptr(fd), name)
	defer func() {
		if err != nil {
			err = errors.Join(err, unlinkPin(g.dir, staged), f.Close())
		}
	}()
	// All registrants reserve the staging name before checking the active
	// name. No cooperating process can install this token between check and
	// rename, even if the token was generated before.
	existing, _, err := openRegular(g.dir, name)
	if err == nil {
		return errors.Join(&os.PathError{Op: "reserve generation pin", Path: name, Err: os.ErrExist}, existing.Close())
	}
	if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err := f.Chmod(0600); err != nil {
		return err
	}
	if err := syscall.Flock(fd, syscall.LOCK_SH|syscall.LOCK_NB); err != nil {
		return &os.PathError{Op: "lock generation pin", Path: staged, Err: err}
	}
	b := make([]byte, 0, pinOverhead+len(g.segments)*segmentNameSize)
	b = append(b, pinMagic...)
	for _, s := range g.segments {
		b = append(b, s.name...)
	}
	sum := sha256.Sum256(b)
	if _, err := f.Write(append(b, sum[:]...)); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := syscall.Renameat(dirFD, staged, dirFD, name); err != nil {
		return &os.LinkError{Op: "install generation pin", Old: staged, New: name, Err: err}
	}
	g.pin = f
	return nil
}

func (g *generation) releasePin() (err error) {
	defer func() { err = errors.Join(err, g.pin.Close()) }()
	lock, err := checkpoint.LockPublication(g.dir, false)
	if errors.Is(err, syscall.EWOULDBLOCK) {
		// Leave the record for a later exclusive reclamation pass.
		return nil
	}
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, lock.Close()) }()
	return unlinkPin(g.dir, g.pin.Name())
}

func unlinkPin(dir *os.File, name string) error {
	if err := syscall.Unlinkat(int(dir.Fd()), name); err != nil {
		return &os.PathError{Op: "unlink generation pin", Path: name, Err: err}
	}
	return nil
}

func isPinName(name, prefix string) bool {
	token, ok := strings.CutPrefix(name, prefix)
	return ok && len(token) == pinTokenSize && strings.Trim(token, "0123456789abcdef") == ""
}

// protectPinned holds the publication lock exclusively. New pins and graceful
// unlinks cannot race the directory walk. A pin that drains during the walk
// can only cause a segment to be conservatively retained until the next call.
func protectPinned(ctx context.Context, dir *os.File, candidates map[string]os.FileInfo, protected map[string]bool) error {
	buf := make([]byte, maxPinSize)
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		names, err := dir.Readdirnames(64)
		for _, name := range names {
			if err := ctx.Err(); err != nil {
				return err
			}
			switch {
			case isPinName(name, pinStagePrefix):
				if err := unlinkPin(dir, name); err != nil {
					return err
				}
			case isPinName(name, pinPrefix):
				if err := protectPin(dir, name, buf, candidates, protected); err != nil {
					return err
				}
			}
		}
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
	}
}

func protectPin(dir *os.File, name string, buf []byte, candidates map[string]os.FileInfo, protected map[string]bool) (err error) {
	f, info, err := openRegular(dir, name)
	if err != nil {
		return fmt.Errorf("%w: open generation pin: %w", ErrIntegrity, err)
	}
	defer func() { err = errors.Join(err, f.Close()) }()
	err = syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
	if err == nil {
		return unlinkPin(dir, name)
	}
	if !errors.Is(err, syscall.EWOULDBLOCK) {
		return &os.PathError{Op: "lock generation pin", Path: name, Err: err}
	}

	size := info.Size()
	if size < int64(pinOverhead) || size > int64(maxPinSize) || (size-int64(pinOverhead))%int64(segmentNameSize) != 0 {
		return corrupt(name, "invalid generation pin size")
	}
	data := buf[:int(size)]
	if err := readExactAt(f, data, 0); err != nil {
		return fmt.Errorf("%w: read generation pin %s: %w", ErrIntegrity, name, err)
	}
	payload := data[:len(data)-sha256.Size]
	if sha256.Sum256(payload) != [sha256.Size]byte(data[len(payload):]) || string(payload[:len(pinMagic)]) != pinMagic {
		return corrupt(name, "invalid generation pin version or checksum")
	}
	for names := payload[len(pinMagic):]; len(names) != 0; names = names[segmentNameSize:] {
		segmentName := string(names[:segmentNameSize])
		if !validSegmentName(segmentName) {
			return corrupt(name, "invalid generation pin segment")
		}
		if _, retired := candidates[segmentName]; retired {
			protected[segmentName] = true
		}
	}
	return nil
}
