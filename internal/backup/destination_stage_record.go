package backup

import (
	"bytes"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"strings"
	"syscall"
)

// The permanent lock inode holds a little-endian, two-part v1 record. The
// intent binds the staging name to this lock inode and exact destination key.
// A second, separately synced record binds the stage's filesystem identity.
// An interruption before binding can leave only an empty directory. Recovery
// may remove that directory with rmdir semantics, but never its contents.
const (
	destinationIntentMagic = "BOGDST01"
	destinationBindMagic   = "BOGDIR01"
	destinationIntentSize  = 60
	destinationBindSize    = 28
	destinationRecordSize  = destinationIntentSize + destinationBindSize
)

func (p *destinationPublication) intent() [destinationIntentSize]byte {
	var b [destinationIntentSize]byte
	copy(b[:8], destinationIntentMagic)
	copy(b[8:40], p.key[:])
	dev, ino := destinationIdentity(p.lockInfo)
	little.PutUint64(b[40:48], dev)
	little.PutUint64(b[48:56], ino)
	little.PutUint32(b[56:], crc32.Checksum(b[:56], crcTable))
	return b
}

func (p *destinationPublication) binding(info os.FileInfo) [destinationBindSize]byte {
	var b [destinationBindSize]byte
	copy(b[:8], destinationBindMagic)
	dev, ino := destinationIdentity(info)
	little.PutUint64(b[8:16], dev)
	little.PutUint64(b[16:24], ino)
	c := crc32.Update(0, crcTable, p.key[:])
	little.PutUint32(b[24:], crc32.Update(c, crcTable, b[:24]))
	return b
}

func (p *destinationPublication) writeRecord(b []byte, offset int64) error {
	if err := p.checkLock(); err != nil {
		return err
	}
	if _, err := p.lock.WriteAt(b, offset); err != nil {
		return err
	}
	return p.lock.Sync()
}

func (p *destinationPublication) clearRecord() error {
	if err := p.checkLock(); err != nil {
		return err
	}
	if err := p.lock.Truncate(0); err != nil {
		return err
	}
	return p.lock.Sync()
}

func (p *destinationPublication) reclaim() error {
	if err := p.checkLock(); err != nil {
		return err
	}
	info, err := p.parent.Lstat(p.stageName)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	// The extra byte detects oversized records without an unbounded read.
	var b [destinationRecordSize + 1]byte
	n, err := p.lock.ReadAt(b[:], 0)
	if err != nil && !errors.Is(err, io.EOF) {
		return err
	}
	intent := p.intent()
	if n > destinationRecordSize || !bytes.Equal(b[:min(n, len(intent))], intent[:min(n, len(intent))]) {
		return corrupt("invalid backup staging intent")
	}
	if info == nil {
		if n == 0 {
			return nil
		}
		// Make the absence durable before forgetting the previous attempt.
		if err := syncDirectory(p.parent); err != nil {
			return err
		}
		return p.clearRecord()
	}
	if n < destinationIntentSize || !privateDestinationDirectory(info, p.lockInfo) {
		return corrupt("unowned backup staging directory %q", p.stageName)
	}
	if n == destinationRecordSize {
		binding := p.binding(info)
		if !bytes.Equal(b[destinationIntentSize:n], binding[:]) {
			return corrupt("backup staging directory %q does not match its ownership record", p.stageName)
		}
		// A previous rename may have reported an uncertain outcome. Confirm
		// the observed namespace before deleting the losing copy.
		if err := syncDirectory(p.parent); err != nil {
			return err
		}
		if err := p.removeStageFiles(info); err != nil {
			return err
		}
	} else if n > destinationIntentSize {
		// A short binding can occur before fill starts. Check its available
		// bytes against the observed directory before attempting rmdir.
		binding := p.binding(info)
		if !bytes.Equal(b[destinationIntentSize:n], binding[:n-destinationIntentSize]) {
			return corrupt("invalid partial backup staging binding")
		}
	}
	if err := p.checkLock(); err != nil {
		return err
	}
	if err := matchDestinationEntry(p.parent, p.stageName, info); err != nil {
		return err
	}
	// Never recursively remove a directory. An unbound or changed image
	// with any remaining files is preserved by the empty-directory check.
	if err := p.parent.Remove(p.stageName); err != nil {
		return err
	}
	if err := syncDirectory(p.parent); err != nil {
		return err
	}
	return p.clearRecord()
}

func (p *destinationPublication) removeStageFiles(info os.FileInfo) (err error) {
	stage, err := p.openStage(info)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, stage.Close()) }()
	dir, err := stage.Open(".")
	if err != nil {
		return err
	}
	const maxFiles = segmentLimit + 3
	names, readErr := dir.Readdirnames(maxFiles + 1)
	if err := errors.Join(unexpectedDirRead(readErr), dir.Close()); err != nil {
		return err
	}
	if len(names) > maxFiles {
		return corrupt("too many files in backup staging")
	}
	entries := make([]os.FileInfo, len(names))
	for i, name := range names {
		entry, err := stage.Lstat(name)
		if err != nil {
			return err
		}
		if !destinationImageFile(name) || !privateDestinationFile(entry) {
			return corrupt("unexpected backup staging entry %q", name)
		}
		entries[i] = entry
	}
	if err := p.checkLock(); err != nil {
		return err
	}
	if err := matchDestinationEntry(p.parent, p.stageName, info); err != nil {
		return err
	}
	for i, name := range names {
		if err := matchDestinationEntry(stage, name, entries[i]); err != nil {
			return err
		}
		if err := stage.Remove(name); err != nil {
			return fmt.Errorf("remove backup staging file %q: %w", name, err)
		}
	}
	return nil
}

func destinationImageFile(name string) bool {
	switch name {
	case metadataName, manifestName, tailName:
		return true
	default:
		suffix, ok := strings.CutPrefix(name, segmentPrefix)
		return ok && lowerHex(suffix, 32)
	}
}

func privateDestinationFile(info os.FileInfo) bool {
	st, ok := info.Sys().(*syscall.Stat_t)
	return ok && info.Mode().IsRegular() && info.Mode().Perm()&0077 == 0 &&
		st.Uid == uint32(os.Geteuid()) && st.Nlink == 1
}

func privateDestinationDirectory(info, lock os.FileInfo) bool {
	st, ok := info.Sys().(*syscall.Stat_t)
	return ok && info.IsDir() && info.Mode().Perm()&0077 == 0 &&
		st.Uid == uint32(os.Geteuid()) && st.Dev == lock.Sys().(*syscall.Stat_t).Dev
}

func destinationIdentity(info os.FileInfo) (dev, ino uint64) {
	st := info.Sys().(*syscall.Stat_t)
	return uint64(st.Dev), uint64(st.Ino)
}
