package backup

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"strings"
	"syscall"
	"time"
)

const (
	stagingPrefix  = ".bog-backup-v1-"
	stagePrefix    = stagingPrefix + "stage-"
	stagingGuard   = stagingPrefix + "lock"
	stageNonceSize = 16
	stageOwnerSize = 20
)

// stageClaim borrows the pinned parent handles for one destination.
type stageClaim struct {
	parent      *os.Root
	dir         *os.File
	parentInfo  os.FileInfo
	destination string
	key         string
}

func newStageClaim(parent *os.Root, dir *os.File, info os.FileInfo, name string) stageClaim {
	return stageClaim{
		parent: parent, dir: dir, parentInfo: info, destination: name,
		key: fmt.Sprintf("%sclaim-%x", stagingPrefix, sha256.Sum256([]byte(name))),
	}
}

// stageAttempt carries the locked stage descriptor returned to the publisher.
type stageAttempt struct {
	parent *os.Root
	file   *os.File // exclusive flock held from bootstrap through finalization
	name   string
	info   os.FileInfo
}

// The backup guard serializes claim lookup, lock acquisition and removal. A
// publisher drops it during copying, keeping the narrower stage lock.
func (c stageClaim) begin(ctx context.Context, guard stagingGuardHandle) (stageAttempt, error) {
	for {
		if err := guard.lock(ctx); err != nil {
			return stageAttempt{}, err
		}
		attempt, busy, err := c.beginLocked(ctx)
		if unlockErr := unlock(guard.file); unlockErr != nil {
			if attempt.file != nil {
				unlockErr = errors.Join(unlockErr, attempt.file.Close())
			}
			return stageAttempt{}, errors.Join(err, unlockErr)
		}
		if !busy || err != nil {
			return attempt, err
		}
		if err := waitForPublisher(ctx); err != nil {
			return stageAttempt{}, err
		}
	}
}

func (c stageClaim) beginLocked(ctx context.Context) (stageAttempt, bool, error) {
	busy, err := c.recover()
	if busy || err != nil {
		return stageAttempt{}, busy, err
	}
	if _, err := emptyDestination(c.parent, c.destination); err != nil {
		return stageAttempt{}, false, err
	}
	if err := ctx.Err(); err != nil {
		return stageAttempt{}, false, err
	}
	attempt, err := c.create()
	return attempt, false, err
}

func (c stageClaim) create() (_ stageAttempt, err error) {
	var nonce [stageNonceSize]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return stageAttempt{}, err
	}
	token := hex.EncodeToString(nonce[:])
	name := c.stageName(token)
	if _, err := c.parent.Lstat(name); !errors.Is(err, os.ErrNotExist) {
		if err == nil {
			err = os.ErrExist
		}
		return stageAttempt{}, fmt.Errorf("reserve backup stage: %w", err)
	}
	// The intent is atomic and durable before making its fresh directory.
	if err := c.parent.Symlink(token, c.key); err != nil {
		return stageAttempt{}, fmt.Errorf("reserve backup staging claim: %w", err)
	}
	cleanup := true
	defer func() {
		if err != nil && cleanup {
			_, recoveryErr := c.recover()
			err = errors.Join(err, recoveryErr)
		}
	}()
	if err := c.dir.Sync(); err != nil {
		return stageAttempt{}, err
	}
	if err := c.parent.Mkdir(name, 0700); err != nil {
		if errors.Is(err, os.ErrExist) {
			cleanup = false
			return stageAttempt{}, errors.Join(err, c.clearClaim(false))
		}
		return stageAttempt{}, err
	}
	if err := c.parent.Chmod(name, 0700); err != nil {
		return stageAttempt{}, err
	}
	info, err := c.parent.Lstat(name)
	if err != nil {
		return stageAttempt{}, err
	}
	identity, err := c.identity(token, info)
	if err != nil {
		return stageAttempt{}, err
	}
	if err := c.parent.Symlink(identity, c.key+".owner"); err != nil {
		return stageAttempt{}, err
	}
	f, err := openStageDirectory(c.parent, name, info)
	if err != nil {
		return stageAttempt{}, err
	}
	defer func() {
		if err != nil {
			err = errors.Join(err, f.Close())
		}
	}()
	if err := tryLock(f); err != nil {
		return stageAttempt{}, err
	}
	if err := c.dir.Sync(); err != nil {
		return stageAttempt{}, err
	}
	return stageAttempt{c.parent, f, name, info}, nil
}

func (c stageClaim) recover() (busy bool, err error) {
	token, err := readStagingLink(c, c.key, hex.EncodedLen(stageNonceSize))
	if errors.Is(err, os.ErrNotExist) {
		// A standalone owner entry has no matching claim and must be
		// preserved. Refuse to start a transaction in that namespace.
		if _, err := c.parent.Lstat(c.key + ".owner"); !errors.Is(err, os.ErrNotExist) {
			if err == nil {
				err = errors.New("backup staging owner has no claim")
			}
			return false, err
		}
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("read backup staging claim: %w", err)
	}
	var nonce [stageNonceSize]byte
	if len(token) != hex.EncodedLen(len(nonce)) {
		return false, errors.New("invalid backup staging claim")
	}
	if _, err := hex.Decode(nonce[:], []byte(token)); err != nil || hex.EncodeToString(nonce[:]) != token {
		return false, errors.New("invalid backup staging claim")
	}
	name := c.stageName(token)
	owner, err := readStagingLink(c, c.key+".owner", hex.EncodedLen(stageOwnerSize))
	hasOwner := err == nil
	if !hasOwner && !errors.Is(err, os.ErrNotExist) {
		return false, fmt.Errorf("read backup staging owner: %w", err)
	}
	if hasOwner && !c.validStageOwner(token, owner) {
		return false, errors.New("invalid backup staging owner")
	}
	info, err := c.parent.Lstat(name)
	if errors.Is(err, os.ErrNotExist) {
		return false, c.clear(name, nil, hasOwner)
	}
	if err != nil {
		return false, err
	}
	identity, err := c.identity(token, info)
	if err != nil {
		return false, err
	}
	if !hasOwner {
		// The writer cannot have copied anything without the owner
		// record. Remove only an empty directory in this bootstrap state.
		if err := c.parent.Remove(name); err != nil {
			return false, fmt.Errorf("remove unfinished backup staging reservation: %w", err)
		}
		return false, c.clear(name, nil, false)
	}
	if identity != owner {
		return false, errors.New("backup staging directory does not match its owner record")
	}
	f, err := openStageDirectory(c.parent, name, info)
	if err != nil {
		return false, err
	}
	defer func() { err = errors.Join(err, f.Close()) }()
	if err := tryLock(f); err != nil {
		if errors.Is(err, syscall.EWOULDBLOCK) {
			return true, nil
		}
		return false, err
	}
	return false, c.clear(name, info, true)
}

func (c stageClaim) identity(token string, info os.FileInfo) (string, error) {
	st, ok := info.Sys().(*syscall.Stat_t)
	parent, parentOK := c.parentInfo.Sys().(*syscall.Stat_t)
	if !ok || !parentOK || !info.IsDir() || st.Uid != uint32(os.Geteuid()) ||
		st.Dev != parent.Dev || info.Mode().Perm()&0077 != 0 {
		return "", errors.New("unsafe backup staging directory")
	}
	var identity [stageOwnerSize]byte
	binary.LittleEndian.PutUint64(identity[:8], uint64(st.Dev))
	binary.LittleEndian.PutUint64(identity[8:16], uint64(st.Ino))
	binary.LittleEndian.PutUint32(identity[16:], c.ownerChecksum(token, identity[:16]))
	return hex.EncodeToString(identity[:]), nil
}

func (c stageClaim) stageName(token string) string {
	return stagePrefix + strings.TrimPrefix(c.key, stagingPrefix+"claim-") + "-" + token
}

func (c stageClaim) ownerChecksum(token string, identity []byte) uint32 {
	checksum := crc32.Update(0, crcTable, []byte(c.stageName(token)))
	return crc32.Update(checksum, crcTable, identity)
}

func (c stageClaim) validStageOwner(token, owner string) bool {
	var identity [stageOwnerSize]byte
	if len(owner) != hex.EncodedLen(len(identity)) {
		return false
	}
	_, err := hex.Decode(identity[:], []byte(owner))
	return err == nil && hex.EncodeToString(identity[:]) == owner &&
		binary.LittleEndian.Uint32(identity[16:]) == c.ownerChecksum(token, identity[:16])
}

func openStageDirectory(parent *os.Root, name string, expected os.FileInfo) (*os.File, error) {
	f, err := parent.OpenFile(name, os.O_RDONLY|syscall.O_DIRECTORY|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return nil, err
	}
	info, err := f.Stat()
	if err == nil && !os.SameFile(info, expected) {
		err = errors.New("backup staging directory changed during open")
	}
	if err != nil {
		return nil, errors.Join(err, f.Close())
	}
	return f, nil
}

func checkStageIdentity(parent *os.Root, name string, expected os.FileInfo) error {
	info, err := parent.Lstat(name)
	if err != nil {
		return err
	}
	if !os.SameFile(info, expected) {
		return errors.New("backup staging directory changed")
	}
	return nil
}

func (c stageClaim) clear(name string, expected os.FileInfo, hasOwner bool) error {
	if expected != nil {
		if err := checkStageIdentity(c.parent, name, expected); err != nil {
			return err
		}
		if err := c.parent.RemoveAll(name); err != nil {
			return err
		}
	}
	// Make absence of the staging name durable before removing the proof
	// needed to reclaim it. This also seals a rename left by a dead process.
	if err := c.dir.Sync(); err != nil {
		return err
	}
	return c.clearClaim(hasOwner)
}

func (c stageClaim) clearClaim(hasOwner bool) error {
	if hasOwner {
		if err := c.parent.Remove(c.key + ".owner"); err != nil {
			return err
		}
		// An interrupted cleanup may leave the intent alone. Keep the
		// opposite state from being made durable by an unclean shutdown.
		if err := c.dir.Sync(); err != nil {
			return err
		}
	}
	if err := c.parent.Remove(c.key); err != nil {
		return err
	}
	return c.dir.Sync()
}

func lockFile(ctx context.Context, file *os.File) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		err := tryLock(file)
		if !errors.Is(err, syscall.EWOULDBLOCK) {
			return err
		}
		if err := waitForPublisher(ctx); err != nil {
			return err
		}
	}
}

func waitForPublisher(ctx context.Context) error {
	timer := time.NewTimer(10 * time.Millisecond)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func tryLock(file *os.File) error {
	return flock(file, syscall.LOCK_EX|syscall.LOCK_NB)
}

func unlock(file *os.File) error {
	return flock(file, syscall.LOCK_UN)
}

func flock(file *os.File, how int) error {
	for {
		err := syscall.Flock(int(file.Fd()), how)
		if err != syscall.EINTR {
			return err
		}
	}
}
