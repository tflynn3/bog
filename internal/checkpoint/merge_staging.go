package checkpoint

import (
	"errors"
	"fmt"
	"io"
	"os"
	"syscall"
)

// The packer addresses staged blocks by stable slots. It may read and replace
// an eligible slot while building the manifest. The attempt owns file names,
// byte charges, and the final set of files recorded in the publication intent.
type stagedBlock struct {
	ownedSegment
	chargeIndex int // Index in the attempt's ledger, or -1 without a budget.
	rewritten   bool
}

func (s stagedBlock) segment() Segment {
	return Segment{Name: s.Name, Size: s.Size, SHA256: s.SHA256}
}

func (s stagedBlock) reusableRoom() int {
	if s.rewritten {
		return 0
	}
	return MaxBlockSize - int(s.Size)
}

func (a *publicationAttempt) ownedFiles() []ownedSegment {
	// Keep the empty intent's v1 encoding.
	if len(a.files) == 0 {
		return nil
	}
	files := make([]ownedSegment, len(a.files))
	for i, s := range a.files {
		files[i] = s.ownedSegment
	}
	return files
}

func (a *publicationAttempt) writeNew(name string, b []byte) error {
	if err := a.pub.stage.reserve(a.ctx, len(b)); err != nil {
		return err
	}
	return a.pub.writeNew(name, b)
}

func (a *publicationAttempt) stageBlock(rows []BlockRow) error {
	if len(a.files) >= MaxSegments {
		return fmt.Errorf("%w: too many staged segments", ErrIntegrity)
	}
	file, err := a.writeBlock(rows, false)
	if err != nil {
		return err
	}
	a.files = append(a.files, file)
	return nil
}

func (a *publicationAttempt) replaceBlock(slot int, rows []BlockRow) error {
	if slot < 0 || slot >= len(a.files) {
		return fmt.Errorf("%w: invalid staged segment slot %d", ErrIntegrity, slot)
	}
	old := a.files[slot]
	if old.reusableRoom() == 0 {
		return fmt.Errorf("%w: staged segment cannot be reused", ErrIntegrity)
	}
	// Until retirement is durable, both reservations belong to the attempt.
	// An error aborts the build before intent; cleanup can discard either
	// name without consulting a slot table or replaying the replacement.
	file, err := a.writeBlock(rows, true)
	if err != nil {
		return err
	}
	if err := a.retireStagedBlock(old); err != nil {
		return err
	}
	file.rewritten = true
	if old.chargeIndex >= 0 {
		// The successor was the most recent reservation. Move it into
		// the retired slot in the cleanup ledger before releasing the old
		// reservation, so settlement visits only outstanding charges.
		charges := a.pub.stage.charges
		retired := charges[old.chargeIndex]
		charges[old.chargeIndex] = charges[file.chargeIndex]
		charges[file.chargeIndex] = nil
		a.pub.stage.charges = charges[:file.chargeIndex]
		file.chargeIndex = old.chargeIndex
		retired.Release()
	}
	a.files[slot] = file
	return nil
}

func (a *publicationAttempt) writeBlock(rows []BlockRow, replacing bool) (stagedBlock, error) {
	if len(rows) == 0 || len(rows) > blockPayload/blockRowSize {
		return stagedBlock{}, errBlock
	}
	size := blockHeaderSize
	for _, row := range rows {
		if len(row.Row.Line) > MaxBlockSize-size-blockRowSize {
			return stagedBlock{}, errBlock
		}
		size += blockRowSize + len(row.Row.Line)
	}
	charge, err := a.reserveBlock(size)
	if err != nil {
		return stagedBlock{}, err
	}
	data, err := EncodeBlock(rows)
	if err != nil {
		return stagedBlock{}, err
	}
	if len(data) != size {
		return stagedBlock{}, fmt.Errorf("%w: encoded block size differs from staging reservation", ErrIntegrity)
	}
	name, err := randomName(segmentPrefix)
	if err != nil {
		return stagedBlock{}, err
	}
	return a.writeSegment(name, data, digest(data), charge, replacing)
}

func (a *publicationAttempt) reserveBlock(size int) (int, error) {
	account := a.pub.stage
	if account == nil {
		return -1, a.ctx.Err()
	}
	charge, err := account.budget.Reserve(a.ctx, StagingBytes, uint64(size))
	if err != nil {
		return -1, err
	}
	// The account also covers partial writes and failed replacements.
	account.charges = append(account.charges, charge)
	return len(account.charges) - 1, nil
}

func (a *publicationAttempt) stageSegment(name string, b []byte, sum string) error {
	if len(a.files) >= MaxSegments {
		return fmt.Errorf("%w: too many staged segments", ErrIntegrity)
	}
	charge, err := a.reserveBlock(len(b))
	if err != nil {
		return err
	}
	file, err := a.writeSegment(name, b, sum, charge, false)
	if err != nil {
		return err
	}
	a.files = append(a.files, file)
	return nil
}

func (a *publicationAttempt) readOriginal(s Segment, ordinalLimit uint64) ([]BlockRow, error) {
	return a.readBlock(a.pub.dir, s, 0, ordinalLimit)
}

func (a *publicationAttempt) readStaged(slot int, ordinalLimit uint64) (_ []BlockRow, err error) {
	s := a.files[slot]
	dir, err := openStaging(a.pub.dir)
	if err != nil {
		return nil, err
	}
	defer func() { err = errors.Join(err, dir.Close()) }()
	return a.readBlock(dir, s.segment(), s.Inode, ordinalLimit)
}

func (a *publicationAttempt) readBlock(dir *os.File, s Segment, inode, ordinalLimit uint64) ([]BlockRow, error) {
	if err := a.ctx.Err(); err != nil {
		return nil, err
	}
	if s.Size < blockHeaderSize+blockRowSize || s.Size > MaxBlockSize {
		return nil, fmt.Errorf("%w: invalid merge input size for %s", ErrIntegrity, s.Name)
	}
	size := int(s.Size)
	if cap(a.scratch) < size {
		a.scratch = make([]byte, size)
	}
	rows, err := readMergeBlock(dir, s, inode, ordinalLimit, a.scratch[:size])
	if err != nil {
		return nil, fmt.Errorf("%w: merge input %s: %w", ErrIntegrity, s.Name, err)
	}
	return rows, nil
}

func readMergeBlock(dir *os.File, s Segment, inode, ordinalLimit uint64, data []byte) (rows []BlockRow, err error) {
	f, err := openPublicationFile(dir, s.Name)
	if err != nil {
		return nil, err
	}
	defer func() { err = errors.Join(err, f.Close()) }()
	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if info.Size() != s.Size || (inode != 0 && info.Sys().(*syscall.Stat_t).Ino != inode) {
		return nil, errors.New("segment identity or size changed")
	}
	if _, err := io.ReadFull(f, data); err != nil {
		return nil, err
	}
	if digest(data) != s.SHA256 {
		return nil, errors.New("segment checksum changed")
	}
	rows, err = DecodeBlock(data)
	if err != nil {
		return nil, err
	}
	for i, row := range rows {
		if row.Ordinal >= ordinalLimit || (i != 0 && !lessRow(rows[i-1], row)) {
			return nil, errors.New("invalid segment row order or ordinal")
		}
	}
	return rows, nil
}
