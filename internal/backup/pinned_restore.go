package backup

import (
	"context"
	"io"
	"os"
)

// VerifyPinnedRestore validates a complete v1 or v2 restore using a borrowed root.
// The caller keeps the image stable during verification and the verified tail
// contents stable while reading them, typically under the recovery lock.
//
// After validating the metadata and manifest, the verifier calls checkReplay,
// when provided, with the logical interval [CoveredOffset, CommittedEnd).
// The check may reject the range before the verifier opens or reads the suffix.
// Its error is returned directly.
//
// On success the caller owns the returned tail and must close it. ReadAt uses
// absolute WAL offsets in the verified interval on the same file descriptor
// used to verify the tail and returns io.EOF for reads outside that interval.
// The tail can outlive the directory root. The root remains caller-owned and
// errors return a zero point and a nil tail.
func VerifyPinnedRestore(ctx context.Context, root *os.Root, checkReplay func(start, end int64) error) (Point, WALReader, error) {
	point, tail, err := VerifyPinnedRecovery(ctx, root, checkReplay)
	if err != nil {
		return Point{}, nil, err
	}
	return Point{point.Generation, point.CoveredOffset, point.NextOrdinal, point.CommittedEnd}, tail, nil
}

type verifiedTail struct {
	file       *os.File
	start, end int64
}

func (t *verifiedTail) ReadAt(p []byte, off int64) (int, error) {
	if off < t.start || off >= t.end {
		return 0, io.EOF
	}
	remaining := t.end - off
	if int64(len(p)) > remaining {
		n, err := t.file.ReadAt(p[:remaining], off-t.start)
		if err != nil {
			return n, err
		}
		return n, io.EOF
	}
	return t.file.ReadAt(p, off-t.start)
}

func (t *verifiedTail) Close() error { return t.file.Close() }
