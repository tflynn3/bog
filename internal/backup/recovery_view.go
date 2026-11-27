package backup

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
)

// RecoveryPoint identifies one pinned generation and exact committed suffix.
// Offsets are logical WAL offsets and both ordinals are first-free.
type RecoveryPoint struct {
	Generation       [32]byte
	CoveredOffset    int64
	NextOrdinal      uint64
	CommittedEnd     int64
	CommittedOrdinal uint64
	SuffixSHA256     [32]byte
}

// RecoveryView is held by the caller through export and error cleanup.
// Each successful open transfers an independent reader. Byte zero of
// OpenSuffix corresponds to CoveredOffset.
type RecoveryView interface {
	Point() RecoveryPoint
	OpenManifest() (io.ReadCloser, error)
	OpenSegment(name string) (io.ReadCloser, error)
	OpenSuffix() (io.ReadCloser, error)
}

type segmentOpener interface {
	OpenSegment(name string) (io.ReadCloser, error)
}

// ExportRecoveryView publishes a v2 bundle of the captured generation and
// suffix. The limit is inclusive and checked before opening any view reader.
// Destination publication uses the same protocol as Export and Restore.
func ExportRecoveryView(ctx context.Context, view RecoveryView, destination string, maxSuffixBytes uint64) error {
	return exportRecoveryView(ctx, view, destination, maxSuffixBytes, publish)
}

func exportRecoveryView(ctx context.Context, view RecoveryView, destination string, maxSuffixBytes uint64, publish publisher) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if view == nil {
		return errors.New("backup recovery view is required")
	}
	point := view.Point()
	m := metadata{
		Point: Point{Generation: point.Generation, CoveredOffset: point.CoveredOffset,
			NextOrdinal: point.NextOrdinal, CommittedEnd: point.CommittedEnd},
		tailSum: point.SuffixSHA256, v2: true, committedOrdinal: point.CommittedOrdinal,
	}
	if !validSuffixBounds(m.Point, m.committedOrdinal) {
		return corrupt("invalid captured recovery point")
	}
	if uint64(m.tailSize()) > maxSuffixBytes {
		return fmt.Errorf("backup suffix exceeds limit of %d bytes", maxSuffixBytes)
	}
	return publish(ctx, destination, func(stage *os.Root) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		r, err := view.OpenManifest()
		if err != nil {
			return fmt.Errorf("open captured manifest: %w", err)
		}
		b, err := readLimited(cancellableReadCloser{contextReader{ctx, r}, r}, manifestLimit)
		if err != nil {
			return fmt.Errorf("read captured manifest: %w", err)
		}
		generation, err := readManifest(b, m.Point, true)
		if err != nil {
			return err
		}
		if err := stageGeneration(ctx, stage, view, b, generation); err != nil {
			return err
		}
		if err := exportSuffix(ctx, view, stage, m); err != nil {
			return err
		}
		return writeBytes(stage, metadataName, m.encode())
	})
}

type cancellableReadCloser struct {
	io.Reader
	io.Closer
}

func stageGeneration(ctx context.Context, stage *os.Root, source segmentOpener, b []byte, m manifest) error {
	if err := writeBytes(stage, manifestName, b); err != nil {
		return err
	}
	for _, s := range m.Segments {
		if err := ctx.Err(); err != nil {
			return err
		}
		r, err := source.OpenSegment(s.Name)
		if err != nil {
			return fmt.Errorf("open captured segment %s: %w", s.Name, err)
		}
		if err := copyFile(ctx, stage, s.Name, r, s.Size); err != nil {
			return err
		}
	}
	_, err := validateSegments(ctx, stage, m)
	return err
}

func exportSuffix(ctx context.Context, view RecoveryView, stage *os.Root, m metadata) (err error) {
	if err := ctx.Err(); err != nil {
		return err
	}
	r, err := view.OpenSuffix()
	if err != nil {
		return fmt.Errorf("open captured suffix: %w", err)
	}
	defer func() { err = errors.Join(err, r.Close()) }()
	source := contextReader{ctx, r}
	return writeFile(stage, tailName, func(w io.Writer) error {
		if _, err := verifySuffix(ctx, io.TeeReader(source, w), m, nil); err != nil {
			return err
		}
		var extra [1]byte
		n, err := source.Read(extra[:])
		switch {
		case n != 0:
			return corrupt("extra bytes in captured suffix")
		case err == io.EOF:
			return ctx.Err()
		case err != nil:
			return err
		default:
			return io.ErrNoProgress
		}
	})
}

func (i image) recoveryPoint() RecoveryPoint {
	return RecoveryPoint{
		Generation: i.Generation, CoveredOffset: i.CoveredOffset, NextOrdinal: i.NextOrdinal,
		CommittedEnd: i.CommittedEnd, CommittedOrdinal: i.recoveredOrdinal, SuffixSHA256: i.tailSum,
	}
}

// VerifyPinnedRecovery validates a v1 or v2 restore and reports its complete
// recovery point. The caller owns the returned bounded logical-offset reader.
// checkReplay can reject the declared byte range before any suffix read.
func VerifyPinnedRecovery(ctx context.Context, root *os.Root, checkReplay func(start, end int64) error) (RecoveryPoint, WALReader, error) {
	img, _, tail, err := verifyPinnedImage(ctx, root, checkReplay)
	if err != nil {
		return RecoveryPoint{}, nil, err
	}
	return img.recoveryPoint(), &verifiedTail{file: tail, start: img.CoveredOffset, end: img.CommittedEnd}, nil
}
