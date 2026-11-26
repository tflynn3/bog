// Package backup exports, verifies, and restores Bog v1 store directories.
package backup

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
)

const (
	indexName    = "bog.backup"
	manifestName = "bog.checkpoint"
	walName      = "bog.wal"
	markerName   = "bog.commit"
	walMagic     = "BOGWAL01"
	markerMagic  = "BOGCMT01"
	indexMagic   = "BOGBAK01"
	indexSize    = 16 + 2*sha256.Size
)

// Snapshot describes one recovery point. Offsets include the WAL magic.
// ManifestSHA256 is zero for a WAL-only snapshot, whose CoveredOffset is 8.
type Snapshot struct {
	CommittedOffset int64
	CoveredOffset   int64
	Rows            uint64
	WALSHA256       [sha256.Size]byte
	ManifestSHA256  [sha256.Size]byte
}

// ErrPublicationUncertain indicates the destination rename was attempted but
// publication was not confirmed. Reopen and verify the destination before use.
var ErrPublicationUncertain = errors.New("backup publication uncertain")

var errChanged = errors.New("source checkpoint or WAL generation changed during capture")

// Backup exports a live v1 store to a new or empty directory. The atomically
// replaced commit marker determines the end of the snapshot, even while ingest
// continues. Whole committed frames are copied and verified before publication.
func Backup(ctx context.Context, source, destination string) (Snapshot, error) {
	for attempt := 0; attempt < 8; attempt++ {
		snapshot, err := copyDirectory(ctx, source, destination, true)
		if err != errChanged {
			return snapshot, err
		}
	}
	return Snapshot{}, errChanged
}

// Verify checks the export index, checkpoint, referenced segments, and WAL.
func Verify(ctx context.Context, source string) (snapshot Snapshot, err error) {
	dir, err := openDirectory(source)
	if err != nil {
		return Snapshot{}, err
	}
	defer joinClose(&err, dir)
	return transfer(ctx, dir, nil, false)
}

// Restore validates and recreates the store in a new or empty directory. An
// error before publication leaves the destination reusable.
func Restore(ctx context.Context, source, destination string) (Snapshot, error) {
	return copyDirectory(ctx, source, destination, false)
}

func copyDirectory(ctx context.Context, source, destination string, export bool) (snapshot Snapshot, err error) {
	err = publish(ctx, destination, func(stage *directory) (err error) {
		src, err := openDirectory(source)
		if err != nil {
			return err
		}
		defer joinClose(&err, src)
		snapshot, err = transfer(ctx, src, stage, export)
		if err != nil || !export {
			return err
		}
		var b [indexSize]byte
		copy(b[:], indexMagic)
		binary.LittleEndian.PutUint64(b[8:], uint64(snapshot.CommittedOffset))
		copy(b[16:], snapshot.WALSHA256[:])
		copy(b[16+sha256.Size:], snapshot.ManifestSHA256[:])
		return stage.write(indexName, b[:])
	})
	if err != nil {
		return Snapshot{}, err
	}
	return snapshot, nil
}

func transfer(ctx context.Context, src, dst *directory, live bool) (snapshot Snapshot, err error) {
	if err := ctx.Err(); err != nil {
		return snapshot, err
	}
	var index []byte
	if !live {
		index, err = src.read(indexName, indexSize)
		if err != nil {
			return snapshot, err
		}
		if len(index) != indexSize || string(index[:8]) != indexMagic {
			return snapshot, errors.New("invalid backup index")
		}
	}

	manifestFile, manifestInfo, err := src.openFile(manifestName)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return snapshot, err
	}
	m := manifestV1{CoveredOffset: int64(len(walMagic))}
	var manifest []byte
	if manifestFile != nil {
		defer joinClose(&err, manifestFile)
		manifest, err = boundedRead(manifestFile, manifestInfo, maxManifestSize)
		if err != nil {
			return snapshot, err
		}
		m, err = decodeManifest(manifest)
		if err != nil {
			return snapshot, err
		}
		snapshot.ManifestSHA256 = sha256.Sum256(manifest)
	}
	snapshot.CoveredOffset = m.CoveredOffset

	wal, walInfo, err := src.openFile(walName)
	if err != nil {
		return snapshot, err
	}
	defer joinClose(&err, wal)
	marker, err := src.read(markerName, 24)
	if errors.Is(err, os.ErrNotExist) && live {
		// A stopped, newly bootstrapped legacy store can consist of just magic.
		info, statErr := wal.Stat()
		if statErr != nil {
			return snapshot, statErr
		}
		if info.Size() == int64(len(walMagic)) {
			marker = makeMarker(int64(len(walMagic)))
			err = nil
		} else {
			// The first marker may have appeared while we checked the WAL.
			marker, err = src.read(markerName, 24)
		}
	}
	if err != nil {
		return snapshot, err
	}
	end, err := parseMarker(marker)
	if err != nil {
		return snapshot, err
	}
	snapshot.CommittedOffset = end
	info, err := wal.Stat()
	if err != nil {
		return snapshot, err
	}
	if live {
		// The writer syncs WAL bytes before replacing the marker. Sync the
		// pinned directory in case we observed the rename before its own sync.
		if err := src.fd.Sync(); err != nil {
			return snapshot, err
		}
		for _, input := range []struct {
			name string
			info os.FileInfo
		}{{manifestName, manifestInfo}, {walName, walInfo}} {
			same, err := src.sameFile(input.name, input.info)
			if err != nil {
				return snapshot, err
			}
			if !same {
				return snapshot, errChanged
			}
		}
	}
	if end > info.Size() || (!live && end != info.Size()) || m.CoveredOffset > end {
		return snapshot, errors.New("WAL size or checkpoint coverage disagrees with commit marker")
	}
	if !live && (binary.LittleEndian.Uint64(index[8:16]) != uint64(end) ||
		!bytes.Equal(index[16+sha256.Size:], snapshot.ManifestSHA256[:])) {
		return snapshot, errors.New("backup index disagrees with store metadata")
	}

	// Validate and copy immutable inputs first. A publisher may retire an old
	// segment before we open it. Retry only if the manifest actually changed.
	if err := copySegments(ctx, src, dst, m); err != nil {
		if live && errors.Is(err, os.ErrNotExist) {
			same, checkErr := src.sameFile(manifestName, manifestInfo)
			if checkErr != nil {
				return snapshot, checkErr
			}
			if !same {
				return snapshot, errChanged
			}
		}
		return snapshot, err
	}
	err = dst.output(walName, func(w io.Writer) error {
		var e error
		snapshot.Rows, snapshot.WALSHA256, e = scanWAL(ctx, wal, w, end, m)
		return e
	})
	if err != nil {
		return snapshot, err
	}
	if !live && !bytes.Equal(index[16:16+sha256.Size], snapshot.WALSHA256[:]) {
		return snapshot, errors.New("backup WAL checksum mismatch")
	}
	if dst != nil {
		if err := dst.write(markerName, marker); err != nil {
			return snapshot, err
		}
		if manifestInfo != nil {
			if err := dst.write(manifestName, manifest); err != nil {
				return snapshot, fmt.Errorf("copy checkpoint manifest: %w", err)
			}
		}
	}
	return snapshot, nil
}

func makeMarker(end int64) []byte {
	b := make([]byte, 24)
	copy(b, markerMagic)
	binary.LittleEndian.PutUint64(b[8:], uint64(end))
	binary.LittleEndian.PutUint64(b[16:], ^uint64(end))
	return b
}

func parseMarker(b []byte) (int64, error) {
	if len(b) == 24 && string(b[:8]) == markerMagic {
		end := binary.LittleEndian.Uint64(b[8:])
		if end >= uint64(len(walMagic)) && end <= uint64(1<<63-1) && binary.LittleEndian.Uint64(b[16:]) == ^end {
			return int64(end), nil
		}
	}
	return 0, errors.New("invalid WAL commit marker")
}
