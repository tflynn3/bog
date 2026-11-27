package client

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	json "encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"strings"
)

const (
	exportFileMagic   = "BOGEXP\n"
	exportStagePrefix = ".export-progress-stage-"
	// Metadata contains at most two bounded cursors, the versioned state,
	// and the binding. The opening body is stored in its original encoding.
	exportMetadataLimit = 16 << 10
	exportFilePrefix    = len(exportFileMagic) + 4
)

// FileExportProgressStore persists a single checkpoint in a trusted, existing
// directory on a local filesystem that supports atomic replacement and file
// and directory sync. Individual operations acquire exclusive ownership.
// ExportSession holds ownership for the entire run. The opening body may
// contain sensitive query rows. Names beginning with .export-progress-stage-
// and the .export-progress-locks directory belong to the store.
type FileExportProgressStore struct {
	dir  string
	name string
}

// NewFileExportProgressStore locates the checkpoint at path. Construction
// does not open or create the file. The directory must exist before use.
func NewFileExportProgressStore(path string) (*FileExportProgressStore, error) {
	if path == "" || filepath.Base(path) == "." || filepath.Base(path) == ".." ||
		filepath.Base(path) == string(filepath.Separator) || strings.HasSuffix(path, string(filepath.Separator)) {
		return nil, errors.New("export checkpoint path must name a file")
	}
	abs, err := filepath.Abs(path)
	if err != nil {
		return nil, fmt.Errorf("resolve export checkpoint path: %w", err)
	}
	name := filepath.Base(abs)
	if strings.HasPrefix(strings.ToLower(name), exportStagePrefix) {
		return nil, errors.New("export checkpoint path uses the reserved staging prefix")
	}
	for part := range strings.SplitSeq(abs, string(filepath.Separator)) {
		if strings.EqualFold(part, exportLocksDir) {
			return nil, errors.New("export checkpoint path uses the reserved lock directory")
		}
	}
	return &FileExportProgressStore{dir: filepath.Dir(abs), name: name}, nil
}

func (s *FileExportProgressStore) Load(ctx context.Context, maxPageBytes int64) (_ ExportProgress, found bool, resultErr error) {
	if err := withQueryCancellation(ctx, nil); err != nil {
		return ExportProgress{}, false, err
	}
	lease, err := s.Acquire()
	if err != nil {
		return ExportProgress{}, false, err
	}
	defer func() { resultErr = joinQueryErrors(resultErr, lease.Close()) }()
	return lease.Load(ctx, maxPageBytes)
}

func (s *FileExportProgressStore) Save(ctx context.Context, progress ExportProgress) (resultErr error) {
	if err := withQueryCancellation(ctx, nil); err != nil {
		return err
	}
	lease, err := s.Acquire()
	if err != nil {
		return err
	}
	defer func() { resultErr = joinQueryErrors(resultErr, lease.Close()) }()
	return lease.Save(ctx, progress)
}

// Load returns the complete durable checkpoint, if present. Syncing the
// directory also settles a previous Save whose rename succeeded but whose
// directory sync failed or was interrupted.
func (s *FileExportProgressLease) Load(ctx context.Context, maxPageBytes int64) (_ ExportProgress, found bool, resultErr error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkOpen(ctx); err != nil {
		return ExportProgress{}, false, err
	}
	if maxPageBytes <= 0 || maxPageBytes == math.MaxInt64 {
		return ExportProgress{}, false, errors.New("invalid export page byte limit")
	}
	root := s.root
	removed, err := s.recoverStages()
	if err != nil {
		return ExportProgress{}, false, err
	}

	info, err := root.Lstat(s.name)
	if errors.Is(err, os.ErrNotExist) {
		if removed {
			return ExportProgress{}, false, syncExportDirectory(root)
		}
		return ExportProgress{}, false, nil
	}
	if err != nil {
		return ExportProgress{}, false, fmt.Errorf("stat export checkpoint: %w", err)
	}
	if !info.Mode().IsRegular() {
		return ExportProgress{}, false, fmt.Errorf("%w: checkpoint is not a regular file", ErrExportProgress)
	}
	file, err := root.Open(s.name)
	if err != nil {
		return ExportProgress{}, false, fmt.Errorf("open export checkpoint: %w", err)
	}
	defer func() {
		resultErr = exportIOError(resultErr, "close export checkpoint", file.Close())
	}()

	progress, err := readExportCheckpoint(ctx, file, info.Size(), maxPageBytes)
	if err != nil {
		return ExportProgress{}, false, err
	}
	if err := progress.validate(maxPageBytes); err != nil {
		return ExportProgress{}, false, err
	}
	if err := withQueryCancellation(ctx, nil); err != nil {
		return ExportProgress{}, false, err
	}
	if err := syncExportDirectory(root); err != nil {
		return ExportProgress{}, false, err
	}
	return progress, true, withQueryCancellation(ctx, nil)
}

// Save writes a private sibling file, syncs and closes it, then atomically
// replaces the checkpoint and syncs the same directory. A pre-rename failure
// leaves the old checkpoint in place. A post-rename failure leaves a complete
// replacement whose durability must be re-established by Load.
func (s *FileExportProgressLease) Save(ctx context.Context, progress ExportProgress) error {
	return s.save(ctx, progress, (*os.Root).Rename, syncExportDirectory)
}

func (s *FileExportProgressLease) save(ctx context.Context, progress ExportProgress,
	rename func(*os.Root, string, string) error, syncDir func(*os.Root) error,
) (resultErr error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkOpen(ctx); err != nil {
		return err
	}
	if err := progress.validate(math.MaxInt64); err != nil {
		return err
	}
	body, metadata, err := encodeExportCheckpoint(progress)
	if err != nil {
		return err
	}

	root, stage := s.root, s.stages[0]
	if _, err := s.recoverStages(); err != nil {
		return err
	}
	if !s.lockSynced {
		// A surviving stage must have a durable owner name, even if this
		// is the first save and there is no installed checkpoint yet.
		if err := s.lock.Sync(); err != nil {
			return fmt.Errorf("sync export checkpoint lock: %w", err)
		}
		if err := syncExportDirectoryAt(root, exportLocksDir); err != nil {
			return err
		}
		if err := syncExportDirectory(root); err != nil {
			return err
		}
		s.lockSynced = true
	}
	file, err := root.OpenFile(stage, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return fmt.Errorf("stage export checkpoint: %w", err)
	}
	renamed := false
	defer func() {
		if !renamed {
			resultErr = exportIOError(resultErr, "remove staged export checkpoint", root.Remove(stage))
		}
	}()

	err = writeExportCheckpoint(ctx, file, body, metadata)
	err = exportIOError(err, "close staged export checkpoint", file.Close())
	if err != nil {
		return err
	}
	if err := withQueryCancellation(ctx, nil); err != nil {
		return err
	}
	if err := rename(root, stage, s.name); err != nil {
		return fmt.Errorf("install export checkpoint: %w", err)
	}
	renamed = true
	return withQueryCancellation(ctx, syncDir(root))
}

func encodeExportCheckpoint(progress ExportProgress) (body, metadata []byte, err error) {
	if progress.Opening != nil {
		opening := *progress.Opening
		body = opening.Body
		opening.Body = nil
		progress.Opening = &opening
	}
	metadata, err = json.Marshal(progress)
	if err != nil {
		return nil, nil, fmt.Errorf("encode export checkpoint metadata: %w", err)
	}
	if len(metadata) > exportMetadataLimit {
		return nil, nil, fmt.Errorf("%w: checkpoint metadata exceeds size limit", ErrExportProgress)
	}
	return body, metadata, nil
}

func writeExportCheckpoint(ctx context.Context, file *os.File, body, metadata []byte) error {
	var prefix [exportFilePrefix]byte
	copy(prefix[:], exportFileMagic)
	binary.BigEndian.PutUint32(prefix[len(exportFileMagic):], uint32(len(metadata)))
	checksum := exportCheckpointChecksum(metadata, body)
	for _, part := range [][]byte{prefix[:], metadata, body, checksum[:]} {
		for len(part) != 0 {
			if err := withQueryCancellation(ctx, nil); err != nil {
				return err
			}
			chunk := part[:min(len(part), queryReadBlock)]
			if _, err := file.Write(chunk); err != nil {
				return fmt.Errorf("write staged export checkpoint: %w", err)
			}
			part = part[len(chunk):]
		}
	}
	return exportIOError(nil, "sync staged export checkpoint", file.Sync())
}

func readExportCheckpoint(ctx context.Context, file *os.File, size, maxPageBytes int64) (ExportProgress, error) {
	var prefix [exportFilePrefix]byte
	if err := readExportCheckpointPart(ctx, file, prefix[:]); err != nil {
		return ExportProgress{}, err
	}
	if string(prefix[:len(exportFileMagic)]) != exportFileMagic {
		return ExportProgress{}, fmt.Errorf("%w: unrecognized checkpoint format", ErrExportProgress)
	}
	metadataSize := int64(binary.BigEndian.Uint32(prefix[len(exportFileMagic):]))
	if metadataSize == 0 || metadataSize > exportMetadataLimit {
		return ExportProgress{}, fmt.Errorf("%w: invalid checkpoint metadata size", ErrExportProgress)
	}
	bodySize := size - int64(exportFilePrefix) - metadataSize - sha256.Size
	if bodySize < 0 {
		return ExportProgress{}, fmt.Errorf("%w: truncated checkpoint", ErrExportProgress)
	}
	if bodySize > maxPageBytes || bodySize > int64(math.MaxInt) {
		return ExportProgress{}, ErrPageBytes
	}
	metadata := make([]byte, int(metadataSize))
	if err := readExportCheckpointPart(ctx, file, metadata); err != nil {
		return ExportProgress{}, err
	}
	var progress ExportProgress
	if err := json.Unmarshal(metadata, &progress, json.RejectUnknownMembers(true)); err != nil {
		return ExportProgress{}, fmt.Errorf("%w: decode checkpoint metadata: %w", ErrExportProgress, err)
	}
	switch {
	case progress.Opening != nil && progress.Opening.Body != nil:
		return ExportProgress{}, fmt.Errorf("%w: opening body in checkpoint metadata", ErrExportProgress)
	case progress.Opening == nil && bodySize != 0:
		return ExportProgress{}, fmt.Errorf("%w: unexpected checkpoint body", ErrExportProgress)
	}
	body := make([]byte, int(bodySize))
	if err := readExportCheckpointPart(ctx, file, body); err != nil {
		return ExportProgress{}, err
	}
	var checksum [sha256.Size]byte
	if err := readExportCheckpointPart(ctx, file, checksum[:]); err != nil {
		return ExportProgress{}, err
	}
	if checksum != exportCheckpointChecksum(metadata, body) {
		return ExportProgress{}, fmt.Errorf("%w: checkpoint checksum mismatch", ErrExportProgress)
	}
	if progress.Opening != nil {
		progress.Opening.Body = body
	}
	return progress, nil
}

func readExportCheckpointPart(ctx context.Context, file *os.File, data []byte) error {
	for len(data) != 0 {
		if err := withQueryCancellation(ctx, nil); err != nil {
			return err
		}
		chunk := data[:min(len(data), queryReadBlock)]
		if _, err := io.ReadFull(file, chunk); err != nil {
			return fmt.Errorf("%w: read checkpoint: %w", ErrExportProgress, err)
		}
		data = data[len(chunk):]
	}
	return nil
}

func exportCheckpointChecksum(metadata, body []byte) [sha256.Size]byte {
	// Hash each field separately to bind the boundary between metadata and
	// body without assembling another copy of the encoded page.
	var parts [2 * sha256.Size]byte
	metadataHash, bodyHash := sha256.Sum256(metadata), sha256.Sum256(body)
	copy(parts[:sha256.Size], metadataHash[:])
	copy(parts[sha256.Size:], bodyHash[:])
	return sha256.Sum256(parts[:])
}

func syncExportDirectory(root *os.Root) error {
	return syncExportDirectoryAt(root, ".")
}

func syncExportDirectoryAt(root *os.Root, name string) error {
	dir, err := root.Open(name)
	if err != nil {
		return fmt.Errorf("open export checkpoint directory for sync: %w", err)
	}
	err = exportIOError(nil, "sync export checkpoint directory", dir.Sync())
	return exportIOError(err, "close export checkpoint directory", dir.Close())
}

func exportIOError(err error, operation string, cause error) error {
	if cause == nil {
		return err
	}
	return joinQueryErrors(err, fmt.Errorf("%s: %w", operation, cause))
}
