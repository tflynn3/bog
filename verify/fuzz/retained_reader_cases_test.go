//go:build linux || (darwin && cgo)

package fuzz

import (
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"math"

	"bog/internal/backup"
	"bog/internal/checkpoint"
)

var errRetainedReaderClose = errors.New("injected recovery suffix close error")

type retainedReaderCase struct {
	name               string
	checkpoint, backup error
	edit               func(*retainedReaderInput, int)
	cancel, closeError bool
	offlineAdmission   bool
}

type retainedReaderInput struct {
	image  files
	digest [32]byte
}

func retainedReaderCases() []retainedReaderCase {
	return []retainedReaderCase{
		{name: "valid"},
		{name: "truncated-manifest", checkpoint: checkpoint.ErrIntegrity, backup: backup.ErrIntegrity,
			edit: func(in *retainedReaderInput, _ int) {
				b := in.image[manifestFile]
				in.image[manifestFile] = b[:len(b)-1]
			}},
		{name: "truncated-second-frame", offlineAdmission: true, checkpoint: io.ErrUnexpectedEOF, backup: io.ErrUnexpectedEOF,
			edit: func(in *retainedReaderInput, _ int) {
				tail := in.image[tailFile]
				// The stream is short relative to its declared committed end.
				in.image[tailFile] = tail[:len(tail)-1]
			}},
		{name: "second-frame-length", offlineAdmission: true, checkpoint: checkpoint.ErrIntegrity, backup: backup.ErrIntegrity,
			edit: func(in *retainedReaderInput, second int) {
				tail := in.image[tailFile]
				copy(tail[second:], frameLength(tail[second:], math.MaxUint32))
				in.rebindTail()
			}},
		{name: "second-frame-checksum", offlineAdmission: true, checkpoint: checkpoint.ErrIntegrity, backup: backup.ErrIntegrity,
			edit: func(in *retainedReaderInput, second int) {
				in.image[tailFile][second+frameHeader] ^= 1
				in.rebindTail()
			}},
		{name: "trailing-data", offlineAdmission: true, checkpoint: checkpoint.ErrIntegrity, backup: backup.ErrIntegrity,
			edit: func(in *retainedReaderInput, _ int) {
				in.image[tailFile] = append(in.image[tailFile], 0xff)
			}},
		{name: "suffix-digest", offlineAdmission: true, checkpoint: checkpoint.ErrIntegrity, backup: backup.ErrIntegrity,
			edit: func(in *retainedReaderInput, _ int) { in.digest[0] ^= 1 }},
		{name: "canceled-second-frame", checkpoint: context.Canceled, backup: context.Canceled, cancel: true},
		{name: "suffix-close-error", checkpoint: errRetainedReaderClose, backup: errRetainedReaderClose, closeError: true},
	}
}

func (in *retainedReaderInput) rebindTail() {
	in.digest = sha256.Sum256(in.image[tailFile])
}

func (tc retainedReaderCase) input(f retainedFixture) retainedReaderInput {
	in := retainedReaderInput{image: cloneRetainedFiles(f.previous.image)}
	delete(in.image, metadataFile)
	in.rebindTail()
	if tc.edit != nil {
		tc.edit(&in, f.cuts[1].end-f.previous.view.covered.end)
	}
	return in
}
