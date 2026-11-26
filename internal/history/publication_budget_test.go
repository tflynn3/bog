package history

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"os"
	"sync"
	"syscall"
	"testing"
	"time"

	"bog/internal/checkpoint"
)

func budgetTestFrame(payload string) []byte {
	var header [16]byte
	binary.LittleEndian.PutUint32(header[:4], uint32(len(payload)))
	binary.LittleEndian.PutUint32(header[4:8], ^uint32(len(payload)))
	binary.LittleEndian.PutUint32(header[8:12], crc32.Checksum([]byte(payload), crcTable))
	binary.LittleEndian.PutUint32(header[12:], crc32.Checksum(header[:12], crcTable))
	return append(header[:], payload...)
}

type budgetTestReader struct {
	io.ReaderAt
	before func()
}

func (r budgetTestReader) ReadAt(p []byte, offset int64) (int, error) {
	r.before()
	return r.ReaderAt.ReadAt(p, offset)
}

func TestLivePublicationKeepsCapturedTailCharged(t *testing.T) {
	ctx := t.Context()
	firstFrame := budgetTestFrame("{\"ts\":\"2026-01-01T00:00:00Z\",\"line\":\"covered\"}\n")
	secondFrame := budgetTestFrame("{\"ts\":\"2026-01-01T00:00:00Z\",\"line\":\"later\"}\n")
	magic := []byte("BOGWAL01")
	firstPrefix := append(bytes.Clone(magic), firstFrame...)
	boundary, cutoff := int64(len(magic)), int64(len(firstPrefix))
	end := cutoff + int64(len(secondFrame))
	budget := checkpoint.NewBudget(checkpoint.Limits{
		checkpoint.StagingBytes:      4096,
		checkpoint.ReplayTailBytes:   uint64(end - boundary),
		checkpoint.PinnedGenerations: 2,
		checkpoint.LiveCaptures:      1,
	})
	root, err := os.OpenRoot(privateDir(t))
	requireOK(t, err)
	cleanup(t, root.Close)
	dir, err := root.Open(".")
	requireOK(t, err)
	cleanup(t, dir.Close)
	requireOK(t, syscall.Flock(int(dir.Fd()), syscall.LOCK_EX|syscall.LOCK_NB))

	publisher := checkpoint.NewPublisher(root, dir, budget)
	err = publisher.RecoverCaptured(func() (*checkpoint.Manifest, error) { return ValidatePinnedAuthority(ctx, dir) })
	if !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("empty authority recovery: %v", err)
	}
	first, err := publisher.PublishCaptured(ctx, bytes.NewReader(magic), boundary, nil, 0, hashText(magic))
	requireOK(t, err)
	cat := NewCatalog(budget)
	cleanup(t, cat.Close)
	old, err := cat.ReplacePinned(ctx, dir)
	requireOK(t, err)
	tail, err := budget.ReplayCapturedTail(ctx, bytes.NewReader(firstPrefix), boundary, cutoff, 0)
	requireOK(t, err)
	live := tail.Batches[0].Reservation
	defer live.Release()

	reader, err := cat.AcquireCurrent()
	requireOK(t, err)
	cleanup(t, reader.Close)
	held := live.Retain()
	defer held.Release()

	ticket, err := cat.ReserveReplacement(ctx)
	requireOK(t, err)
	defer ticket.Close()
	began, resume := make(chan struct{}), make(chan struct{})
	var readOnce, resumeOnce sync.Once
	unblock := func() { resumeOnce.Do(func() { close(resume) }) }
	defer unblock()
	input := budgetTestReader{
		ReaderAt: bytes.NewReader(firstPrefix),
		before:   func() { readOnce.Do(func() { close(began); <-resume }) },
	}
	done := make(chan error, 1)
	go func() {
		_, err := publisher.PublishCaptured(ctx, input, cutoff, first, tail.NextOrdinal, hashText(firstPrefix))
		done <- err
	}()
	select {
	case <-began:
	case err := <-done:
		t.Fatalf("publication returned before reading the prefix: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("publication did not reach the WAL")
	}

	// This is the WAL owner's admission and transfer for a concurrent append.
	appended, appendErr := budget.ReserveReplayTail(ctx, cutoff, end)
	unblock()
	requireOK(t, <-done)
	requireOK(t, appendErr)
	defer appended.Release()
	next, err := ticket.ReplacePinned(ctx, dir)
	requireOK(t, err)
	if next.Boundary.CoveredOffset != cutoff || next.Boundary.NextOrdinal != 1 {
		t.Fatalf("active boundary = %+v", next.Boundary)
	}
	live.Release()
	_, err = budget.ReserveReplayTail(ctx, end, end+1)
	assertExhausted(t, err, checkpoint.ReplayTailBytes, uint64(end-boundary))
	if got := scanRows(t, reader, all); len(got) != 0 || reader.Generation().ID != old.ID {
		t.Fatalf("captured generation changed after publication: %v", got)
	}
	requireOK(t, reader.Close())
	held.Release()
	more, err := budget.ReserveReplayTail(ctx, end, end+1)
	requireOK(t, err)
	more.Release()
	current, err := cat.AcquireCurrent()
	requireOK(t, err)
	cleanup(t, current.Close)
	if got := scanRows(t, current, all); len(got) != 1 || got[0] != row(epoch, 0, "covered") {
		t.Fatalf("active generation = %v", got)
	}
	usage := budget.Usage()
	if usage.InUse[checkpoint.StagingBytes] != 0 || usage.Peak[checkpoint.StagingBytes] == 0 ||
		usage.InUse[checkpoint.ReplayTailBytes] != uint64(len(secondFrame)) ||
		usage.Peak[checkpoint.ReplayTailBytes] != uint64(end-boundary) {
		t.Fatalf("publication evidence = %+v", usage)
	}
}
