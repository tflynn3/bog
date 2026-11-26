package idempotency

import (
	"bytes"
	"context"
	"errors"
	"slices"
	"sync"
	"time"
)

var errInjected = errors.New("injected storage failure")

type commitFault uint8

const (
	noFault commitFault = iota
	rollback
	uncertainRollback
	lostCommitReply
	unresolved
)

// journalEntry is the transactional test backend's durable image. A commit
// entry contains both the batch and its retry metadata. A deletion affects
// only the retry index. The image can reconstruct an empty backend.
type journalEntry struct {
	key     ScopedKey
	record  Record
	batch   []byte
	deleted bool
}

type expiryCandidate struct {
	key    ScopedKey
	record Record
}

type testBackend struct {
	mu       sync.Mutex
	journal  []journalEntry
	records  map[ScopedKey]Record
	expiry   []expiryCandidate
	fenced   map[ScopedKey]AttemptID
	reverted map[AttemptID]ScopedKey

	// Hooks are installed before starting callers and run outside mu.
	beforeCommit  func(context.Context, CommitRequest) commitFault
	afterCommit   func()
	beforeCleanup func()
}

var _ Backend = (*testBackend)(nil)

func openTestBackend(image []journalEntry) *testBackend {
	b := &testBackend{
		records:  make(map[ScopedKey]Record),
		fenced:   make(map[ScopedKey]AttemptID),
		reverted: make(map[AttemptID]ScopedKey),
	}
	for _, entry := range image {
		entry.batch = bytes.Clone(entry.batch)
		b.apply(entry)
	}
	return b
}

// apply updates the committed image and both derived indexes under the same
// lock. Opening a backend runs it before exposing the instance to callers.
func (b *testBackend) apply(entry journalEntry) {
	if old, found := b.records[entry.key]; found {
		index, found := slices.BinarySearchFunc(b.expiry, expiryCandidate{entry.key, old}, compareExpiry)
		if !found {
			panic("retry record missing from fixture expiry index")
		}
		b.expiry = slices.Delete(b.expiry, index, index+1)
	}
	b.journal = append(b.journal, entry)
	if entry.deleted {
		delete(b.records, entry.key)
		return
	}
	b.records[entry.key] = entry.record
	candidate := expiryCandidate{entry.key, entry.record}
	at, _ := slices.BinarySearchFunc(b.expiry, candidate, compareExpiry)
	b.expiry = slices.Insert(b.expiry, at, candidate)
}

func compareExpiry(a, b expiryCandidate) int {
	if order := a.record.ExpiresAt.Compare(b.record.ExpiresAt); order != 0 {
		return order
	}
	return bytes.Compare(a.record.AttemptID[:], b.record.AttemptID[:])
}

func (b *testBackend) Commit(ctx context.Context, req CommitRequest) (BackendResult, error) {
	var fault commitFault
	if b.beforeCommit != nil {
		fault = b.beforeCommit(ctx, req)
	}
	b.mu.Lock()
	result, err := b.commitLocked(ctx, req, fault)
	b.mu.Unlock()
	if fault == lostCommitReply && result.State == BackendStored {
		if b.afterCommit != nil {
			b.afterCommit()
		}
		return BackendResult{}, errInjected
	}
	return result, err
}

func (b *testBackend) commitLocked(ctx context.Context, req CommitRequest, fault commitFault) (BackendResult, error) {
	if _, fenced := b.fenced[req.Key]; fenced {
		return BackendResult{}, errInjected
	}
	if err := ctx.Err(); err != nil {
		return BackendResult{State: BackendRolledBack}, err
	}
	now := req.Now()
	if record, found := b.records[req.Key]; found && record.ActiveAt(now) {
		return BackendResult{State: BackendExisting, Record: record}, nil
	}
	switch fault {
	case rollback:
		return BackendResult{State: BackendRolledBack}, errInjected
	case uncertainRollback:
		b.reverted[req.AttemptID] = req.Key
		return BackendResult{}, errInjected
	case unresolved:
		b.fenced[req.Key] = req.AttemptID
		return BackendResult{}, errInjected
	}
	record := req.RecordAt(now)
	b.apply(journalEntry{key: req.Key, record: record, batch: bytes.Clone(req.Batch)})
	return BackendResult{State: BackendStored, Record: record}, nil
}

func (b *testBackend) Resolve(ctx context.Context, key ScopedKey, attempt AttemptID) (BackendResult, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return BackendResult{}, err
	}
	if record, found := b.records[key]; found && record.AttemptID == attempt {
		return BackendResult{State: BackendStored, Record: record}, nil
	}
	if rolledBackKey, found := b.reverted[attempt]; found && rolledBackKey == key {
		delete(b.reverted, attempt)
		return BackendResult{State: BackendRolledBack}, nil
	}
	return BackendResult{}, nil
}

func (b *testBackend) DeleteExpired(ctx context.Context, cutoff time.Time, limit int) (int, error) {
	b.mu.Lock()
	var candidates []expiryCandidate
	for _, candidate := range b.expiry[:min(limit, len(b.expiry))] {
		if candidate.record.ActiveAt(cutoff) {
			break
		}
		candidates = append(candidates, candidate)
	}
	b.mu.Unlock()

	if b.beforeCleanup != nil {
		b.beforeCleanup()
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	deleted := 0
	for _, candidate := range candidates {
		if err := ctx.Err(); err != nil {
			return deleted, err
		}
		record, found := b.records[candidate.key]
		if found && record.AttemptID == candidate.record.AttemptID && !record.ActiveAt(cutoff) {
			b.apply(journalEntry{key: candidate.key, deleted: true})
			deleted++
		}
	}
	return deleted, nil
}

func (b *testBackend) image() []journalEntry {
	b.mu.Lock()
	defer b.mu.Unlock()
	// Entries are immutable. Reopening copies their byte slices.
	return slices.Clone(b.journal)
}

func (b *testBackend) batches() [][]byte {
	var batches [][]byte
	for _, entry := range b.image() {
		if !entry.deleted {
			batches = append(batches, bytes.Clone(entry.batch))
		}
	}
	return batches
}

func (b *testBackend) record(key ScopedKey) (Record, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	r, exists := b.records[key]
	return r, exists
}
