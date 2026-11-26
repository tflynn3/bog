package checkpoint

import (
	"context"
	"errors"
	"io"
	"os"
	"sync"
	"syscall"
)

// Publisher owns the live staging reservations for a pinned directory. Use
// one per directory, including during recovery. The WAL owner holds its
// directory lock and keeps root and dir open through publication and recovery.
type Publisher struct {
	mu    sync.Mutex
	root  *os.Root
	dir   *os.File
	stage stagingAccount
}

func NewPublisher(root *os.Root, dir *os.File, budget *Budget) *Publisher {
	return &Publisher{root: root, dir: dir, stage: stagingAccount{budget: budget}}
}

// PublishCaptured publishes a committed prefix using this publisher's budget.
// A non-nil manifest identifies the durable authority even if cleanup failed.
func (p *Publisher) PublishCaptured(ctx context.Context, input io.ReaderAt, end int64, previous *Manifest, next uint64, source string) (*Manifest, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if !p.mu.TryLock() {
		return nil, syscall.EWOULDBLOCK
	}
	defer p.mu.Unlock()
	if p.stage.budget == nil {
		return nil, errors.New("missing checkpoint publication budget")
	}
	if p.stage.pending {
		return nil, ErrPublicationUncertain
	}
	return publishCaptured(ctx, p.root, p.dir, input, end, previous, next, source, &p.stage)
}

// RecoverCaptured validates and syncs the authority, reclaims losing outputs,
// and settles staging charges. The validator runs under the publication lock.
func (p *Publisher) RecoverCaptured(authoritative func() (*Manifest, error)) error {
	if !p.mu.TryLock() {
		return syscall.EWOULDBLOCK
	}
	defer p.mu.Unlock()
	return recoverCapturedWithBudget(p.root, p.dir, authoritative, &p.stage)
}

type stagingAccount struct {
	budget  *Budget
	charges []*Reservation
	pending bool
}

func (s *stagingAccount) reserve(ctx context.Context, size int) error {
	if s == nil {
		return ctx.Err()
	}
	charge, err := s.budget.Reserve(ctx, StagingBytes, uint64(size))
	if err != nil {
		return err
	}
	s.charges = append(s.charges, charge)
	return nil
}

func (s *stagingAccount) settle() {
	if s == nil {
		return
	}
	for _, charge := range s.charges {
		charge.Release()
	}
	s.charges = nil
	s.pending = false
}
