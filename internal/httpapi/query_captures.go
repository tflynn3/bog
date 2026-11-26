package httpapi

import (
	"context"
	"errors"
	"log"
	"sync"
	"time"
)

const (
	queryCaptureLifetime = 15 * time.Minute
	maxQueryCaptures     = 64
)

type queryCapture struct {
	id         uint64
	deadline   time.Time
	sourceID   string
	scans      []certifiedScan
	workBudget uint64 // effective work allowance pinned at issuance

	// The registry lock protects lifetime state. The source ID and scan plan
	// are immutable after publication.
	timer   *time.Timer
	users   int
	retired bool
}

type queryCaptures struct {
	source PageSource

	mu      sync.Mutex
	next    uint64
	live    map[uint64]*queryCapture
	closed  bool
	drained chan struct{}
	err     error
}

func (s *queryCaptures) reserve() (*queryCapture, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || len(s.live) >= maxQueryCaptures {
		return nil, ErrQueryCapacityExhausted
	}
	s.next++
	c := &queryCapture{id: s.next, deadline: time.Now().Add(queryCaptureLifetime), users: 1}
	s.live[c.id] = c
	return c, nil
}

func (s *queryCaptures) publish(c *queryCapture) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if c.retired {
		return ErrQueryCapacityExhausted
	}
	delay := time.Until(c.deadline)
	if delay <= 0 {
		return ErrCaptureExpired
	}
	c.timer = time.AfterFunc(delay, func() { s.retire(c) })
	return nil
}

func (s *queryCaptures) acquire(id uint64) (*queryCapture, error) {
	s.mu.Lock()
	c := s.live[id]
	if c == nil || c.retired || c.timer == nil {
		s.mu.Unlock()
		return nil, ErrCaptureExpired
	}
	if !time.Now().Before(c.deadline) {
		release := s.retireLocked(c)
		s.mu.Unlock()
		if release {
			s.release(c)
		}
		return nil, ErrCaptureExpired
	}
	c.users++
	s.mu.Unlock()
	return c, nil
}

func (s *queryCaptures) done(c *queryCapture, retire bool) {
	s.mu.Lock()
	c.users--
	release := c.retired && c.users == 0
	if retire && !c.retired {
		release = s.retireLocked(c)
	}
	s.mu.Unlock()
	if release {
		s.release(c)
	}
}

func (s *queryCaptures) retireLocked(c *queryCapture) bool {
	if c.retired {
		return false
	}
	c.retired = true
	if c.timer != nil {
		c.timer.Stop()
	}
	return c.users == 0
}

func (s *queryCaptures) retire(c *queryCapture) {
	s.mu.Lock()
	release := s.retireLocked(c)
	s.mu.Unlock()
	if release {
		s.release(c)
	}
}

func (s *queryCaptures) release(c *queryCapture) {
	var err error
	if c.sourceID != "" {
		err = s.source.Release(c.sourceID)
		if err != nil {
			log.Printf("release query capture: %v", err)
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.live, c.id)
	if s.closed {
		s.err = errors.Join(s.err, err)
		if len(s.live) == 0 {
			close(s.drained)
		}
	}
}

func (s *queryCaptures) close(ctx context.Context) error {
	s.mu.Lock()
	if !s.closed {
		s.closed = true
		if len(s.live) == 0 {
			close(s.drained)
		}
		for _, c := range s.live {
			if s.retireLocked(c) {
				// Allow the caller's drain deadline to cover source cleanup.
				go s.release(c)
			}
		}
	}
	s.mu.Unlock()
	select {
	case <-s.drained:
		s.mu.Lock()
		defer s.mu.Unlock()
		return s.err
	case <-ctx.Done():
		return ctx.Err()
	}
}
