package httpapi

import (
	"context"
	"errors"
	"log"
	"math"
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
	legacyScan queryReservation
	scans      []certifiedScan
	oversized  *certifiedOversized

	// The registry lock protects lifetime state. The source ID and scan plan
	// are immutable after publication.
	timer   *time.Timer
	users   int
	retired bool
}

type queryCaptures struct {
	source PageSource

	resourcesOnce sync.Once
	resources     *queryResources

	mu      sync.Mutex
	next    uint64
	live    map[uint64]*queryCapture
	closed  bool
	queries int
	drained chan struct{}
	err     error
}

func (s *queryCaptures) startQuery() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return false
	}
	s.queries++
	return true
}

func (s *queryCaptures) finishQuery() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.queries--
	s.checkDrainedLocked()
}

// Each transition that removes an owner checks for final drain. Close seals
// admission before checking, so once drained the counts cannot rise again.
func (s *queryCaptures) checkDrainedLocked() {
	if s.closed && s.queries == 0 && len(s.live) == 0 {
		close(s.drained)
	}
}

func (s *queryCaptures) reserve(maxCaptures uint64) (*queryCapture, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.next == math.MaxUint64 || len(s.live) >= maxQueryCaptures {
		return nil, ErrQueryCapacityExhausted
	}
	if uint64(len(s.live)) >= maxCaptures {
		return nil, ErrQueryCaptureCapacityExhausted
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
	if s.closed || c == nil || c.retired || c.timer == nil {
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
	s.retireByID(c.id)
}

func (s *queryCaptures) retireByID(id uint64) error {
	s.mu.Lock()
	c := s.live[id]
	release := c != nil && s.retireLocked(c)
	s.mu.Unlock()
	if release {
		return s.release(c)
	}
	return nil
}

// A transient capture is owned by an ordinary query through delivery.
func (s *queryCaptures) releaseTransient(id string) {
	if err := s.source.Release(id); err != nil {
		log.Printf("release query capture: %v", err)
		s.mu.Lock()
		s.err = errors.Join(s.err, err)
		s.mu.Unlock()
	}
}

// release logs failures for callers without an error return and includes
// failures during shutdown in the result of close.
func (s *queryCaptures) release(c *queryCapture) error {
	var err error
	if c.sourceID != "" {
		err = s.source.Release(c.sourceID)
		if err != nil {
			log.Printf("release query capture: %v", err)
		}
	}
	c.legacyScan.release()
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.live, c.id)
	if s.closed {
		s.err = errors.Join(s.err, err)
		s.checkDrainedLocked()
	}
	return err
}

func (s *queryCaptures) close(ctx context.Context) error {
	s.mu.Lock()
	if !s.closed {
		s.closed = true
		s.checkDrainedLocked()
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
