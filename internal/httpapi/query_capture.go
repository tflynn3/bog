package httpapi

import (
	"context"
	"log"
	"sync"
	"time"
)

const (
	queryCaptureLifetime = 15 * time.Minute
	maxQueryCaptures     = 32
)

type queryCapture struct {
	id       uint64
	sourceID string
	deadline time.Time
	windows  []queryWindow

	// Protected by captureRegistry.mu.
	users     int
	published bool
	retired   bool
	timer     *time.Timer
}

type captureRegistry struct {
	mu              sync.Mutex
	source          PageSource
	now             func() time.Time
	next            uint64
	live            map[uint64]*queryCapture
	count, requests int
	closing         bool
	drained         chan struct{}
	releaseErr      error
}

func (r *captureRegistry) init(source PageSource) {
	r.source = source
	r.now = time.Now
	r.live = make(map[uint64]*queryCapture)
	r.drained = make(chan struct{})
}

func (r *captureRegistry) beginRequest() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closing {
		return ErrQueryCapacityExhausted
	}
	r.requests++
	return nil
}

func (r *captureRegistry) endRequest() {
	r.mu.Lock()
	r.requests--
	r.signalDrain()
	r.mu.Unlock()
}

func (r *captureRegistry) reserve() (*queryCapture, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closing || r.count >= maxQueryCaptures {
		return nil, ErrQueryCapacityExhausted
	}
	r.next++
	r.count++
	return &queryCapture{id: r.next, deadline: r.now().Add(queryCaptureLifetime), users: 1}, nil
}

func (r *captureRegistry) publish(c *queryCapture) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closing {
		return ErrQueryCapacityExhausted
	}
	left := c.deadline.Sub(r.now())
	if left <= 0 {
		return ErrCaptureExpired
	}
	c.published = true
	r.live[c.id] = c
	c.timer = time.AfterFunc(left, func() { r.retire(c) })
	return nil
}

func (r *captureRegistry) acquire(id uint64) (*queryCapture, error) {
	r.mu.Lock()
	c := r.live[id]
	if c == nil || r.closing {
		r.mu.Unlock()
		return nil, ErrCaptureExpired
	}
	if !r.now().Before(c.deadline) {
		release := r.markRetired(c)
		r.mu.Unlock()
		if release {
			r.release(c)
		}
		return nil, ErrCaptureExpired
	}
	c.users++
	r.mu.Unlock()
	return c, nil
}

func (r *captureRegistry) finish(c *queryCapture) {
	r.mu.Lock()
	c.users--
	if !c.published {
		r.markRetired(c)
	}
	release := c.retired && c.users == 0
	r.mu.Unlock()
	if release {
		r.release(c)
	}
}

func (r *captureRegistry) retire(c *queryCapture) {
	r.mu.Lock()
	release := r.markRetired(c)
	r.mu.Unlock()
	if release {
		r.release(c)
	}
}

// markRetired hands the sole release to its caller or to the last user.
func (r *captureRegistry) markRetired(c *queryCapture) bool {
	if c.retired {
		return false
	}
	c.retired = true
	delete(r.live, c.id)
	if c.timer != nil {
		c.timer.Stop()
	}
	return c.users == 0
}

func (r *captureRegistry) release(c *queryCapture) {
	var err error
	if c.sourceID != "" {
		err = r.source.Release(c.sourceID)
		if err != nil {
			log.Printf("release query capture: %v", err)
		}
	}
	r.mu.Lock()
	if r.releaseErr == nil {
		r.releaseErr = err
	}
	r.count--
	r.signalDrain()
	r.mu.Unlock()
}

func (r *captureRegistry) signalDrain() {
	if r.closing && r.count == 0 && r.requests == 0 {
		close(r.drained)
	}
}

func (r *captureRegistry) shutdown(ctx context.Context) error {
	r.mu.Lock()
	if !r.closing {
		r.closing = true
		for _, c := range r.live {
			if r.markRetired(c) {
				go r.release(c)
			}
		}
		r.signalDrain()
	}
	r.mu.Unlock()

	select {
	case <-r.drained:
		return r.releaseErr
	case <-ctx.Done():
		return ctx.Err()
	}
}
