package httpapi

import (
	"log"
	"sync"
	"time"
)

const (
	maxCaptureLifetime = 15 * time.Minute
	maxQueryCaptures   = 64
)

type queryCapture struct {
	sourceID string
	deadline time.Time
	blocks   []queryBlock
	timer    *time.Timer
	active   int
	retired  bool
}

type captureRegistry struct {
	mu     sync.Mutex
	next   uint64
	live   map[uint64]*queryCapture
	slots  chan struct{}
	source PageSource
}

func newCaptureRegistry(source PageSource) captureRegistry {
	return captureRegistry{
		live:   make(map[uint64]*queryCapture),
		slots:  make(chan struct{}, maxQueryCaptures),
		source: source,
	}
}

func (r *captureRegistry) reserve() bool {
	select {
	case r.slots <- struct{}{}:
		return true
	default:
		return false
	}
}

func (r *captureRegistry) release(c *queryCapture) {
	if c.sourceID != "" {
		if err := r.source.Release(c.sourceID); err != nil {
			log.Printf("release query capture: %v", err)
		}
	}
	<-r.slots
}

// publish transfers ownership to the registry and takes an operation reference
// for the initial response. Even a failed body write can expose the cursor.
func (r *captureRegistry) publish(c *queryCapture) (uint64, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delay := time.Until(c.deadline)
	if delay <= 0 {
		return 0, ErrCaptureExpired
	}
	r.next++
	id := r.next
	c.active = 1
	r.live[id] = c
	c.timer = time.AfterFunc(delay, func() { r.retire(id) })
	return id, nil
}

func (r *captureRegistry) acquire(id uint64) (*queryCapture, error) {
	r.mu.Lock()
	c := r.live[id]
	if c == nil {
		r.mu.Unlock()
		return nil, ErrCaptureExpired
	}
	if !time.Now().Before(c.deadline) {
		r.mu.Unlock()
		r.retire(id)
		return nil, ErrCaptureExpired
	}
	c.active++
	r.mu.Unlock()
	return c, nil
}

// canContinue checks that an admitted request can still publish a cursor.
// An admitted terminal response may finish after the registry retires its pin.
func (r *captureRegistry) canContinue(c *queryCapture) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return !c.retired && time.Now().Before(c.deadline)
}

func (r *captureRegistry) retire(id uint64) {
	r.mu.Lock()
	c := r.live[id]
	if c == nil {
		r.mu.Unlock()
		return
	}
	delete(r.live, id)
	c.timer.Stop()
	c.retired = true
	release := c.active == 0
	r.mu.Unlock()
	if release {
		r.release(c)
	}
}

func (r *captureRegistry) done(c *queryCapture) {
	r.mu.Lock()
	c.active--
	release := c.retired && c.active == 0
	r.mu.Unlock()
	if release {
		r.release(c)
	}
}
