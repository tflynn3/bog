package idempotency

import (
	"context"
	"sync"
)

// keyGate gives each active key a semaphore. References cover both holders and
// waiters so cancellation and release cannot split a key between semaphores.
type keyGate struct {
	mu      sync.Mutex
	entries map[ScopedKey]*gateEntry
}

type gateEntry struct {
	token chan struct{}
	users int
}

func (g *keyGate) acquire(ctx context.Context, key ScopedKey) (func(), error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	g.mu.Lock()
	entry := g.entries[key]
	if entry == nil {
		entry = &gateEntry{token: make(chan struct{}, 1)}
		if g.entries == nil {
			g.entries = make(map[ScopedKey]*gateEntry)
		}
		g.entries[key] = entry
	}
	entry.users++
	g.mu.Unlock()

	select {
	case entry.token <- struct{}{}:
		// Cancellation and availability can become ready together.
		if err := ctx.Err(); err != nil {
			g.release(key, entry)
			return nil, err
		}
		return func() { g.release(key, entry) }, nil
	case <-ctx.Done():
		g.drop(key, entry)
		return nil, ctx.Err()
	}
}

func (g *keyGate) release(key ScopedKey, entry *gateEntry) {
	<-entry.token
	g.drop(key, entry)
}

func (g *keyGate) drop(key ScopedKey, entry *gateEntry) {
	g.mu.Lock()
	defer g.mu.Unlock()
	entry.users--
	if entry.users == 0 {
		delete(g.entries, key)
		if len(g.entries) == 0 {
			// Release the map's storage after an admission burst.
			g.entries = nil
		}
	}
}
