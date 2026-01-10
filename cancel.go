package relay

import (
	"context"
	"fmt"
	"sync"
)

func (srv *Server) runCancelListener(ctx context.Context) {
	defer srv.wg.Done()
	ch, closeFn := srv.rdb.SubscribeCancel(ctx)
	defer closeFn()
	for {
		select {
		case <-ctx.Done():
			return
		case id, ok := <-ch:
			if !ok {
				return
			}
			if srv.cancels.cancel(id) {
				srv.logger.Info(fmt.Sprintf("relay: cancelled running task %s", id))
			}
		}
	}
}

type cancelRegistry struct {
	mu sync.Mutex
	m  map[string]context.CancelFunc
}

func newCancelRegistry() *cancelRegistry {
	return &cancelRegistry{m: make(map[string]context.CancelFunc)}
}

func (c *cancelRegistry) add(id string, fn context.CancelFunc) {
	c.mu.Lock()
	c.m[id] = fn
	c.mu.Unlock()
}

func (c *cancelRegistry) remove(id string) {
	c.mu.Lock()
	delete(c.m, id)
	c.mu.Unlock()
}

func (c *cancelRegistry) cancel(id string) bool {
	c.mu.Lock()
	fn, ok := c.m[id]
	c.mu.Unlock()
	if ok {
		fn()
	}
	return ok
}
