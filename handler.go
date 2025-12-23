package relay

import (
	"context"
	"fmt"
	"sync"
)

type Handler interface {
	ProcessTask(context.Context, *Task) error
}

type HandlerFunc func(context.Context, *Task) error

func (f HandlerFunc) ProcessTask(ctx context.Context, t *Task) error {
	return f(ctx, t)
}

type ServeMux struct {
	mu sync.RWMutex
	m  map[string]Handler
}

func NewServeMux() *ServeMux {
	return &ServeMux{m: make(map[string]Handler)}
}

func (mux *ServeMux) Handle(pattern string, h Handler) {
	mux.mu.Lock()
	defer mux.mu.Unlock()
	if pattern == "" {
		panic("relay: invalid pattern")
	}
	if h == nil {
		panic("relay: nil handler")
	}
	mux.m[pattern] = h
}

func (mux *ServeMux) HandleFunc(pattern string, fn func(context.Context, *Task) error) {
	mux.Handle(pattern, HandlerFunc(fn))
}

func (mux *ServeMux) ProcessTask(ctx context.Context, t *Task) error {
	mux.mu.RLock()
	h, ok := mux.m[t.Type()]
	mux.mu.RUnlock()
	if !ok {
		return fmt.Errorf("relay: no handler registered for %q", t.Type())
	}
	return h.ProcessTask(ctx, t)
}
