package relay

import (
	"context"
	"errors"
	"math/rand"
	"os"
	"os/signal"
	"sort"
	"sync"
	"syscall"
	"time"

	"github.com/MohamedAklamaash/relay/internal/rdb"
)

const (
	leaseDuration     = 30 * time.Second
	heartbeatInterval = 10 * time.Second
	idlePollInterval  = time.Second
	forwardInterval   = 5 * time.Second
	recoverInterval   = 30 * time.Second
)

type Config struct {
	Concurrency     int
	Queues          map[string]int
	StrictPriority  bool
	RetryDelayFunc  RetryDelayFunc
	ShutdownTimeout time.Duration
	Logger          Logger
}

type weightedQueue struct {
	name   string
	weight int
}

type Server struct {
	rdb             *rdb.RDB
	handler         Handler
	logger          Logger
	retryDelay      RetryDelayFunc
	concurrency     int
	queues          []weightedQueue
	strict          bool
	shutdownTimeout time.Duration
	cancels         *cancelRegistry

	cancel context.CancelFunc
	wg     sync.WaitGroup
	mu     sync.Mutex
	closed bool
}

func NewServer(r RedisConnOpt, cfg Config) *Server {
	concurrency := cfg.Concurrency
	if concurrency < 1 {
		concurrency = 10
	}
	queues := normalizeQueues(cfg.Queues)
	logger := cfg.Logger
	if logger == nil {
		logger = defaultLogger()
	}
	retry := cfg.RetryDelayFunc
	if retry == nil {
		retry = defaultRetryDelay
	}
	shutdown := cfg.ShutdownTimeout
	if shutdown <= 0 {
		shutdown = 8 * time.Second
	}
	return &Server{
		rdb:             rdb.New(r.MakeClient()),
		logger:          logger,
		retryDelay:      retry,
		concurrency:     concurrency,
		queues:          queues,
		strict:          cfg.StrictPriority,
		shutdownTimeout: shutdown,
		cancels:         newCancelRegistry(),
	}
}

func normalizeQueues(in map[string]int) []weightedQueue {
	if len(in) == 0 {
		return []weightedQueue{{name: defaultQueue, weight: 1}}
	}
	out := make([]weightedQueue, 0, len(in))
	for name, w := range in {
		if w < 1 {
			w = 1
		}
		out = append(out, weightedQueue{name: name, weight: w})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].weight == out[j].weight {
			return out[i].name < out[j].name
		}
		return out[i].weight > out[j].weight
	})
	return out
}

func (srv *Server) queueNames() []string {
	names := make([]string, len(srv.queues))
	for i, q := range srv.queues {
		names[i] = q.name
	}
	return names
}

func (srv *Server) queueOrder() []string {
	if srv.strict || len(srv.queues) == 1 {
		return srv.queueNames()
	}
	var expanded []string
	for _, q := range srv.queues {
		for i := 0; i < q.weight; i++ {
			expanded = append(expanded, q.name)
		}
	}
	rand.Shuffle(len(expanded), func(i, j int) {
		expanded[i], expanded[j] = expanded[j], expanded[i]
	})
	seen := make(map[string]bool, len(srv.queues))
	out := make([]string, 0, len(srv.queues))
	for _, n := range expanded {
		if !seen[n] {
			seen[n] = true
			out = append(out, n)
		}
	}
	return out
}

func (srv *Server) Start(h Handler) error {
	if h == nil {
		return errors.New("relay: handler cannot be nil")
	}
	srv.mu.Lock()
	defer srv.mu.Unlock()
	if srv.cancel != nil {
		return errors.New("relay: server already started")
	}
	if err := srv.rdb.Ping(context.Background()); err != nil {
		return err
	}
	srv.handler = h

	ctx, cancel := context.WithCancel(context.Background())
	srv.cancel = cancel

	srv.wg.Add(4)
	go srv.runProcessor(ctx)
	go srv.runForwarder(ctx)
	go srv.runRecoverer(ctx)
	go srv.runCancelListener(ctx)

	srv.logger.Info("relay: server started")
	return nil
}

func (srv *Server) Run(h Handler) error {
	if err := srv.Start(h); err != nil {
		return err
	}
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	srv.Shutdown()
	return nil
}

func (srv *Server) Shutdown() {
	srv.mu.Lock()
	if srv.closed || srv.cancel == nil {
		srv.mu.Unlock()
		return
	}
	srv.closed = true
	cancel := srv.cancel
	srv.mu.Unlock()

	srv.logger.Info("relay: shutting down")
	cancel()

	done := make(chan struct{})
	go func() {
		srv.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(srv.shutdownTimeout):
		srv.logger.Warn("relay: shutdown timed out, some tasks may be reprocessed")
	}
	_ = srv.rdb.Close()
	srv.logger.Info("relay: stopped")
}
