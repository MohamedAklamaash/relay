package relay

import (
	"errors"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/robfig/cron/v3"
)

type SchedulerOpts struct {
	Logger   Logger
	Location *time.Location
}

type schedEntry struct {
	id   string
	spec string
	task *Task
	opts []Option
}

type Scheduler struct {
	client  *Client
	cron    *cron.Cron
	logger  Logger
	nowFunc func() time.Time

	mu      sync.Mutex
	entries map[cron.EntryID]*schedEntry
}

func NewScheduler(r RedisConnOpt, opts *SchedulerOpts) *Scheduler {
	if opts == nil {
		opts = &SchedulerOpts{}
	}
	loc := opts.Location
	if loc == nil {
		loc = time.Local
	}
	logger := opts.Logger
	if logger == nil {
		logger = defaultLogger()
	}
	return &Scheduler{
		client:  NewClient(r),
		cron:    cron.New(cron.WithLocation(loc)),
		logger:  logger,
		nowFunc: time.Now,
		entries: make(map[cron.EntryID]*schedEntry),
	}
}

func (s *Scheduler) Register(cronspec string, task *Task, opts ...Option) (string, error) {
	if task == nil {
		return "", errors.New("relay: cannot register nil task")
	}
	entry := &schedEntry{id: uuid.NewString(), spec: cronspec, task: task, opts: opts}
	cid, err := s.cron.AddFunc(cronspec, func() { s.fire(entry) })
	if err != nil {
		return "", err
	}
	s.mu.Lock()
	s.entries[cid] = entry
	s.mu.Unlock()
	return entry.id, nil
}

func (s *Scheduler) fire(e *schedEntry) {
	fireAt := s.nowFunc().Truncate(time.Minute)
	id := fmt.Sprintf("cron:%s:%d", e.id, fireAt.Unix())
	opts := append([]Option{TaskID(id)}, e.opts...)

	_, err := s.client.Enqueue(e.task, opts...)
	switch {
	case err == nil:
		s.logger.Info(fmt.Sprintf("relay: scheduler enqueued %s (%s)", e.task.Type(), e.spec))
	case errors.Is(err, ErrTaskIDConflict), errors.Is(err, ErrDuplicateTask):
		return
	default:
		s.logger.Error(fmt.Sprintf("relay: scheduler failed to enqueue %s: %v", e.task.Type(), err))
	}
}

func (s *Scheduler) Start() error {
	s.cron.Start()
	s.logger.Info("relay: scheduler started")
	return nil
}

func (s *Scheduler) Run() error {
	if err := s.Start(); err != nil {
		return err
	}
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	s.Shutdown()
	return nil
}

func (s *Scheduler) Shutdown() {
	ctx := s.cron.Stop()
	<-ctx.Done()
	_ = s.client.Close()
	s.logger.Info("relay: scheduler stopped")
}
