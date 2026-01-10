package relay

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/MohamedAklamaash/relay/internal/base"
	"github.com/MohamedAklamaash/relay/internal/rdb"
)

func (srv *Server) runProcessor(ctx context.Context) {
	defer srv.wg.Done()
	sema := make(chan struct{}, srv.concurrency)

	for {
		select {
		case <-ctx.Done():
			return
		case sema <- struct{}{}:
		}

		msg, err := srv.dequeue(ctx)
		if err != nil {
			<-sema
			if ctx.Err() != nil {
				return
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(idlePollInterval):
			}
			continue
		}

		srv.wg.Add(1)
		go func() {
			defer srv.wg.Done()
			defer func() { <-sema }()
			srv.process(ctx, msg)
		}()
	}
}

func (srv *Server) dequeue(ctx context.Context) (*base.TaskMessage, error) {
	expiry := time.Now().Add(leaseDuration)
	for _, q := range srv.queueOrder() {
		paused, err := srv.rdb.IsPaused(ctx, q)
		if err == nil && paused {
			continue
		}
		msg, err := srv.rdb.Dequeue(ctx, q, expiry)
		if errors.Is(err, rdb.ErrNoTask) {
			continue
		}
		if err != nil {
			return nil, err
		}
		return msg, nil
	}
	return nil, rdb.ErrNoTask
}

func (srv *Server) process(ctx context.Context, msg *base.TaskMessage) {
	taskCtx, cancel := taskContext(msg)
	defer cancel()

	srv.cancels.add(msg.ID, cancel)
	defer srv.cancels.remove(msg.ID)

	stopHeartbeat := srv.startHeartbeat(ctx, msg)
	defer stopHeartbeat()

	tasksInProgress.WithLabelValues(msg.Queue).Inc()
	defer tasksInProgress.WithLabelValues(msg.Queue).Dec()

	task := NewTask(msg.Type, msg.Payload)
	start := time.Now()
	err := srv.safeProcess(taskCtx, task)
	processingDuration.WithLabelValues(msg.Queue, msg.Type).Observe(time.Since(start).Seconds())

	if err == nil {
		tasksProcessed.WithLabelValues(msg.Queue, msg.Type).Inc()
		retention := time.Duration(msg.Retention) * time.Second
		if derr := srv.rdb.Done(context.Background(), msg, retention); derr != nil {
			srv.logger.Error(fmt.Sprintf("relay: failed to mark task %s done: %v", msg.ID, derr))
		}
		return
	}

	tasksFailed.WithLabelValues(msg.Queue, msg.Type).Inc()
	srv.handleFailure(msg, task, err)
}

func (srv *Server) safeProcess(ctx context.Context, task *Task) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("relay: handler panic: %v", r)
		}
	}()
	return srv.handler.ProcessTask(ctx, task)
}

func (srv *Server) handleFailure(msg *base.TaskMessage, task *Task, procErr error) {
	msg.ErrorMsg = procErr.Error()
	msg.LastFailedAt = time.Now().Unix()

	if errors.Is(procErr, SkipRetry) || msg.Retried >= msg.MaxRetry {
		tasksArchived.WithLabelValues(msg.Queue, msg.Type).Inc()
		if aerr := srv.rdb.Archive(context.Background(), msg); aerr != nil {
			srv.logger.Error(fmt.Sprintf("relay: failed to archive task %s: %v", msg.ID, aerr))
		}
		return
	}

	msg.Retried++
	tasksRetried.WithLabelValues(msg.Queue, msg.Type).Inc()
	delay := srv.retryDelay(msg.Retried, procErr, task)
	processAt := time.Now().Add(delay)
	if rerr := srv.rdb.Retry(context.Background(), msg, processAt); rerr != nil {
		srv.logger.Error(fmt.Sprintf("relay: failed to schedule retry for task %s: %v", msg.ID, rerr))
	}
}

func taskContext(msg *base.TaskMessage) (context.Context, context.CancelFunc) {
	var deadline time.Time
	if msg.Timeout > 0 {
		deadline = time.Now().Add(time.Duration(msg.Timeout) * time.Second)
	}
	if msg.Deadline > 0 {
		d := time.Unix(msg.Deadline, 0)
		if deadline.IsZero() || d.Before(deadline) {
			deadline = d
		}
	}
	if deadline.IsZero() {
		return context.WithCancel(context.Background())
	}
	return context.WithDeadline(context.Background(), deadline)
}

func (srv *Server) startHeartbeat(ctx context.Context, msg *base.TaskMessage) func() {
	done := make(chan struct{})
	go func() {
		ticker := time.NewTicker(heartbeatInterval)
		defer ticker.Stop()
		for {
			select {
			case <-done:
				return
			case <-ctx.Done():
				return
			case <-ticker.C:
				expiry := time.Now().Add(leaseDuration)
				if err := srv.rdb.ExtendLease(context.Background(), msg.Queue, msg.ID, expiry); err != nil {
					srv.logger.Warn(fmt.Sprintf("relay: failed to extend lease for %s: %v", msg.ID, err))
				}
			}
		}
	}()
	return func() { close(done) }
}
