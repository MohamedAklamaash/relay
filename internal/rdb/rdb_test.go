package rdb

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/MohamedAklamaash/relay/internal/base"
	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func newTestRDB(t *testing.T) *RDB {
	t.Helper()
	mr := miniredis.RunT(t)
	c := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	return New(c)
}

func msg(id, queue string) *base.TaskMessage {
	return &base.TaskMessage{ID: id, Type: "test", Payload: []byte("x"), Queue: queue, MaxRetry: 3}
}

func TestEnqueueDequeueDone(t *testing.T) {
	r := newTestRDB(t)
	ctx := context.Background()

	if err := r.Enqueue(ctx, msg("t1", "default"), time.Now(), 0); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	got, err := r.Dequeue(ctx, "default", time.Now().Add(time.Minute))
	if err != nil {
		t.Fatalf("dequeue: %v", err)
	}
	if got.ID != "t1" {
		t.Fatalf("want t1, got %s", got.ID)
	}

	if err := r.Done(ctx, got, 0); err != nil {
		t.Fatalf("done: %v", err)
	}
	if _, err := r.Get(ctx, "default", "t1"); !errors.Is(err, ErrTaskNotFound) {
		t.Fatalf("want task deleted, got %v", err)
	}
}

func TestDequeueEmpty(t *testing.T) {
	r := newTestRDB(t)
	_, err := r.Dequeue(context.Background(), "default", time.Now().Add(time.Minute))
	if !errors.Is(err, ErrNoTask) {
		t.Fatalf("want ErrNoTask, got %v", err)
	}
}

func TestUniqueDedup(t *testing.T) {
	r := newTestRDB(t)
	ctx := context.Background()
	m := msg("u1", "default")
	m.UniqueHash = "abc"

	if err := r.Enqueue(ctx, m, time.Now(), time.Minute); err != nil {
		t.Fatalf("first enqueue: %v", err)
	}
	m2 := msg("u2", "default")
	m2.UniqueHash = "abc"
	if err := r.Enqueue(ctx, m2, time.Now(), time.Minute); !errors.Is(err, ErrDuplicateTask) {
		t.Fatalf("want ErrDuplicateTask, got %v", err)
	}
}

func TestTaskIDConflict(t *testing.T) {
	r := newTestRDB(t)
	ctx := context.Background()
	if err := r.Enqueue(ctx, msg("same", "default"), time.Now(), 0); err != nil {
		t.Fatalf("first: %v", err)
	}
	if err := r.Enqueue(ctx, msg("same", "default"), time.Now(), 0); !errors.Is(err, ErrTaskIDConflict) {
		t.Fatalf("want ErrTaskIDConflict, got %v", err)
	}
}

func TestRetryThenForward(t *testing.T) {
	r := newTestRDB(t)
	ctx := context.Background()
	if err := r.Enqueue(ctx, msg("r1", "default"), time.Now(), 0); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	got, err := r.Dequeue(ctx, "default", time.Now().Add(time.Minute))
	if err != nil {
		t.Fatalf("dequeue: %v", err)
	}

	got.Retried++
	if err := r.Retry(ctx, got, time.Now().Add(-time.Second)); err != nil {
		t.Fatalf("retry: %v", err)
	}
	stats, _ := r.Stats(ctx, "default")
	if stats.Retry != 1 {
		t.Fatalf("want 1 in retry, got %d", stats.Retry)
	}

	n, err := r.ForwardDue(ctx, "default")
	if err != nil {
		t.Fatalf("forward: %v", err)
	}
	if n != 1 {
		t.Fatalf("want 1 forwarded, got %d", n)
	}
	if _, err := r.Dequeue(ctx, "default", time.Now().Add(time.Minute)); err != nil {
		t.Fatalf("dequeue after forward: %v", err)
	}
}

func TestArchive(t *testing.T) {
	r := newTestRDB(t)
	ctx := context.Background()
	r.Enqueue(ctx, msg("a1", "default"), time.Now(), 0)
	got, _ := r.Dequeue(ctx, "default", time.Now().Add(time.Minute))
	got.ErrorMsg = "boom"
	if err := r.Archive(ctx, got); err != nil {
		t.Fatalf("archive: %v", err)
	}
	stats, _ := r.Stats(ctx, "default")
	if stats.Archived != 1 {
		t.Fatalf("want 1 archived, got %d", stats.Archived)
	}
}

func TestRecoverExpiredLease(t *testing.T) {
	r := newTestRDB(t)
	ctx := context.Background()
	r.Enqueue(ctx, msg("rec1", "default"), time.Now(), 0)
	if _, err := r.Dequeue(ctx, "default", time.Now().Add(-time.Second)); err != nil {
		t.Fatalf("dequeue: %v", err)
	}

	n, err := r.Recover(ctx, "default")
	if err != nil {
		t.Fatalf("recover: %v", err)
	}
	if n != 1 {
		t.Fatalf("want 1 recovered, got %d", n)
	}
	if _, err := r.Dequeue(ctx, "default", time.Now().Add(time.Minute)); err != nil {
		t.Fatalf("redequeue after recover: %v", err)
	}
}

func TestPauseUnpause(t *testing.T) {
	r := newTestRDB(t)
	ctx := context.Background()
	if err := r.Pause(ctx, "default"); err != nil {
		t.Fatalf("pause: %v", err)
	}
	paused, _ := r.IsPaused(ctx, "default")
	if !paused {
		t.Fatal("want paused")
	}
	r.Unpause(ctx, "default")
	paused, _ = r.IsPaused(ctx, "default")
	if paused {
		t.Fatal("want unpaused")
	}
}
