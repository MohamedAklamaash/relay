package relay

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
)

func testRedis(t *testing.T) RedisClientOpt {
	t.Helper()
	mr := miniredis.RunT(t)
	return RedisClientOpt{Addr: mr.Addr()}
}

func TestServeMuxRouting(t *testing.T) {
	mux := NewServeMux()
	called := ""
	mux.HandleFunc("a", func(ctx context.Context, task *Task) error {
		called = "a"
		return nil
	})
	mux.HandleFunc("b", func(ctx context.Context, task *Task) error {
		called = "b"
		return nil
	})

	if err := mux.ProcessTask(context.Background(), NewTask("b", nil)); err != nil {
		t.Fatal(err)
	}
	if called != "b" {
		t.Fatalf("want handler b, got %q", called)
	}
	if err := mux.ProcessTask(context.Background(), NewTask("missing", nil)); err == nil {
		t.Fatal("want error for unregistered type")
	}
}

func TestEndToEndProcessing(t *testing.T) {
	opt := testRedis(t)
	client := NewClient(opt)
	defer client.Close()

	processed := make(chan string, 1)
	srv := NewServer(opt, Config{Concurrency: 2})

	mux := NewServeMux()
	mux.HandleFunc("greet", func(ctx context.Context, task *Task) error {
		processed <- string(task.Payload())
		return nil
	})

	if err := srv.Start(mux); err != nil {
		t.Fatal(err)
	}
	defer srv.Shutdown()

	if _, err := client.Enqueue(NewTask("greet", []byte("hello"))); err != nil {
		t.Fatal(err)
	}

	select {
	case got := <-processed:
		if got != "hello" {
			t.Fatalf("want hello, got %q", got)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("task was not processed in time")
	}
}

func TestEnqueueUniqueRejected(t *testing.T) {
	opt := testRedis(t)
	client := NewClient(opt)
	defer client.Close()

	task := NewTask("dup", []byte("payload"))
	if _, err := client.Enqueue(task, Unique(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Enqueue(task, Unique(time.Minute)); !errors.Is(err, ErrDuplicateTask) {
		t.Fatalf("want ErrDuplicateTask, got %v", err)
	}
}

func TestCancelRunningTask(t *testing.T) {
	opt := testRedis(t)
	client := NewClient(opt)
	defer client.Close()

	started := make(chan struct{})
	result := make(chan error, 1)
	srv := NewServer(opt, Config{Concurrency: 1})

	mux := NewServeMux()
	mux.HandleFunc("slow", func(ctx context.Context, task *Task) error {
		close(started)
		<-ctx.Done()
		result <- ctx.Err()
		return ctx.Err()
	})

	if err := srv.Start(mux); err != nil {
		t.Fatal(err)
	}
	defer srv.Shutdown()

	info, err := client.Enqueue(NewTask("slow", nil), Timeout(time.Minute))
	if err != nil {
		t.Fatal(err)
	}

	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("handler never started")
	}

	insp := NewInspector(opt)
	defer insp.Close()
	if err := insp.CancelTask(info.ID); err != nil {
		t.Fatal(err)
	}

	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("want context.Canceled, got %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("task was not cancelled")
	}
}

func TestEnqueueScheduledState(t *testing.T) {
	opt := testRedis(t)
	client := NewClient(opt)
	defer client.Close()

	info, err := client.Enqueue(NewTask("later", nil), ProcessIn(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if info.State != "scheduled" {
		t.Fatalf("want scheduled, got %s", info.State)
	}
}
