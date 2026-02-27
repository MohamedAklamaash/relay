//go:build integration

package relay

import (
	"context"
	"errors"
	"os"
	"sync"
	"testing"
	"time"
)

func integrationRedis(t *testing.T) RedisClientOpt {
	t.Helper()
	addr := os.Getenv("RELAY_REDIS_ADDR")
	if addr == "" {
		t.Skip("RELAY_REDIS_ADDR not set")
	}
	opt := RedisClientOpt{Addr: addr}
	c := opt.MakeClient()
	if err := c.FlushDB(context.Background()).Err(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	c.Close()
	return opt
}

func TestIntegrationImmediateAndDelayed(t *testing.T) {
	opt := integrationRedis(t)
	client := NewClient(opt)
	defer client.Close()

	var mu sync.Mutex
	seen := map[string]bool{}
	done := make(chan string, 2)

	srv := NewServer(opt, Config{Concurrency: 5})
	mux := NewServeMux()
	mux.HandleFunc("job", func(ctx context.Context, task *Task) error {
		id := string(task.Payload())
		mu.Lock()
		seen[id] = true
		mu.Unlock()
		done <- id
		return nil
	})
	if err := srv.Start(mux); err != nil {
		t.Fatal(err)
	}
	defer srv.Shutdown()

	if _, err := client.Enqueue(NewTask("job", []byte("immediate"))); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Enqueue(NewTask("job", []byte("delayed")), ProcessIn(2*time.Second)); err != nil {
		t.Fatal(err)
	}

	deadline := time.After(20 * time.Second)
	for count := 0; count < 2; count++ {
		select {
		case <-done:
		case <-deadline:
			t.Fatalf("only processed %d/2 tasks", count)
		}
	}
	mu.Lock()
	defer mu.Unlock()
	if !seen["immediate"] || !seen["delayed"] {
		t.Fatalf("missing tasks: %v", seen)
	}
}

func TestIntegrationUnique(t *testing.T) {
	opt := integrationRedis(t)
	client := NewClient(opt)
	defer client.Close()

	task := NewTask("once", []byte("x"))
	if _, err := client.Enqueue(task, Unique(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Enqueue(task, Unique(time.Minute)); !errors.Is(err, ErrDuplicateTask) {
		t.Fatalf("want ErrDuplicateTask, got %v", err)
	}
}

func TestIntegrationCancel(t *testing.T) {
	opt := integrationRedis(t)
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
	<-started

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
	case <-time.After(10 * time.Second):
		t.Fatal("task not cancelled")
	}
}
