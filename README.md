# relay

A simple, reliable, distributed job scheduler for Go, backed by Redis.

Enqueue background jobs from anywhere, process them across a fleet of workers, and schedule recurring work with cron. Jobs survive worker crashes and are retried automatically.

## Features

- At-least-once delivery with automatic crash recovery (visibility-timeout leases)
- Immediate, delayed (`ProcessIn` / `ProcessAt`), and recurring **cron** jobs
- Automatic retries with exponential backoff and a dead-letter archive
- Optional task deduplication (`Unique`) and explicit task IDs
- Weighted priority queues with pause/resume
- Per-task timeouts, deadlines, and remote cancellation
- Prometheus metrics out of the box
- A CLI to inspect and manage queues

## Install

```sh
go get github.com/MohamedAklamaash/relay
```

Requires Go 1.24+ and a Redis server.

## Quickstart

Enqueue a task:

```go
client := relay.NewClient(relay.RedisClientOpt{Addr: "127.0.0.1:6379"})
defer client.Close()

client.Enqueue(relay.NewTask("email:send", []byte(`{"to":"a@b.com"}`)))
client.Enqueue(relay.NewTask("report:build", nil), relay.ProcessIn(10*time.Minute))
```

Process tasks:

```go
srv := relay.NewServer(relay.RedisClientOpt{Addr: "127.0.0.1:6379"}, relay.Config{
    Concurrency: 10,
    Queues:      map[string]int{"critical": 6, "default": 3, "low": 1},
})

mux := relay.NewServeMux()
mux.HandleFunc("email:send", func(ctx context.Context, t *relay.Task) error {
    return sendEmail(t.Payload())
})

srv.Run(mux)
```

Schedule recurring work:

```go
scheduler := relay.NewScheduler(relay.RedisClientOpt{Addr: "127.0.0.1:6379"}, nil)
scheduler.Register("0 * * * *", relay.NewTask("report:build", nil))
scheduler.Run()
```

## Enqueue options

| Option | Description |
| --- | --- |
| `Queue(name)` | Target queue (default `"default"`) |
| `ProcessIn(d)` / `ProcessAt(t)` | Delay execution |
| `MaxRetry(n)` | Retry attempts before archiving (default 25) |
| `Timeout(d)` / `Deadline(t)` | Cancel the handler context after a limit |
| `Unique(ttl)` | Reject duplicate enqueues within the window |
| `TaskID(id)` | Set an explicit ID; conflicting IDs are rejected |
| `Retention(d)` | Keep completed task data for inspection |

## Delivery semantics

`relay` guarantees **at-least-once** delivery. A task may run more than once (for
example when a worker crashes after finishing but before acknowledging), so your
handlers must be **idempotent**. `Unique` deduplicates *enqueues*, not executions.

Return `relay.SkipRetry` from a handler to archive a task immediately instead of
retrying.

## Metrics

`relay` exposes Prometheus metrics (processed, failed, retried, archived,
in-progress, and processing duration). Mount the handler on your own server:

```go
http.Handle("/metrics", relay.MetricsHandler())
go http.ListenAndServe(":2112", nil)
```

## Cancellation

Cancel a running task from anywhere; the worker's handler context is cancelled:

```go
insp := relay.NewInspector(relay.RedisClientOpt{Addr: "127.0.0.1:6379"})
insp.CancelTask(taskID)
```

## CLI

```sh
go install github.com/MohamedAklamaash/relay/cmd/relay@latest

relay stats
relay ls default retry
relay run default <task-id>
relay rm default <task-id>
relay cancel <task-id>
relay pause low
```

## Running locally

```sh
docker compose up -d redis
go run ./examples/worker
go run ./examples/producer
go run ./examples/cron
```

## License

MIT
