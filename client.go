package relay

import (
	"context"
	"errors"
	"time"

	"github.com/MohamedAklamaash/relay/internal/base"
	"github.com/MohamedAklamaash/relay/internal/rdb"
	"github.com/google/uuid"
)

var (
	ErrDuplicateTask  = rdb.ErrDuplicateTask
	ErrTaskIDConflict = rdb.ErrTaskIDConflict
)

type Client struct {
	rdb *rdb.RDB
}

func NewClient(r RedisConnOpt) *Client {
	return &Client{rdb: rdb.New(r.MakeClient())}
}

func (c *Client) Close() error {
	return c.rdb.Close()
}

func (c *Client) Enqueue(task *Task, opts ...Option) (*TaskInfo, error) {
	return c.EnqueueContext(context.Background(), task, opts...)
}

func (c *Client) EnqueueContext(ctx context.Context, task *Task, opts ...Option) (*TaskInfo, error) {
	if task == nil {
		return nil, errors.New("relay: cannot enqueue nil task")
	}
	o := composeOptions(defaultOptions(), append(task.opts, opts...)...)

	id := o.taskID
	if id == "" {
		id = uuid.NewString()
	}

	msg := &base.TaskMessage{
		ID:         id,
		Type:       task.typename,
		Payload:    task.payload,
		Queue:      o.queue,
		MaxRetry:   o.maxRetry,
		Timeout:    int64(o.timeout.Seconds()),
		Retention:  int64(o.retention.Seconds()),
		EnqueuedAt: time.Now().Unix(),
	}
	if !o.deadline.IsZero() {
		msg.Deadline = o.deadline.Unix()
	}
	if o.uniqueTTL > 0 {
		msg.UniqueHash = base.HashUnique(o.queue, task.typename, task.payload)
	}

	if err := c.rdb.Enqueue(ctx, msg, o.processAt, o.uniqueTTL); err != nil {
		return nil, err
	}

	state := "pending"
	if o.processAt.After(time.Now()) {
		state = "scheduled"
	}
	return &TaskInfo{
		ID:            msg.ID,
		Queue:         msg.Queue,
		Type:          msg.Type,
		Payload:       msg.Payload,
		State:         state,
		MaxRetry:      msg.MaxRetry,
		NextProcessAt: o.processAt,
	}, nil
}
