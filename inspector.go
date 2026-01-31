package relay

import (
	"context"

	"github.com/MohamedAklamaash/relay/internal/base"
	"github.com/MohamedAklamaash/relay/internal/rdb"
)

type QueueStats struct {
	Queue     string
	Paused    bool
	Pending   int64
	Active    int64
	Scheduled int64
	Retry     int64
	Archived  int64
	Completed int64
}

type Inspector struct {
	rdb *rdb.RDB
}

func NewInspector(r RedisConnOpt) *Inspector {
	return &Inspector{rdb: rdb.New(r.MakeClient())}
}

func (i *Inspector) Close() error {
	return i.rdb.Close()
}

func (i *Inspector) Queues() ([]string, error) {
	return i.rdb.Queues(context.Background())
}

func (i *Inspector) Stats(qname string) (*QueueStats, error) {
	s, err := i.rdb.Stats(context.Background(), qname)
	if err != nil {
		return nil, err
	}
	return &QueueStats{
		Queue:     s.Queue,
		Paused:    s.Paused,
		Pending:   s.Pending,
		Active:    s.Active,
		Scheduled: s.Scheduled,
		Retry:     s.Retry,
		Archived:  s.Archived,
		Completed: s.Completed,
	}, nil
}

func (i *Inspector) ListTasks(qname, state string, limit int) ([]*TaskInfo, error) {
	if limit <= 0 {
		limit = 50
	}
	msgs, err := i.rdb.List(context.Background(), qname, state, limit)
	if err != nil {
		return nil, err
	}
	out := make([]*TaskInfo, 0, len(msgs))
	for _, m := range msgs {
		out = append(out, toTaskInfo(m, state))
	}
	return out, nil
}

func (i *Inspector) DeleteTask(qname, id string) error {
	return i.rdb.Delete(context.Background(), qname, id)
}

func (i *Inspector) RunTask(qname, id string) error {
	return i.rdb.Run(context.Background(), qname, id)
}

func (i *Inspector) CancelTask(id string) error {
	return i.rdb.PublishCancel(context.Background(), id)
}

func (i *Inspector) PauseQueue(qname string) error {
	return i.rdb.Pause(context.Background(), qname)
}

func (i *Inspector) UnpauseQueue(qname string) error {
	return i.rdb.Unpause(context.Background(), qname)
}

func toTaskInfo(m *base.TaskMessage, state string) *TaskInfo {
	return &TaskInfo{
		ID:       m.ID,
		Queue:    m.Queue,
		Type:     m.Type,
		Payload:  m.Payload,
		State:    state,
		MaxRetry: m.MaxRetry,
		Retried:  m.Retried,
		LastErr:  m.ErrorMsg,
	}
}
