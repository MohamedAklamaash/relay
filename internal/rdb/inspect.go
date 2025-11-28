package rdb

import (
	"context"

	"github.com/MohamedAklamaash/relay/internal/base"
	"github.com/redis/go-redis/v9"
)

type Stats struct {
	Queue     string
	Paused    bool
	Pending   int64
	Active    int64
	Scheduled int64
	Retry     int64
	Archived  int64
	Completed int64
}

func (r *RDB) Stats(ctx context.Context, qname string) (*Stats, error) {
	pipe := r.client.Pipeline()
	pending := pipe.LLen(ctx, base.PendingKey(qname))
	active := pipe.LLen(ctx, base.ActiveKey(qname))
	scheduled := pipe.ZCard(ctx, base.ScheduledKey(qname))
	retry := pipe.ZCard(ctx, base.RetryKey(qname))
	archived := pipe.ZCard(ctx, base.ArchivedKey(qname))
	completed := pipe.ZCard(ctx, base.CompletedKey(qname))
	paused := pipe.Exists(ctx, base.PausedKey(qname))
	if _, err := pipe.Exec(ctx); err != nil {
		return nil, err
	}
	return &Stats{
		Queue:     qname,
		Paused:    paused.Val() > 0,
		Pending:   pending.Val(),
		Active:    active.Val(),
		Scheduled: scheduled.Val(),
		Retry:     retry.Val(),
		Archived:  archived.Val(),
		Completed: completed.Val(),
	}, nil
}

func (r *RDB) listIDs(ctx context.Context, qname, state string, limit int) ([]string, error) {
	switch state {
	case "pending":
		return r.client.LRange(ctx, base.PendingKey(qname), 0, int64(limit-1)).Result()
	case "active":
		return r.client.LRange(ctx, base.ActiveKey(qname), 0, int64(limit-1)).Result()
	case "scheduled":
		return r.client.ZRange(ctx, base.ScheduledKey(qname), 0, int64(limit-1)).Result()
	case "retry":
		return r.client.ZRange(ctx, base.RetryKey(qname), 0, int64(limit-1)).Result()
	case "archived":
		return r.client.ZRange(ctx, base.ArchivedKey(qname), 0, int64(limit-1)).Result()
	case "completed":
		return r.client.ZRange(ctx, base.CompletedKey(qname), 0, int64(limit-1)).Result()
	default:
		return nil, ErrTaskNotFound
	}
}

func (r *RDB) List(ctx context.Context, qname, state string, limit int) ([]*base.TaskMessage, error) {
	ids, err := r.listIDs(ctx, qname, state, limit)
	if err != nil {
		return nil, err
	}
	out := make([]*base.TaskMessage, 0, len(ids))
	for _, id := range ids {
		msg, err := r.Get(ctx, qname, id)
		if err != nil {
			continue
		}
		out = append(out, msg)
	}
	return out, nil
}

func (r *RDB) Get(ctx context.Context, qname, id string) (*base.TaskMessage, error) {
	data, err := r.client.Get(ctx, base.TaskKey(qname, id)).Result()
	if err == redis.Nil {
		return nil, ErrTaskNotFound
	}
	if err != nil {
		return nil, err
	}
	return base.DecodeMessage([]byte(data))
}

func (r *RDB) Delete(ctx context.Context, qname, id string) error {
	pipe := r.client.Pipeline()
	pipe.LRem(ctx, base.PendingKey(qname), 0, id)
	pipe.LRem(ctx, base.ActiveKey(qname), 0, id)
	pipe.ZRem(ctx, base.ScheduledKey(qname), id)
	pipe.ZRem(ctx, base.RetryKey(qname), id)
	pipe.ZRem(ctx, base.ArchivedKey(qname), id)
	pipe.ZRem(ctx, base.CompletedKey(qname), id)
	pipe.ZRem(ctx, base.LeaseKey(qname), id)
	pipe.Del(ctx, base.TaskKey(qname, id))
	_, err := pipe.Exec(ctx)
	return err
}

func (r *RDB) Run(ctx context.Context, qname, id string) error {
	pipe := r.client.Pipeline()
	pipe.ZRem(ctx, base.ScheduledKey(qname), id)
	pipe.ZRem(ctx, base.RetryKey(qname), id)
	pipe.ZRem(ctx, base.ArchivedKey(qname), id)
	pipe.LPush(ctx, base.PendingKey(qname), id)
	_, err := pipe.Exec(ctx)
	return err
}
