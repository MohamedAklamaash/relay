package rdb

import (
	"context"
	"errors"
	"time"

	"github.com/MohamedAklamaash/relay/internal/base"
	"github.com/redis/go-redis/v9"
)

var (
	ErrNoTask         = errors.New("no task available")
	ErrTaskIDConflict = errors.New("task id already exists")
	ErrDuplicateTask  = errors.New("duplicate task")
	ErrTaskNotFound   = errors.New("task not found")
)

const forwardBatchSize = 100

type RDB struct {
	client redis.UniversalClient
}

func New(c redis.UniversalClient) *RDB {
	return &RDB{client: c}
}

func (r *RDB) Client() redis.UniversalClient {
	return r.client
}

func (r *RDB) Ping(ctx context.Context) error {
	return r.client.Ping(ctx).Err()
}

func (r *RDB) Close() error {
	return r.client.Close()
}

func (r *RDB) Enqueue(ctx context.Context, msg *base.TaskMessage, processAt time.Time, uniqueTTL time.Duration) error {
	data, err := base.EncodeMessage(msg)
	if err != nil {
		return err
	}
	uniqueKey := ""
	ttlSeconds := 0
	if uniqueTTL > 0 && msg.UniqueHash != "" {
		uniqueKey = base.UniqueKey(msg.Queue, msg.UniqueHash)
		ttlSeconds = int(uniqueTTL.Seconds())
		if ttlSeconds < 1 {
			ttlSeconds = 1
		}
	}

	mode := "now"
	dest := base.PendingKey(msg.Queue)
	score := int64(0)
	if processAt.After(time.Now()) {
		mode = "at"
		dest = base.ScheduledKey(msg.Queue)
		score = processAt.Unix()
	}

	keys := []string{base.TaskKey(msg.Queue, msg.ID), dest, base.AllQueues, uniqueKey}
	argv := []any{msg.ID, data, msg.Queue, ttlSeconds, mode, score}

	res, err := enqueueScript.Run(ctx, r.client, keys, argv...).Int()
	if err != nil {
		return err
	}
	switch res {
	case -1:
		return ErrTaskIDConflict
	case 0:
		return ErrDuplicateTask
	default:
		return nil
	}
}

func (r *RDB) Dequeue(ctx context.Context, qname string, leaseExpiry time.Time) (*base.TaskMessage, error) {
	keys := []string{base.PendingKey(qname), base.ActiveKey(qname), base.LeaseKey(qname)}
	argv := []any{leaseExpiry.Unix(), base.TaskKeyPrefix(qname)}

	res, err := dequeueScript.Run(ctx, r.client, keys, argv...).Slice()
	if errors.Is(err, redis.Nil) {
		return nil, ErrNoTask
	}
	if err != nil {
		return nil, err
	}
	if len(res) < 2 || res[1] == nil {
		return nil, ErrNoTask
	}
	data, ok := res[1].(string)
	if !ok {
		return nil, ErrNoTask
	}
	return base.DecodeMessage([]byte(data))
}

func (r *RDB) Done(ctx context.Context, msg *base.TaskMessage, retention time.Duration) error {
	data, err := base.EncodeMessage(msg)
	if err != nil {
		return err
	}
	uniqueKey := ""
	if msg.UniqueHash != "" {
		uniqueKey = base.UniqueKey(msg.Queue, msg.UniqueHash)
	}
	keys := []string{
		base.ActiveKey(msg.Queue),
		base.LeaseKey(msg.Queue),
		base.TaskKey(msg.Queue, msg.ID),
		uniqueKey,
		base.CompletedKey(msg.Queue),
	}
	argv := []any{msg.ID, int(retention.Seconds()), data, time.Now().Unix()}
	return doneScript.Run(ctx, r.client, keys, argv...).Err()
}

func (r *RDB) Retry(ctx context.Context, msg *base.TaskMessage, processAt time.Time) error {
	data, err := base.EncodeMessage(msg)
	if err != nil {
		return err
	}
	keys := []string{
		base.ActiveKey(msg.Queue),
		base.LeaseKey(msg.Queue),
		base.RetryKey(msg.Queue),
		base.TaskKey(msg.Queue, msg.ID),
	}
	argv := []any{msg.ID, data, processAt.Unix()}
	return retryScript.Run(ctx, r.client, keys, argv...).Err()
}

func (r *RDB) Archive(ctx context.Context, msg *base.TaskMessage) error {
	data, err := base.EncodeMessage(msg)
	if err != nil {
		return err
	}
	uniqueKey := ""
	if msg.UniqueHash != "" {
		uniqueKey = base.UniqueKey(msg.Queue, msg.UniqueHash)
	}
	keys := []string{
		base.ActiveKey(msg.Queue),
		base.LeaseKey(msg.Queue),
		base.ArchivedKey(msg.Queue),
		base.TaskKey(msg.Queue, msg.ID),
		uniqueKey,
	}
	argv := []any{msg.ID, data, time.Now().Unix()}
	return archiveScript.Run(ctx, r.client, keys, argv...).Err()
}

func (r *RDB) ForwardDue(ctx context.Context, qname string) (int, error) {
	now := time.Now().Unix()
	pending := base.PendingKey(qname)
	total := 0
	for _, src := range []string{base.ScheduledKey(qname), base.RetryKey(qname)} {
		n, err := forwardScript.Run(ctx, r.client, []string{src, pending}, now, forwardBatchSize).Int()
		if err != nil {
			return total, err
		}
		total += n
	}
	return total, nil
}

func (r *RDB) Recover(ctx context.Context, qname string) (int, error) {
	keys := []string{base.LeaseKey(qname), base.ActiveKey(qname), base.PendingKey(qname)}
	return recoverScript.Run(ctx, r.client, keys, time.Now().Unix(), forwardBatchSize).Int()
}

func (r *RDB) ExtendLease(ctx context.Context, qname, id string, expiry time.Time) error {
	keys := []string{base.LeaseKey(qname)}
	return heartbeatScript.Run(ctx, r.client, keys, expiry.Unix(), id).Err()
}

func (r *RDB) Queues(ctx context.Context) ([]string, error) {
	return r.client.SMembers(ctx, base.AllQueues).Result()
}

func (r *RDB) PublishCancel(ctx context.Context, id string) error {
	return r.client.Publish(ctx, base.CancelChannel, id).Err()
}

func (r *RDB) SubscribeCancel(ctx context.Context) (<-chan string, func() error) {
	pubsub := r.client.Subscribe(ctx, base.CancelChannel)
	out := make(chan string)
	go func() {
		defer close(out)
		for msg := range pubsub.Channel() {
			out <- msg.Payload
		}
	}()
	return out, pubsub.Close
}

func (r *RDB) Pause(ctx context.Context, qname string) error {
	return r.client.Set(ctx, base.PausedKey(qname), "1", 0).Err()
}

func (r *RDB) Unpause(ctx context.Context, qname string) error {
	return r.client.Del(ctx, base.PausedKey(qname)).Err()
}

func (r *RDB) IsPaused(ctx context.Context, qname string) (bool, error) {
	n, err := r.client.Exists(ctx, base.PausedKey(qname)).Result()
	return n > 0, err
}
