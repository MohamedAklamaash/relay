package relay

import "time"

const (
	defaultQueue    = "default"
	defaultMaxRetry = 25
	defaultTimeout  = 30 * time.Minute
)

type Option interface {
	apply(*taskOptions)
}

type taskOptions struct {
	queue     string
	taskID    string
	maxRetry  int
	timeout   time.Duration
	deadline  time.Time
	processAt time.Time
	uniqueTTL time.Duration
	retention time.Duration
}

func defaultOptions() taskOptions {
	return taskOptions{
		queue:     defaultQueue,
		maxRetry:  defaultMaxRetry,
		timeout:   defaultTimeout,
		processAt: time.Now(),
	}
}

func composeOptions(base taskOptions, opts ...Option) taskOptions {
	for _, o := range opts {
		o.apply(&base)
	}
	return base
}

type optionFunc func(*taskOptions)

func (f optionFunc) apply(o *taskOptions) { f(o) }

func Queue(name string) Option {
	return optionFunc(func(o *taskOptions) { o.queue = name })
}

func TaskID(id string) Option {
	return optionFunc(func(o *taskOptions) { o.taskID = id })
}

func MaxRetry(n int) Option {
	if n < 0 {
		n = 0
	}
	return optionFunc(func(o *taskOptions) { o.maxRetry = n })
}

func Timeout(d time.Duration) Option {
	return optionFunc(func(o *taskOptions) { o.timeout = d })
}

func Deadline(t time.Time) Option {
	return optionFunc(func(o *taskOptions) { o.deadline = t })
}

func ProcessAt(t time.Time) Option {
	return optionFunc(func(o *taskOptions) { o.processAt = t })
}

func ProcessIn(d time.Duration) Option {
	return optionFunc(func(o *taskOptions) { o.processAt = time.Now().Add(d) })
}

func Unique(ttl time.Duration) Option {
	return optionFunc(func(o *taskOptions) { o.uniqueTTL = ttl })
}

func Retention(d time.Duration) Option {
	return optionFunc(func(o *taskOptions) { o.retention = d })
}
