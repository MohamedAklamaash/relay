package relay

import (
	"errors"
	"math"
	"math/rand"
	"time"
)

var SkipRetry = errors.New("relay: skip retry for the task")

type RetryDelayFunc func(n int, err error, task *Task) time.Duration

func defaultRetryDelay(n int, _ error, _ *Task) time.Duration {
	const cap = 10 * time.Minute
	backoff := time.Duration(math.Pow(2, float64(n))) * time.Second
	if backoff <= 0 || backoff > cap {
		backoff = cap
	}
	jitter := time.Duration(rand.Int63n(int64(time.Second)))
	return backoff + jitter
}
