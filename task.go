package relay

import "time"

type Task struct {
	typename string
	payload  []byte
	opts     []Option
}

func NewTask(typename string, payload []byte, opts ...Option) *Task {
	return &Task{typename: typename, payload: payload, opts: opts}
}

func (t *Task) Type() string {
	return t.typename
}

func (t *Task) Payload() []byte {
	return t.payload
}

type TaskInfo struct {
	ID            string
	Queue         string
	Type          string
	Payload       []byte
	State         string
	MaxRetry      int
	Retried       int
	LastErr       string
	NextProcessAt time.Time
}
