package base

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
)

const prefix = "relay"

const AllQueues = prefix + ":queues"

const CancelChannel = prefix + ":cancel"

func queuePrefix(qname string) string {
	return prefix + ":{" + qname + "}:"
}

func TaskKeyPrefix(qname string) string {
	return queuePrefix(qname) + "t:"
}

func TaskKey(qname, id string) string {
	return TaskKeyPrefix(qname) + id
}

func PendingKey(qname string) string {
	return queuePrefix(qname) + "pending"
}

func ActiveKey(qname string) string {
	return queuePrefix(qname) + "active"
}

func LeaseKey(qname string) string {
	return queuePrefix(qname) + "lease"
}

func ScheduledKey(qname string) string {
	return queuePrefix(qname) + "scheduled"
}

func RetryKey(qname string) string {
	return queuePrefix(qname) + "retry"
}

func ArchivedKey(qname string) string {
	return queuePrefix(qname) + "archived"
}

func CompletedKey(qname string) string {
	return queuePrefix(qname) + "completed"
}

func PausedKey(qname string) string {
	return queuePrefix(qname) + "paused"
}

func UniqueKey(qname, hash string) string {
	return queuePrefix(qname) + "unique:" + hash
}

func HashUnique(qname, tasktype string, payload []byte) string {
	h := sha256.New()
	h.Write([]byte(qname))
	h.Write([]byte{0})
	h.Write([]byte(tasktype))
	h.Write([]byte{0})
	h.Write(payload)
	return hex.EncodeToString(h.Sum(nil))
}

type TaskMessage struct {
	ID           string `json:"id"`
	Type         string `json:"type"`
	Payload      []byte `json:"payload"`
	Queue        string `json:"queue"`
	MaxRetry     int    `json:"max_retry"`
	Retried      int    `json:"retried"`
	Timeout      int64  `json:"timeout"`
	Deadline     int64  `json:"deadline"`
	UniqueHash   string `json:"unique_hash"`
	Retention    int64  `json:"retention"`
	ErrorMsg     string `json:"error_msg"`
	LastFailedAt int64  `json:"last_failed_at"`
	CompletedAt  int64  `json:"completed_at"`
	EnqueuedAt   int64  `json:"enqueued_at"`
}

func EncodeMessage(m *TaskMessage) ([]byte, error) {
	return json.Marshal(m)
}

func DecodeMessage(b []byte) (*TaskMessage, error) {
	var m TaskMessage
	if err := json.Unmarshal(b, &m); err != nil {
		return nil, err
	}
	return &m, nil
}
