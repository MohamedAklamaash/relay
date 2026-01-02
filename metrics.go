package relay

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	tasksProcessed = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "relay",
		Name:      "tasks_processed_total",
		Help:      "Number of tasks processed successfully.",
	}, []string{"queue", "type"})

	tasksFailed = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "relay",
		Name:      "tasks_failed_total",
		Help:      "Number of task executions that returned an error.",
	}, []string{"queue", "type"})

	tasksRetried = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "relay",
		Name:      "tasks_retried_total",
		Help:      "Number of tasks scheduled for retry.",
	}, []string{"queue", "type"})

	tasksArchived = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "relay",
		Name:      "tasks_archived_total",
		Help:      "Number of tasks moved to the archive after exhausting retries.",
	}, []string{"queue", "type"})

	tasksInProgress = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "relay",
		Name:      "tasks_in_progress",
		Help:      "Number of tasks currently being processed.",
	}, []string{"queue"})

	processingDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "relay",
		Name:      "task_duration_seconds",
		Help:      "Time spent processing a task.",
		Buckets:   prometheus.DefBuckets,
	}, []string{"queue", "type"})
)

func MetricsHandler() http.Handler {
	return promhttp.Handler()
}
