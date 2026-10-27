package metrics

import (
	"context"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

type API struct {
	Ingest *prometheus.CounterVec // result: created, duplicate, conflict, invalid, error
}

func NewAPI(reg prometheus.Registerer) *API {
	m := &API{Ingest: prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "mailq_ingest_total", Help: "POST /messages requests by result.",
	}, []string{"result"})}
	reg.MustRegister(m.Ingest)
	return m
}

// StateCounter is the part of the store the queue depth collector needs.
type StateCounter interface {
	CountByStateString(ctx context.Context) (map[string]int, error)
}

// QueueDepth reports messages per state, queried from Postgres at scrape
// time. It lives in the API process so the number is reported once, not
// once per worker. Terminal states are included: they show volume, and the
// gauge for queued+deferred+sending is the actual backlog.
type QueueDepth struct {
	store StateCounter
	desc  *prometheus.Desc
}

func NewQueueDepth(s StateCounter) *QueueDepth {
	return &QueueDepth{store: s, desc: prometheus.NewDesc("mailq_queue_depth",
		"Messages by state, read from Postgres at scrape time.", []string{"state"}, nil)}
}

var allStates = []string{"queued", "sending", "deferred", "delivered", "bounced", "failed"}

func (q *QueueDepth) Describe(ch chan<- *prometheus.Desc) { ch <- q.desc }

func (q *QueueDepth) Collect(ch chan<- prometheus.Metric) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	counts, err := q.store.CountByStateString(ctx)
	if err != nil {
		ch <- prometheus.NewInvalidMetric(q.desc, err)
		return
	}
	for _, s := range allStates {
		ch <- prometheus.MustNewConstMetric(q.desc, prometheus.GaugeValue, float64(counts[s]), s)
	}
}
