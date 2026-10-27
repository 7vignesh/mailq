// Package metrics defines mailq's Prometheus metrics.
//
// Domain labels are capped: only domains on an allowlist (big mailbox
// providers by default) get their own label value, everything else is
// "other". Recipient domains are unbounded, and every distinct label value
// is a new time series held in memory by both mailq and Prometheus, so an
// uncapped domain label grows with the customer base. Per-domain detail for
// the long tail lives in logs and the attempts table instead.
//
// A static allowlist was chosen over "top N by volume" because top-N
// ranking changes over time: a domain would move between its own series
// and "other", which makes rates over time misleading, and series that
// already exist are not freed until restart anyway.
package metrics

import (
	"strings"

	"github.com/prometheus/client_golang/prometheus"
)

const Other = "other"

// DefaultDomains are large consumer mailbox providers. Unverified: chosen
// by general knowledge of market share, not from any traffic data.
var DefaultDomains = []string{
	"gmail.com", "googlemail.com", "outlook.com", "hotmail.com", "live.com", "msn.com",
	"yahoo.com", "aol.com", "icloud.com", "me.com", "proton.me", "protonmail.com",
	"gmx.com", "gmx.de", "web.de", "mail.ru", "yandex.ru", "qq.com", "163.com", "comcast.net",
}

type DomainLabeler struct{ allowed map[string]bool }

func NewDomainLabeler(domains []string) DomainLabeler {
	l := DomainLabeler{allowed: map[string]bool{}}
	for _, d := range domains {
		if d = strings.ToLower(strings.TrimSpace(d)); d != "" {
			l.allowed[d] = true
		}
	}
	return l
}

func (l DomainLabeler) Label(domain string) string {
	if l.allowed[domain] {
		return domain
	}
	return Other
}

// Buckets for one SMTP attempt, connect to final reply. Spans a fast local
// 250 (milliseconds) to the 10-minute end-of-DATA timeout.
var AttemptBuckets = []float64{0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120, 300, 600, 900}

// Buckets for enqueue to final state. Spans first-try delivery (seconds)
// through retries over hours, to the 72h max age and a little past it.
var EndToEndBuckets = []float64{1, 5, 15, 60, 300, 900, 1800, 3600, 7200, 14400, 28800,
	43200, 86400, 172800, 259200, 345600}

type Worker struct {
	Domains           DomainLabeler
	Attempts          *prometheus.CounterVec   // domain, outcome, reason
	Finished          *prometheus.CounterVec   // domain, state
	Retries           *prometheus.CounterVec   // domain, reason
	AttemptDuration   *prometheus.HistogramVec // outcome
	TimeToFinal       *prometheus.HistogramVec // state
	AttemptsToFinal   *prometheus.HistogramVec // state
	ThrottleDecreases *prometheus.CounterVec   // domain
	ThrottleReleases  *prometheus.CounterVec   // domain
	Reaped            prometheus.Counter
	LeaseLost         prometheus.Counter
	InFlight          prometheus.Gauge
}

func NewWorker(reg prometheus.Registerer, domains []string) *Worker {
	m := &Worker{
		Domains: NewDomainLabeler(domains),
		Attempts: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "mailq_attempts_total", Help: "Delivery attempts by classified outcome and reason.",
		}, []string{"domain", "outcome", "reason"}),
		Finished: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "mailq_messages_finished_total", Help: "Messages reaching a terminal state.",
		}, []string{"domain", "state"}),
		Retries: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "mailq_retries_scheduled_total", Help: "Transient failures that scheduled a retry.",
		}, []string{"domain", "reason"}),
		AttemptDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "mailq_attempt_duration_seconds", Help: "Duration of one SMTP attempt, all MX hosts tried.",
			Buckets: AttemptBuckets,
		}, []string{"outcome"}),
		TimeToFinal: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "mailq_time_to_final_seconds", Help: "Time from enqueue to a terminal state, retries included.",
			Buckets: EndToEndBuckets,
		}, []string{"state"}),
		AttemptsToFinal: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "mailq_attempts_per_message", Help: "Attempts a message needed to reach a terminal state.",
			Buckets: []float64{1, 2, 3, 5, 8, 13, 21, 34},
		}, []string{"state"}),
		ThrottleDecreases: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "mailq_throttle_decreases_total", Help: "Times a domain's send rate was cut after a throttle signal.",
		}, []string{"domain"}),
		ThrottleReleases: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "mailq_throttle_releases_total", Help: "Claimed messages handed back because the domain had no budget.",
		}, []string{"domain"}),
		Reaped: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "mailq_leases_reaped_total", Help: "Expired leases returned to the queue (worker died or stalled).",
		}),
		LeaseLost: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "mailq_lease_lost_total", Help: "Attempt results not applied because the lease had been reaped.",
		}),
		InFlight: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "mailq_attempts_in_flight", Help: "SMTP attempts currently running in this process.",
		}),
	}
	reg.MustRegister(m.Attempts, m.Finished, m.Retries, m.AttemptDuration, m.TimeToFinal,
		m.AttemptsToFinal, m.ThrottleDecreases, m.ThrottleReleases, m.Reaped, m.LeaseLost, m.InFlight)
	return m
}
