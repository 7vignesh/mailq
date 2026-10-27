// Package worker runs the delivery loop: claim a due message, send it,
// classify the result, record the attempt, and move the message on.
package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/7vignesh/mailq/internal/backoff"
	"github.com/7vignesh/mailq/internal/classifier"
	"github.com/7vignesh/mailq/internal/clock"
	"github.com/7vignesh/mailq/internal/metrics"
	"github.com/7vignesh/mailq/internal/smtp"
	"github.com/7vignesh/mailq/internal/store"
	"github.com/7vignesh/mailq/internal/throttle"
)

type Config struct {
	ID             string        // unique per process; used as the lease owner
	Concurrency    int           // delivery goroutines
	PollInterval   time.Duration // sleep when the queue has nothing due
	AttemptTimeout time.Duration // hard deadline for one attempt, all MX hosts included
	Lease          time.Duration // a lease older than this is presumed dead and reaped
	ReapInterval   time.Duration
	ShutdownGrace  time.Duration // how long in-flight attempts may run after shutdown starts
	HeloName       string
}

func (c Config) Validate() error {
	switch {
	case c.ID == "" || c.HeloName == "":
		return errors.New("worker: ID and HeloName are required")
	case c.Concurrency < 1:
		return errors.New("worker: concurrency must be >= 1")
	case c.Lease <= c.AttemptTimeout:
		// If a lease could expire while its attempt is still running, the
		// reaper would hand a live message to a second worker: a duplicate
		// caused by configuration rather than by a crash.
		return fmt.Errorf("worker: lease (%v) must be longer than attempt timeout (%v)", c.Lease, c.AttemptTimeout)
	}
	return nil
}

type Worker struct {
	cfg     Config
	store   *store.Store
	client  *smtp.Client
	backoff backoff.Policy
	limiter *throttle.Limiter
	metrics *metrics.Worker
	clock   clock.Clock
	log     *slog.Logger
}

type Deps struct {
	Store   *store.Store
	Client  *smtp.Client
	Backoff backoff.Policy
	Limiter *throttle.Limiter
	Metrics *metrics.Worker
	Clock   clock.Clock
	Log     *slog.Logger
}

func New(cfg Config, d Deps) (*Worker, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if err := d.Backoff.Validate(); err != nil {
		return nil, err
	}
	return &Worker{cfg: cfg, store: d.Store, client: d.Client, backoff: d.Backoff, limiter: d.Limiter,
		metrics: d.Metrics, clock: d.Clock, log: d.Log.With("worker_id", cfg.ID)}, nil
}

// Run delivers until ctx is canceled, then shuts down in two phases:
//
//  1. Stop claiming immediately. In-flight attempts keep running.
//  2. After ShutdownGrace, cancel whatever is still running. Those
//     attempts are recorded as interrupted and deferred, due immediately.
//
// Run returns once every attempt has been recorded. A message is never
// left in 'sending' by a clean shutdown; only a crash (or a failed database
// write) leaves a lease for the reaper.
func (w *Worker) Run(ctx context.Context) {
	sendCtx, cancelSend := context.WithCancel(context.WithoutCancel(ctx))
	defer cancelSend()
	done := make(chan struct{})
	go func() {
		select {
		case <-done:
			return
		case <-ctx.Done():
		}
		w.log.Info("shutdown: stopped claiming, draining in-flight attempts", "grace", w.cfg.ShutdownGrace)
		t := time.NewTimer(w.cfg.ShutdownGrace)
		defer t.Stop()
		select {
		case <-done:
		case <-t.C:
			w.log.Warn("shutdown: grace period over, interrupting in-flight attempts")
			cancelSend()
		}
	}()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		w.reapLoop(ctx)
	}()
	for range w.cfg.Concurrency {
		wg.Add(1)
		go func() {
			defer wg.Done()
			w.deliverLoop(ctx, sendCtx)
		}()
	}
	wg.Wait()
	close(done)
}

// claimCtx bounds a claim without tying it to shutdown. Canceling a claim
// mid-query could commit the UPDATE while we never see the row, leaving a
// lease only the reaper can clear.
func claimCtx() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), 10*time.Second)
}

func (w *Worker) deliverLoop(ctx, sendCtx context.Context) {
	for ctx.Err() == nil {
		cctx, cancel := claimCtx()
		m, err := w.store.Claim(cctx, w.cfg.ID, w.clock.Now(), w.limiter.Blocked())
		cancel()
		if err != nil {
			w.log.Error("claim failed", "err", err)
			sleep(ctx, w.cfg.PollInterval)
			continue
		}
		if m == nil {
			sleep(ctx, w.cfg.PollInterval)
			continue
		}
		permit, wait := w.limiter.TryAcquire(m.Domain)
		if permit == nil {
			// Lost a race with another goroutine for the domain's last slot.
			// Hand the message back without counting an attempt.
			w.metrics.ThrottleReleases.WithLabelValues(w.metrics.Domains.Label(m.Domain)).Inc()
			w.release(m, wait)
			continue
		}
		w.attempt(sendCtx, m, permit)
	}
}

func (w *Worker) release(m *store.Message, wait time.Duration) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	now := w.clock.Now()
	if err := w.store.Release(ctx, w.cfg.ID, m.ID, m.AttemptCount, now.Add(wait), now); err != nil {
		w.log.Error("release failed", "message_id", m.ID, "err", err)
	}
}

// signal tells the throttle what an attempt says about the domain's load.
func signal(cls classifier.Result) throttle.Signal {
	switch {
	case cls.Outcome == classifier.Delivered:
		return throttle.Success
	case cls.Reason == classifier.Throttled:
		return throttle.Throttled
	default:
		// Bounces, greylisting and timeouts say nothing reliable about load.
		// Unverified: timeouts may well mean an overloaded server, but
		// treating them as throttle signals would also slow us down on
		// plain network trouble.
		return throttle.Neutral
	}
}

func (w *Worker) attempt(ctx context.Context, m *store.Message, permit *throttle.Permit) {
	n := m.AttemptCount + 1
	log := w.log.With("message_id", m.ID, "domain", m.Domain, "attempt", n)

	actx, cancel := context.WithTimeout(ctx, w.cfg.AttemptTimeout)
	defer cancel()
	w.metrics.InFlight.Inc()
	started, t := w.clock.Now(), time.Now() // queue clock for records, monotonic clock for latency
	res := w.client.Send(actx, smtp.Envelope{
		From: m.From,
		To:   m.To,
		Data: smtp.BuildMessage(m.ID, m.From, m.To, m.Subject, m.Body, w.cfg.HeloName, m.CreatedAt),
	})
	elapsed := time.Since(t)
	finished := w.clock.Now()
	w.metrics.InFlight.Dec()

	cls := classifier.Classify(classifier.Input{
		Code: res.Code, Enhanced: res.Enhanced, Text: res.Text,
		Stage: string(res.Stage), Transport: string(res.ErrKind),
	})
	domain := w.metrics.Domains.Label(m.Domain)
	if permit.Done(signal(cls)) {
		w.metrics.ThrottleDecreases.WithLabelValues(domain).Inc()
		st, _ := w.limiter.Stats(m.Domain)
		log.Warn("throttle signal: domain rate cut", "rate_per_min", st.Rate, "smtp_code", res.Code,
			"enhanced_code", res.Enhanced)
	}
	newState, next := w.decide(m, n, cls, finished)

	c := store.Completion{
		WorkerID: w.cfg.ID, MessageID: m.ID, AttemptNumber: n,
		StartedAt: started, FinishedAt: finished,
		MXHost: res.MXHost, SMTPCode: res.Code, EnhancedCode: res.Enhanced,
		ResponseText: res.Text, Stage: string(res.Stage), Error: errText(res.Err),
		Outcome: string(cls.Outcome), Reason: string(cls.Reason),
		NewState: newState, NextAttemptAt: next,
	}
	// Record the result even if we are shutting down: the attempt happened.
	dbctx, dbcancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer dbcancel()
	err := w.store.Complete(dbctx, c)

	log = log.With("mx_host", res.MXHost, "outcome", cls.Outcome, "reason", cls.Reason,
		"smtp_code", res.Code, "enhanced_code", res.Enhanced, "stage", res.Stage,
		"rule", cls.Rule, "state", newState, "duration_ms", elapsed.Milliseconds())
	if res.Err != nil {
		log = log.With("err", res.Err)
	}
	switch {
	case errors.Is(err, store.ErrLeaseLost):
		w.metrics.LeaseLost.Inc()
		log.Warn("lease lost before result was recorded; another worker owns the message")
		return
	case err != nil:
		// The message stays 'sending' until its lease expires and the
		// reaper retries it. If this attempt was delivered, that retry
		// is a duplicate.
		log.Error("failed to record attempt", "err", err)
		return
	case newState == store.StateDeferred:
		log.Info("attempt deferred", "next_attempt_at", next)
	default:
		log.Info("attempt finished")
	}
	w.observe(m, n, domain, cls, newState, elapsed, finished)
}

// observe updates metrics once an attempt's result is safely recorded.
func (w *Worker) observe(m *store.Message, n int, domain string, cls classifier.Result,
	st store.State, elapsed time.Duration, now time.Time) {
	w.metrics.Attempts.WithLabelValues(domain, string(cls.Outcome), string(cls.Reason)).Inc()
	w.metrics.AttemptDuration.WithLabelValues(string(cls.Outcome)).Observe(elapsed.Seconds())
	if st == store.StateDeferred {
		w.metrics.Retries.WithLabelValues(domain, string(cls.Reason)).Inc()
		return
	}
	w.metrics.Finished.WithLabelValues(domain, string(st)).Inc()
	w.metrics.TimeToFinal.WithLabelValues(string(st)).Observe(now.Sub(m.CreatedAt).Seconds())
	w.metrics.AttemptsToFinal.WithLabelValues(string(st)).Observe(float64(n))
}

// decide maps a classification to the message's next state.
func (w *Worker) decide(m *store.Message, n int, cls classifier.Result, now time.Time) (store.State, time.Time) {
	switch cls.Outcome {
	case classifier.Delivered:
		return store.StateDelivered, now
	case classifier.Permanent:
		return store.StateBounced, now
	}
	if cls.Reason == classifier.Interrupted {
		return store.StateDeferred, now // we stopped it, not the server: retry right away
	}
	d := w.backoff.Next(n, m.CreatedAt, now)
	if d.GiveUp {
		return store.StateFailed, now
	}
	return store.StateDeferred, d.At
}

func (w *Worker) reapLoop(ctx context.Context) {
	for {
		now := w.clock.Now()
		n, err := w.store.Reap(ctx, now.Add(-w.cfg.Lease), now)
		switch {
		case err != nil && ctx.Err() == nil:
			w.log.Error("reap failed", "err", err)
		case n > 0:
			w.metrics.Reaped.Add(float64(n))
			w.log.Warn("reaped expired leases", "count", n)
		}
		w.limiter.Sweep()
		if !sleep(ctx, w.cfg.ReapInterval) {
			return
		}
	}
}

// sleep waits for d or until ctx is done, and reports whether ctx is alive.
func sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

func errText(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
