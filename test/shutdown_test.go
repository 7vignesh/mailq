package integration_test

import (
	"context"
	"testing"
	"time"

	"github.com/7vignesh/mailq/internal/store"
	"github.com/7vignesh/mailq/test/fakesmtp"
)

// waitSending blocks until a worker holds the message's lease.
func (h *harness) waitSending(id string) {
	h.t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		m, _, _ := h.st.Get(context.Background(), id)
		if m.State == store.StateSending {
			return
		}
		if time.Now().After(deadline) {
			h.t.Fatalf("message %s never reached sending (state %s)", id, m.State)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

func TestShutdownLetsInFlightAttemptFinish(t *testing.T) {
	h := newHarness(t, fakesmtp.Options{SlowDelay: time.Second})
	o := h.defaultWorker("w1")
	o.client.Timeouts.DataEnd = 10 * time.Second
	o.cfg.AttemptTimeout, o.cfg.ShutdownGrace = 15*time.Second, 10*time.Second
	stop := h.startWorker(o)

	id := h.send("slow@a.test")
	h.waitSending(id)
	start := time.Now()
	stop() // returns only once the in-flight attempt is recorded

	m, attempts, _ := h.st.Get(context.Background(), id)
	if m.State != store.StateDelivered || len(attempts) != 1 {
		t.Fatalf("after shutdown: state %s, %d attempts; want delivered, 1", m.State, len(attempts))
	}
	if d := time.Since(start); d > 5*time.Second {
		t.Fatalf("shutdown took %v; should end as soon as the attempt finishes", d)
	}

	// Stopped means stopped: nothing new is claimed.
	later := h.send("accept@a.test")
	time.Sleep(100 * time.Millisecond)
	if m, _, _ := h.st.Get(context.Background(), later); m.State != store.StateQueued {
		t.Fatalf("message claimed after shutdown: %s", m.State)
	}
}

// When the grace period runs out mid-attempt, the attempt is interrupted
// and the message goes back to the queue, due immediately. If the cut
// happens after the body was sent, the server may already have the
// message: this test pins down that duplicate window instead of hiding it.
func TestShutdownGraceExpiryRequeuesAndCanDuplicate(t *testing.T) {
	h := newHarness(t, fakesmtp.Options{SlowDelay: 3 * time.Second})
	o := h.defaultWorker("w1")
	o.client.Timeouts.DataEnd = 10 * time.Second
	o.cfg.AttemptTimeout, o.cfg.ShutdownGrace = 15*time.Second, 200*time.Millisecond
	stop := h.startWorker(o)

	id := h.send("slow@a.test")
	h.waitSending(id)
	time.Sleep(300 * time.Millisecond) // let the body reach the server
	start := time.Now()
	stop()
	if d := time.Since(start); d > 2*time.Second {
		t.Fatalf("shutdown took %v; grace was 200ms", d)
	}

	m, attempts, _ := h.st.Get(context.Background(), id)
	if m.State != store.StateDeferred || len(attempts) != 1 {
		t.Fatalf("state %s, %d attempts; want deferred, 1", m.State, len(attempts))
	}
	a := attempts[0]
	if a.Reason != "interrupted" || a.Stage != "data_end" {
		t.Fatalf("attempt %+v; want interrupted at data_end", a)
	}
	if !m.NextAttemptAt.Equal(h.clk.Now()) {
		t.Fatalf("interrupted message due at %v, want now (%v)", m.NextAttemptAt, h.clk.Now())
	}

	// A new worker picks it up and delivers it. The server now holds two
	// copies: at-least-once, not exactly-once.
	o2 := h.defaultWorker("w2")
	o2.client.Timeouts.DataEnd = 10 * time.Second
	o2.cfg.AttemptTimeout = 15 * time.Second
	h.startWorker(o2)
	h.waitFor(id, 2, store.StateDelivered)
	if got := len(h.fake.Received()); got != 2 {
		t.Fatalf("fake received %d copies; expected the documented duplicate (2)", got)
	}
}
