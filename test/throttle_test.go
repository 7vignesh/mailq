package integration_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/7vignesh/mailq/internal/store"
	"github.com/7vignesh/mailq/test/fakesmtp"
)

func TestThrottleReplySlowsDomain(t *testing.T) {
	h := newHarness(t, fakesmtp.Options{})
	o := h.defaultWorker("w1")
	o.throttle.InitialRate, o.throttle.MaxRate = 100, 100
	h.startWorker(o)

	id := h.send("throttle@slow.test")
	h.waitFor(id, 1, store.StateDeferred)
	st, ok := o.limiter.Stats("slow.test")
	if !ok || st.Rate != 50 || st.Decreases != 1 {
		t.Fatalf("after a 421: %+v, want rate 50 after one decrease", st)
	}

	// A greylist (4.7.1 with greylist wording) is not a load signal.
	id = h.send("greylist@grey.test")
	h.waitFor(id, 1, store.StateDeferred)
	if st, _ := o.limiter.Stats("grey.test"); st.Rate != 100 || st.Decreases != 0 {
		t.Fatalf("greylisting changed the rate: %+v", st)
	}
}

// A domain with no budget left must not hold up other domains: the worker
// skips its messages at claim time instead of blocking on them.
func TestThrottledDomainDoesNotBlockOthers(t *testing.T) {
	h := newHarness(t, fakesmtp.Options{})
	o := h.defaultWorker("w1")
	o.throttle.InitialRate, o.throttle.MinRate, o.throttle.Burst = 6, 1, 1 // one token, then one per 10s
	o.throttle.MaxRate = 6

	var slow, fast []string
	for i := range 5 {
		slow = append(slow, h.send(fmt.Sprintf("s%d@slow.test", i)))
	}
	for i := range 5 {
		fast = append(fast, h.send(fmt.Sprintf("f%d@fast.test", i)))
	}
	h.startWorker(o)

	// fast.test gets a token per message only as time passes, so step the
	// fake clock in 10s ticks. Each tick gives every domain one token.
	deliveredFast := func() int {
		n := 0
		for _, id := range fast {
			if m, _, _ := h.st.Get(context.Background(), id); m.State == store.StateDelivered {
				n++
			}
		}
		return n
	}
	for tick := 0; deliveredFast() < len(fast) && tick < 20; tick++ {
		time.Sleep(50 * time.Millisecond)
		h.clk.Advance(10 * time.Second)
	}
	if n := deliveredFast(); n != len(fast) {
		t.Fatalf("only %d/%d fast.test messages delivered", n, len(fast))
	}

	// slow.test was held to the same rate, not starved and not exceeded,
	// and handing messages back never counted as an attempt.
	for _, id := range slow {
		m, attempts, _ := h.st.Get(context.Background(), id)
		if len(attempts) > 1 {
			t.Fatalf("throttled message %s has %d attempts", id, len(attempts))
		}
		if m.State != store.StateDelivered && m.AttemptCount != 0 {
			t.Fatalf("message %s state %s attempt_count %d", id, m.State, m.AttemptCount)
		}
	}
}
