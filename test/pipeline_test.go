package integration_test

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/7vignesh/mailq/internal/store"
	"github.com/7vignesh/mailq/test/fakesmtp"
)

func TestPipelineOutcomes(t *testing.T) {
	h := newHarness(t, fakesmtp.Options{})
	h.startWorker(h.defaultWorker("w1"))

	tests := []struct {
		rcpt     string
		state    store.State
		code     int
		enhanced string
		outcome  string
		reason   string
		stage    string
	}{
		{"accept@a.test", store.StateDelivered, 250, "2.0.0", "delivered", "", "data_end"},
		{"throttle@a.test", store.StateDeferred, 421, "4.7.0", "transient", "throttled", "rcpt"},
		{"busy@a.test", store.StateDeferred, 450, "4.2.1", "transient", "mailbox_busy", "rcpt"},
		{"greylist@a.test", store.StateDeferred, 451, "4.7.1", "transient", "greylisted", "rcpt"},
		{"full@a.test", store.StateDeferred, 452, "4.2.2", "transient", "mailbox_full", "rcpt"},
		{"nouser@a.test", store.StateBounced, 550, "5.1.1", "permanent", "no_such_user", "rcpt"},
		{"policy@a.test", store.StateBounced, 554, "5.7.1", "permanent", "policy_blocked", "data_end"},
		{"hang@a.test", store.StateDeferred, 0, "", "transient", "timeout", "rcpt"},
		{"drop@a.test", store.StateDeferred, 0, "", "transient", "connection_failure", "data_end"},
	}
	ids := map[string]string{}
	for _, tt := range tests {
		ids[tt.rcpt] = h.send(tt.rcpt)
	}
	for _, tt := range tests {
		t.Run(tt.rcpt, func(t *testing.T) {
			m, attempts := h.waitFor(ids[tt.rcpt], 1, tt.state)
			if len(attempts) != 1 || m.AttemptCount != 1 {
				t.Fatalf("attempts = %d (count %d), want 1", len(attempts), m.AttemptCount)
			}
			a := attempts[0]
			if a.SMTPCode != tt.code || a.EnhancedCode != tt.enhanced || a.Outcome != tt.outcome ||
				a.Reason != tt.reason || a.Stage != tt.stage {
				t.Fatalf("attempt = %+v", a)
			}
			if a.MXHost == "" || a.WorkerID != "w1" {
				t.Fatalf("attempt missing mx_host/worker_id: %+v", a)
			}
			if tt.state == store.StateDeferred && !m.NextAttemptAt.After(h.clk.Now()) {
				t.Fatalf("deferred without a future next_attempt_at: %v", m.NextAttemptAt)
			}
		})
	}
	if got := h.fake.Received(); len(got) != 1 || !got[0].TLS {
		t.Fatalf("fake received %d messages (want 1, over TLS): %+v", len(got), got)
	}
}

func TestRetryThenDeliverAndDebugEndpoint(t *testing.T) {
	h := newHarness(t, fakesmtp.Options{})
	h.fake.Script("bob@a.test", fakesmtp.Greylist, fakesmtp.Accept)
	h.startWorker(h.defaultWorker("w1"))

	id := h.send("bob@a.test")
	m, _ := h.waitFor(id, 1, store.StateDeferred)

	// Not due yet: nothing happens.
	h.clk.Set(m.NextAttemptAt.Add(-time.Second))
	time.Sleep(100 * time.Millisecond)
	if m2, _, _ := h.st.Get(context.Background(), id); m2.State != store.StateDeferred {
		t.Fatalf("retried before next_attempt_at: %s", m2.State)
	}

	h.clk.Set(m.NextAttemptAt)
	h.waitFor(id, 2, store.StateDelivered)

	v := h.getDebug(id)
	if v.Message.State != store.StateDelivered || len(v.Attempts) != 2 {
		t.Fatalf("debug view: %+v", v)
	}
	first, second := v.Attempts[0], v.Attempts[1]
	if first.Reason != "greylisted" || first.SMTPCode != 451 || first.AttemptNumber != 1 {
		t.Fatalf("first attempt %+v", first)
	}
	if second.Outcome != "delivered" || second.SMTPCode != 250 || second.AttemptNumber != 2 {
		t.Fatalf("second attempt %+v", second)
	}
}

func TestGiveUpAfterMaxAge(t *testing.T) {
	h := newHarness(t, fakesmtp.Options{})
	o := h.defaultWorker("w1")
	o.backoff.MaxAge = 2 * time.Hour
	h.startWorker(o)

	id := h.send("busy@a.test")
	h.waitFor(id, 1, store.StateDeferred)
	h.clk.Advance(5 * time.Hour) // past the 2h deadline the retry was clamped to
	m, attempts := h.waitFor(id, 2, store.StateFailed)
	if m.AttemptCount != 2 || attempts[1].Reason != "mailbox_busy" {
		t.Fatalf("failed message: count=%d attempts=%+v", m.AttemptCount, attempts)
	}
}

func TestConcurrentDuplicatePostsSendOnce(t *testing.T) {
	h := newHarness(t, fakesmtp.Options{})
	body := map[string]string{"from": "sender@mailq.test", "to": "once@a.test",
		"subject": "s", "body": "b", "idempotency_key": "order-42"}

	const n = 25
	codes := make([]int, n)
	ids := make([]string, n)
	var wg sync.WaitGroup
	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			code, out := h.post(body)
			codes[i] = code
			ids[i], _ = out["id"].(string)
		}()
	}
	wg.Wait()

	accepted := 0
	for i := range n {
		if codes[i] == http.StatusAccepted {
			accepted++
		} else if codes[i] != http.StatusOK {
			t.Fatalf("POST %d = %d", i, codes[i])
		}
		if ids[i] != ids[0] {
			t.Fatalf("different ids for one idempotency key: %s vs %s", ids[0], ids[i])
		}
	}
	if accepted != 1 {
		t.Fatalf("%d requests created a message, want 1", accepted)
	}

	body["body"] = "changed"
	if code, _ := h.post(body); code != http.StatusConflict {
		t.Fatalf("key reuse with new payload = %d, want 409", code)
	}

	h.startWorker(h.defaultWorker("w1"))
	h.waitFor(ids[0], 1, store.StateDelivered)
	time.Sleep(100 * time.Millisecond)
	if got := len(h.fake.Received()); got != 1 {
		t.Fatalf("fake received %d messages, want exactly 1", got)
	}
}

func TestTwoWorkersDeliverEachMessageOnce(t *testing.T) {
	h := newHarness(t, fakesmtp.Options{})
	const n = 60
	ids := make([]string, n)
	for i := range n {
		ids[i] = h.send(fmt.Sprintf("user%d@a.test", i))
	}
	h.startWorker(h.defaultWorker("w1"))
	h.startWorker(h.defaultWorker("w2"))

	byWorker := map[string]int{}
	for _, id := range ids {
		_, attempts := h.waitFor(id, 1, store.StateDelivered)
		if len(attempts) != 1 {
			t.Fatalf("message %s has %d attempts, want 1", id, len(attempts))
		}
		byWorker[attempts[0].WorkerID]++
	}
	seen := map[string]bool{}
	for _, r := range h.fake.Received() {
		if seen[r.To] {
			t.Fatalf("%s delivered twice", r.To)
		}
		seen[r.To] = true
	}
	if len(seen) != n {
		t.Fatalf("fake received %d distinct recipients, want %d", len(seen), n)
	}
	t.Logf("deliveries per worker: %v", byWorker)
	if len(byWorker) != 2 {
		t.Fatalf("expected both workers to deliver, got %v", byWorker)
	}
}

// A worker that claims a message and dies leaves it in 'sending'. Once the
// lease expires, the reaper returns it to the queue and a live worker
// delivers it. The history shows both the lost attempt and the delivery.
func TestCrashedWorkerLeaseIsReaped(t *testing.T) {
	h := newHarness(t, fakesmtp.Options{})
	id := h.send("accept@a.test")
	claimed, err := h.st.Claim(context.Background(), "dead-worker", h.clk.Now())
	if err != nil || claimed == nil || claimed.ID != id {
		t.Fatalf("claim: %v %v", claimed, err)
	}

	o := h.defaultWorker("w1")
	h.startWorker(o)
	time.Sleep(100 * time.Millisecond)
	if m, _, _ := h.st.Get(context.Background(), id); m.State != store.StateSending {
		t.Fatalf("lease broken early: %s", m.State)
	}

	h.clk.Advance(o.cfg.Lease + time.Second)
	_, attempts := h.waitFor(id, 2, store.StateDelivered)
	if attempts[0].WorkerID != "dead-worker" || attempts[0].Reason != store.ReapReason {
		t.Fatalf("first attempt should be the reaped lease: %+v", attempts[0])
	}
	if attempts[1].WorkerID != "w1" || attempts[1].Outcome != "delivered" || attempts[1].AttemptNumber != 2 {
		t.Fatalf("second attempt %+v", attempts[1])
	}
}
