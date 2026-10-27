package integration_test

import (
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/7vignesh/mailq/internal/store"
	"github.com/7vignesh/mailq/test/fakesmtp"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestMetrics(t *testing.T) {
	h := newHarness(t, fakesmtp.Options{})
	o := h.defaultWorker("w1")
	o.domains = []string{"big.test"} // the only domain with its own label
	h.startWorker(o)

	h.waitFor(h.send("accept@big.test"), 1, store.StateDelivered)
	h.waitFor(h.send("nouser@small1.test"), 1, store.StateBounced)
	h.waitFor(h.send("nouser@small2.test"), 1, store.StateBounced)
	grey := h.send("greylist@big.test")
	h.waitFor(grey, 1, store.StateDeferred)
	h.waitFor(h.send("throttle@big.test"), 1, store.StateDeferred)

	m := o.metrics
	checks := []struct {
		name string
		got  float64
		want float64
	}{
		{"delivered big.test", testutil.ToFloat64(m.Attempts.WithLabelValues("big.test", "delivered", "")), 1},
		{"long tail folded into other", testutil.ToFloat64(m.Attempts.WithLabelValues("other", "permanent", "no_such_user")), 2},
		{"bounced finished", testutil.ToFloat64(m.Finished.WithLabelValues("other", "bounced")), 2},
		{"greylist retry", testutil.ToFloat64(m.Retries.WithLabelValues("big.test", "greylisted")), 1},
		{"throttle decrease", testutil.ToFloat64(m.ThrottleDecreases.WithLabelValues("big.test")), 1},
		{"nothing in flight", testutil.ToFloat64(m.InFlight), 0},
	}
	for _, c := range checks {
		if c.got != c.want {
			t.Errorf("%s = %v, want %v", c.name, c.got, c.want)
		}
	}
	// Two domains in the long tail, one label value. 'other' plus the
	// allowlist is the whole cardinality, however many domains we send to.
	if n := testutil.CollectAndCount(m.Attempts); n != 4 {
		t.Errorf("attempts series = %d, want 4", n)
	}
	if n := testutil.CollectAndCount(m.TimeToFinal); n != 2 {
		t.Errorf("time-to-final series = %d (delivered, bounced), want 2", n)
	}

	h.clk.Advance(time.Hour)
	h.waitFor(grey, 2, store.StateDeferred) // greylist repeats: second attempt
	if got := testutil.ToFloat64(m.Retries.WithLabelValues("big.test", "greylisted")); got != 2 {
		t.Errorf("greylist retries after second attempt = %v, want 2", got)
	}

	// The API's /metrics reports queue depth from Postgres and ingest counts.
	resp, err := http.Get(h.api.URL + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	for _, want := range []string{
		`mailq_queue_depth{state="delivered"} 1`,
		`mailq_queue_depth{state="bounced"} 2`,
		`mailq_queue_depth{state="deferred"} 2`,
		`mailq_ingest_total{result="created"} 5`,
	} {
		if !strings.Contains(string(body), want) {
			t.Errorf("/metrics missing %q", want)
		}
	}
}
