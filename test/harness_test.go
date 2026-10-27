// Integration tests: the whole pipeline (HTTP API, Postgres queue, worker,
// real SMTP client) against the fake SMTP server. Time is a fake clock, so
// retries and lease expiry happen when a test advances it.
package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/7vignesh/mailq/internal/api"
	"github.com/7vignesh/mailq/internal/backoff"
	"github.com/7vignesh/mailq/internal/clock"
	"github.com/7vignesh/mailq/internal/smtp"
	"github.com/7vignesh/mailq/internal/store"
	"github.com/7vignesh/mailq/internal/testdb"
	"github.com/7vignesh/mailq/internal/worker"
	"github.com/7vignesh/mailq/test/fakesmtp"
)

var t0 = time.Date(2026, 3, 1, 9, 0, 0, 0, time.UTC)

type harness struct {
	t    *testing.T
	st   *store.Store
	fake *fakesmtp.Server
	clk  *clock.Fake
	api  *httptest.Server
	log  *slog.Logger
}

type workerOpts struct {
	cfg     worker.Config
	backoff backoff.Policy
	client  *smtp.Client
}

func newHarness(t *testing.T, fakeOpts fakesmtp.Options) *harness {
	t.Helper()
	st := testdb.New(t)
	if fakeOpts.TLS == nil {
		cfg, err := fakesmtp.SelfSignedTLS()
		if err != nil {
			t.Fatal(err)
		}
		fakeOpts.TLS = cfg
	}
	fake, err := fakesmtp.Start(fakeOpts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { fake.Close() })

	h := &harness{t: t, st: st, fake: fake, clk: clock.NewFake(t0),
		log: slog.New(slog.NewTextHandler(testWriter{t}, &slog.HandlerOptions{Level: slog.LevelWarn}))}
	h.api = httptest.NewServer(api.New(st, h.clk, h.log).Routes())
	t.Cleanup(h.api.Close)
	return h
}

func (h *harness) defaultWorker(id string) workerOpts {
	resolver, dialer := smtp.StaticRoute(h.fake.Addr())
	return workerOpts{
		cfg: worker.Config{
			ID: id, Concurrency: 4, PollInterval: 5 * time.Millisecond,
			AttemptTimeout: 3 * time.Second, Lease: 10 * time.Minute, ReapInterval: 20 * time.Millisecond,
			HeloName: "mailq.test",
		},
		backoff: backoff.Default(),
		client: &smtp.Client{
			HeloName: "mailq.test", Resolver: resolver, Dialer: dialer, Port: "25", StartTLS: true,
			Timeouts: smtp.Timeouts{Dial: time.Second, Greeting: time.Second, Command: 300 * time.Millisecond,
				DataWrite: time.Second, DataEnd: time.Second},
		},
	}
}

// startWorker runs a worker until the test ends and returns a function that
// stops it and waits for it to exit.
func (h *harness) startWorker(o workerOpts) (stop func()) {
	h.t.Helper()
	w, err := worker.New(o.cfg, h.st, o.client, o.backoff, h.clk, h.log)
	if err != nil {
		h.t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); w.Run(ctx) }()
	var once sync.Once
	stop = func() { once.Do(func() { cancel(); <-done }) }
	h.t.Cleanup(stop)
	return stop
}

func (h *harness) post(body map[string]string) (int, map[string]any) {
	h.t.Helper()
	b, _ := json.Marshal(body)
	resp, err := http.Post(h.api.URL+"/messages", "application/json", bytes.NewReader(b))
	if err != nil {
		h.t.Fatal(err)
	}
	defer resp.Body.Close()
	var out map[string]any
	json.NewDecoder(resp.Body).Decode(&out)
	return resp.StatusCode, out
}

// send enqueues a message through the API and returns its id.
func (h *harness) send(to string) string {
	h.t.Helper()
	code, out := h.post(map[string]string{
		"from": "sender@mailq.test", "to": to, "subject": "test", "body": "hello",
		"idempotency_key": fmt.Sprintf("%s-%d", to, time.Now().UnixNano()),
	})
	if code != http.StatusAccepted {
		h.t.Fatalf("POST /messages = %d %v", code, out)
	}
	return out["id"].(string)
}

// waitFor polls until the message is in one of states with at least
// minAttempts attempts recorded.
func (h *harness) waitFor(id string, minAttempts int, states ...store.State) (store.Message, []store.Attempt) {
	h.t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		m, attempts, err := h.st.Get(context.Background(), id)
		if err != nil {
			h.t.Fatal(err)
		}
		if slices.Contains(states, m.State) && len(attempts) >= minAttempts {
			return m, attempts
		}
		if time.Now().After(deadline) {
			h.t.Fatalf("message %s: state %s with %d attempts, want %v with >= %d", id, m.State, len(attempts), states, minAttempts)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

type debugView struct {
	Message  store.Message   `json:"message"`
	Attempts []store.Attempt `json:"attempts"`
}

func (h *harness) getDebug(id string) debugView {
	h.t.Helper()
	resp, err := http.Get(h.api.URL + "/messages/" + id)
	if err != nil {
		h.t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		h.t.Fatalf("GET /messages/%s = %d %s", id, resp.StatusCode, b)
	}
	var v debugView
	if err := json.NewDecoder(resp.Body).Decode(&v); err != nil {
		h.t.Fatal(err)
	}
	return v
}

type testWriter struct{ t *testing.T }

func (w testWriter) Write(p []byte) (int, error) {
	w.t.Helper()
	w.t.Log(string(bytes.TrimRight(p, "\n")))
	return len(p), nil
}
