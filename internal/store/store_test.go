package store_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/7vignesh/mailq/internal/store"
	"github.com/7vignesh/mailq/internal/testdb"
)

var t0 = time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)

func newMsg(key string) store.NewMessage {
	return store.NewMessage{
		IdempotencyKey: key, From: "sender@mailq.test", To: "rcpt@example.com",
		Domain: "example.com", Subject: "hi", Body: "hello",
	}
}

func TestEnqueueConcurrentDuplicatesInsertOnce(t *testing.T) {
	s := testdb.New(t)
	ctx := context.Background()

	const n = 50
	var wg sync.WaitGroup
	ids := make([]string, n)
	created := make([]bool, n)
	errs := make([]error, n)
	start := make(chan struct{})
	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			m, c, err := s.Enqueue(ctx, newMsg("same-key"), t0)
			ids[i], created[i], errs[i] = m.ID, c, err
		}()
	}
	close(start)
	wg.Wait()

	nCreated := 0
	for i := range n {
		if errs[i] != nil {
			t.Fatalf("enqueue %d: %v", i, errs[i])
		}
		if created[i] {
			nCreated++
		}
		if ids[i] != ids[0] {
			t.Fatalf("got different ids %s and %s for the same key", ids[0], ids[i])
		}
	}
	if nCreated != 1 {
		t.Fatalf("created = %d, want exactly 1", nCreated)
	}
	counts, err := s.CountByState(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if counts[store.StateQueued] != 1 {
		t.Fatalf("queued rows = %d, want 1", counts[store.StateQueued])
	}
}

func TestEnqueueKeyReuseWithDifferentPayload(t *testing.T) {
	s := testdb.New(t)
	ctx := context.Background()
	orig, _, err := s.Enqueue(ctx, newMsg("k"), t0)
	if err != nil {
		t.Fatal(err)
	}
	changed := newMsg("k")
	changed.Body = "a different body"
	got, created, err := s.Enqueue(ctx, changed, t0)
	if !errors.Is(err, store.ErrIdempotencyConflict) {
		t.Fatalf("err = %v, want ErrIdempotencyConflict", err)
	}
	if created || got.ID != orig.ID {
		t.Fatalf("conflict should return the original message, got created=%v id=%s", created, got.ID)
	}
}

func TestClaimSkipLockedNeverDoubleClaims(t *testing.T) {
	s := testdb.New(t)
	ctx := context.Background()
	const nMsgs, nWorkers = 200, 8
	for i := range nMsgs {
		if _, _, err := s.Enqueue(ctx, newMsg(fmt.Sprintf("k%d", i)), t0); err != nil {
			t.Fatal(err)
		}
	}

	var mu sync.Mutex
	claimedBy := map[string][]string{} // message id -> workers that claimed it
	perWorker := make([]int, nWorkers)
	var wg sync.WaitGroup
	for w := range nWorkers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			id := fmt.Sprintf("w%d", w)
			for {
				m, err := s.Claim(ctx, id, t0)
				if err != nil {
					t.Errorf("claim: %v", err)
					return
				}
				if m == nil {
					return
				}
				mu.Lock()
				claimedBy[m.ID] = append(claimedBy[m.ID], id)
				perWorker[w]++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	if len(claimedBy) != nMsgs {
		t.Fatalf("claimed %d distinct messages, want %d", len(claimedBy), nMsgs)
	}
	for id, ws := range claimedBy {
		if len(ws) != 1 {
			t.Fatalf("message %s claimed %d times by %v", id, len(ws), ws)
		}
	}
	busy := 0
	for _, n := range perWorker {
		if n > 0 {
			busy++
		}
	}
	t.Logf("claims per worker: %v", perWorker)
	if busy < 2 {
		t.Fatalf("only %d worker(s) claimed anything; expected concurrent claiming", busy)
	}
}

func TestClaimOnlyDueMessages(t *testing.T) {
	s := testdb.New(t)
	ctx := context.Background()
	m, _, _ := s.Enqueue(ctx, newMsg("k"), t0)
	c, _ := s.Claim(ctx, "w1", t0)
	if err := s.Complete(ctx, store.Completion{
		WorkerID: "w1", MessageID: c.ID, AttemptNumber: 1, StartedAt: t0, FinishedAt: t0,
		Outcome: "transient", NewState: store.StateDeferred, NextAttemptAt: t0.Add(10 * time.Minute),
	}); err != nil {
		t.Fatal(err)
	}

	if got, _ := s.Claim(ctx, "w1", t0.Add(9*time.Minute)); got != nil {
		t.Fatal("claimed a message before next_attempt_at")
	}
	got, err := s.Claim(ctx, "w1", t0.Add(10*time.Minute))
	if err != nil || got == nil || got.ID != m.ID {
		t.Fatalf("claim at due time = %v, %v; want message %s", got, err, m.ID)
	}
	if got.AttemptCount != 1 {
		t.Fatalf("attempt_count = %d, want 1", got.AttemptCount)
	}
}

func TestReapAndFencing(t *testing.T) {
	s := testdb.New(t)
	ctx := context.Background()
	m, _, _ := s.Enqueue(ctx, newMsg("k"), t0)
	if _, err := s.Claim(ctx, "w1", t0); err != nil {
		t.Fatal(err)
	}

	// Lease not yet stale: nothing reaped.
	if n, err := s.Reap(ctx, t0, t0); err != nil || n != 0 {
		t.Fatalf("early reap = %d, %v; want 0", n, err)
	}
	// w1 "died". After the lease, the reaper returns the message to the queue.
	later := t0.Add(20 * time.Minute)
	if n, err := s.Reap(ctx, later.Add(-15*time.Minute), later); err != nil || n != 1 {
		t.Fatalf("reap = %d, %v; want 1", n, err)
	}
	got, attempts, err := s.Get(ctx, m.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.State != store.StateDeferred || got.AttemptCount != 1 || got.LockedBy != nil {
		t.Fatalf("after reap: state=%s attempts=%d locked_by=%v", got.State, got.AttemptCount, got.LockedBy)
	}
	if len(attempts) != 1 || attempts[0].Reason != store.ReapReason || attempts[0].WorkerID != "w1" {
		t.Fatalf("reap attempt = %+v", attempts)
	}

	// w2 picks it up. Then w1 turns out to be alive and tries to report:
	// its write must not clobber w2's lease.
	if c, _ := s.Claim(ctx, "w2", later); c == nil {
		t.Fatal("w2 could not claim reaped message")
	}
	err = s.Complete(ctx, store.Completion{
		WorkerID: "w1", MessageID: m.ID, AttemptNumber: 1, StartedAt: t0, FinishedAt: later,
		Outcome: "delivered", NewState: store.StateDelivered,
	})
	if !errors.Is(err, store.ErrLeaseLost) {
		t.Fatalf("stale worker complete err = %v, want ErrLeaseLost", err)
	}
	got, attempts, _ = s.Get(ctx, m.ID)
	if got.State != store.StateSending || *got.LockedBy != "w2" {
		t.Fatalf("stale write clobbered lease: state=%s locked_by=%v", got.State, *got.LockedBy)
	}
	if len(attempts) != 2 {
		t.Fatalf("stale worker's attempt should still be logged, got %d attempts", len(attempts))
	}
}

func TestReleaseRestoresPreviousState(t *testing.T) {
	s := testdb.New(t)
	ctx := context.Background()
	m, _, _ := s.Enqueue(ctx, newMsg("k"), t0)
	_, _ = s.Claim(ctx, "w1", t0)
	if err := s.Release(ctx, "w1", m.ID, t0.Add(time.Second), t0); err != nil {
		t.Fatal(err)
	}
	got, _, _ := s.Get(ctx, m.ID)
	if got.State != store.StateQueued || got.AttemptCount != 0 {
		t.Fatalf("after release: state=%s attempts=%d, want queued/0", got.State, got.AttemptCount)
	}
	if err := s.Release(ctx, "w1", m.ID, t0, t0); !errors.Is(err, store.ErrLeaseLost) {
		t.Fatalf("double release err = %v, want ErrLeaseLost", err)
	}
}

func TestGetUnknownID(t *testing.T) {
	s := testdb.New(t)
	_, _, err := s.Get(context.Background(), "00000000-0000-0000-0000-000000000000")
	if !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("err = %v, want ErrNotFound", err)
	}
}
