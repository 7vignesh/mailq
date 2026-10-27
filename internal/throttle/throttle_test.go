package throttle

import (
	"math"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/7vignesh/mailq/internal/clock"
)

var t0 = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

func newTest(mod func(*Config)) (*Limiter, *clock.Fake) {
	cfg := DefaultConfig()
	if mod != nil {
		mod(&cfg)
	}
	clk := clock.NewFake(t0)
	return New(cfg, clk), clk
}

func approx(a, b float64) bool { return math.Abs(a-b) < 1e-6 }

func TestTokenBucket(t *testing.T) {
	tests := []struct {
		name      string
		rate      float64
		burst     float64
		wantBurst int           // acquires that succeed immediately
		wait      time.Duration // suggested wait once empty
	}{
		{"burst capped by Burst", 600, 10, 10, 100 * time.Millisecond},
		{"burst capped by 10s of rate", 60, 100, 10, time.Second},
		{"slow domain gets one token", 1, 10, 1, time.Minute},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l, clk := newTest(func(c *Config) { c.InitialRate, c.Burst, c.MaxConcurrent = tt.rate, tt.burst, 1000 })
			got := 0
			for {
				p, wait := l.TryAcquire("a.test")
				if p == nil {
					if d := wait - tt.wait; d < -time.Millisecond || d > time.Millisecond {
						t.Fatalf("wait = %v, want %v", wait, tt.wait)
					}
					break
				}
				got++
			}
			if got != tt.wantBurst {
				t.Fatalf("burst = %d, want %d", got, tt.wantBurst)
			}
			clk.Advance(tt.wait)
			if p, _ := l.TryAcquire("a.test"); p == nil {
				t.Fatal("no token after the suggested wait")
			}
		})
	}
}

func TestConcurrencyCap(t *testing.T) {
	l, _ := newTest(func(c *Config) { c.MaxConcurrent = 2 })
	p1, _ := l.TryAcquire("a.test")
	p2, _ := l.TryAcquire("a.test")
	if p1 == nil || p2 == nil {
		t.Fatal("first two acquires should succeed")
	}
	if p, wait := l.TryAcquire("a.test"); p != nil || wait != time.Second {
		t.Fatalf("third acquire = %v, %v; want nil, BusyRetry", p, wait)
	}
	if p, _ := l.TryAcquire("b.test"); p == nil {
		t.Fatal("another domain must not be affected")
	}
	p1.Done(Neutral)
	p1.Done(Neutral) // idempotent: must not free a second slot
	if p, _ := l.TryAcquire("a.test"); p == nil {
		t.Fatal("slot not freed by Done")
	}
	if p, _ := l.TryAcquire("a.test"); p != nil {
		t.Fatal("double Done freed two slots")
	}
}

func TestAIMD(t *testing.T) {
	type step struct {
		advance time.Duration
		signal  Signal
		rate    float64
	}
	tests := []struct {
		name  string
		steps []step
	}{
		{"throttle halves the rate", []step{{0, Throttled, 30}}},
		{"throttles within cooldown count once", []step{
			{0, Throttled, 30}, {time.Second, Throttled, 30}, {59 * time.Second, Throttled, 15}}},
		{"floor at MinRate", []step{
			{0, Throttled, 30}, {time.Minute, Throttled, 15}, {time.Minute, Throttled, 7.5},
			{time.Minute, Throttled, 3.75}, {time.Minute, Throttled, 1.875}, {time.Minute, Throttled, 1},
			{time.Minute, Throttled, 1}}},
		{"additive increase once per interval", []step{
			{0, Success, 60}, {time.Minute, Success, 70}, {time.Second, Success, 70},
			{time.Minute, Success, 80}}},
		{"no increase right after a decrease", []step{
			{0, Throttled, 30}, {30 * time.Second, Success, 30}, {30 * time.Second, Success, 40}}},
		{"neutral changes nothing", []step{{time.Minute, Neutral, 60}}},
		{"capped at MaxRate", []step{
			{time.Minute, Success, 70}, {time.Minute, Success, 80}, {time.Minute, Success, 80}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l, clk := newTest(func(c *Config) { c.MaxRate = 80; c.Burst = 1000; c.MaxConcurrent = 1000 })
			l.TryAcquire("a.test") // create state at t0
			for i, s := range tt.steps {
				clk.Advance(s.advance)
				p := &Permit{l: l, domain: "a.test"}
				l.mu.Lock()
				l.domains["a.test"].inflight++ // pretend this permit was acquired
				l.mu.Unlock()
				p.Done(s.signal)
				st, _ := l.Stats("a.test")
				if !approx(st.Rate, s.rate) {
					t.Fatalf("step %d: rate = %v, want %v", i, st.Rate, s.rate)
				}
			}
		})
	}
}

func TestThrottleStopsSendingImmediately(t *testing.T) {
	l, _ := newTest(nil)
	p, _ := l.TryAcquire("a.test") // 9 tokens left
	p.Done(Throttled)
	if q, wait := l.TryAcquire("a.test"); q != nil || wait <= 0 {
		t.Fatalf("acquire right after a throttle = %v, %v; want blocked", q, wait)
	}
	if b := l.Blocked(); len(b) != 1 || b[0] != "a.test" {
		t.Fatalf("Blocked = %v", b)
	}
}

func TestBlockedAndSweep(t *testing.T) {
	l, clk := newTest(func(c *Config) { c.MaxConcurrent = 1 })
	busy, _ := l.TryAcquire("busy.test")
	idle, _ := l.TryAcquire("idle.test")
	idle.Done(Success)
	if b := l.Blocked(); len(b) != 1 || b[0] != "busy.test" {
		t.Fatalf("Blocked = %v, want [busy.test]", b)
	}

	clk.Advance(2 * time.Hour)
	if n := l.Sweep(); n != 1 {
		t.Fatalf("Sweep removed %d, want 1 (in-flight domains must be kept)", n)
	}
	if _, ok := l.Stats("busy.test"); !ok {
		t.Fatal("swept a domain with a delivery in flight")
	}
	busy.Done(Neutral)
}

func TestConcurrentUseNeverExceedsCap(t *testing.T) {
	l, _ := newTest(func(c *Config) { c.MaxConcurrent = 3; c.Burst = 1e9; c.InitialRate = 1e12 })
	var inflight, peak atomic.Int32
	var wg sync.WaitGroup
	for range 50 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 200 {
				p, _ := l.TryAcquire("a.test")
				if p == nil {
					continue
				}
				n := inflight.Add(1)
				for {
					old := peak.Load()
					if n <= old || peak.CompareAndSwap(old, n) {
						break
					}
				}
				inflight.Add(-1)
				p.Done(Success)
			}
		}()
	}
	wg.Wait()
	if p := peak.Load(); p > 3 || p == 0 {
		t.Fatalf("peak concurrency %d, want 1..3", p)
	}
	if st, _ := l.Stats("a.test"); st.InFlight != 0 {
		t.Fatalf("leaked %d slots", st.InFlight)
	}
}

func TestValidate(t *testing.T) {
	if err := DefaultConfig().Validate(); err != nil {
		t.Fatal(err)
	}
	bad := DefaultConfig()
	bad.MinRate = 0
	if bad.Validate() == nil {
		t.Fatal("MinRate 0 accepted")
	}
	bad = DefaultConfig()
	bad.DecreaseFactor = 1
	if bad.Validate() == nil {
		t.Fatal("DecreaseFactor 1 accepted")
	}
}
