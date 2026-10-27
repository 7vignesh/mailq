// Package throttle limits how hard mailq pushes each recipient domain: a
// cap on concurrent connections and a token bucket for messages per minute,
// whose rate adapts with AIMD (additive increase, multiplicative decrease).
//
// State is in memory and per process. On restart every domain starts again
// at InitialRate, which is why InitialRate is deliberately conservative:
// a restart forgets that a domain recently throttled us, and the first
// minutes after it must not be a burst. With several worker processes each
// one has its own budget, so a domain sees up to N times the configured
// rate; a multi-process deployment would need shared state (a Postgres
// table or Redis) for these limits to mean what they say.
package throttle

import (
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/7vignesh/mailq/internal/clock"
)

// Config defaults are starting points, not measured provider limits.
// Unverified: real providers publish few numbers, and what they tolerate
// depends on sender reputation.
type Config struct {
	MaxConcurrent    int           // concurrent deliveries per domain
	InitialRate      float64       // messages/minute for a domain we have no state for
	MinRate          float64       // never slow a domain below this
	MaxRate          float64       // never speed up beyond this
	Burst            float64       // bucket capacity in messages (see capacity)
	IncreaseStep     float64       // messages/minute added per IncreaseInterval of success
	IncreaseInterval time.Duration // at most one increase per interval
	DecreaseFactor   float64       // rate is multiplied by this on a throttle signal
	DecreaseCooldown time.Duration // at most one decrease per cooldown
	BusyRetry        time.Duration // suggested wait when all connections are in use
	IdleTTL          time.Duration // Sweep forgets domains idle this long
}

func DefaultConfig() Config {
	return Config{
		MaxConcurrent: 5, InitialRate: 60, MinRate: 1, MaxRate: 600, Burst: 10,
		IncreaseStep: 10, IncreaseInterval: time.Minute,
		DecreaseFactor: 0.5, DecreaseCooldown: time.Minute,
		BusyRetry: time.Second, IdleTTL: time.Hour,
	}
}

// Signal is what an attempt told us about the domain's capacity.
type Signal int

const (
	Neutral   Signal = iota // says nothing about load (bounce, greylist, timeout)
	Success                 // accepted: room to speed up
	Throttled               // 421 / 4.7.x / rate-limit wording: slow down
)

type Limiter struct {
	cfg   Config
	clock clock.Clock

	mu      sync.Mutex
	domains map[string]*bucket
}

type bucket struct {
	rate         float64 // messages per minute
	tokens       float64
	inflight     int
	refilled     time.Time
	lastIncrease time.Time
	lastDecrease time.Time
	lastUsed     time.Time
	decreases    int // lifetime count, for metrics and tests
}

func New(cfg Config, clk clock.Clock) *Limiter {
	return &Limiter{cfg: cfg, clock: clk, domains: map[string]*bucket{}}
}

func (c Config) Validate() error {
	switch {
	case c.MaxConcurrent < 1:
		return errors.New("throttle: MaxConcurrent must be >= 1")
	case c.MinRate <= 0 || c.InitialRate < c.MinRate || c.MaxRate < c.InitialRate:
		return fmt.Errorf("throttle: need 0 < min (%v) <= initial (%v) <= max (%v)", c.MinRate, c.InitialRate, c.MaxRate)
	case c.DecreaseFactor <= 0 || c.DecreaseFactor >= 1:
		return errors.New("throttle: DecreaseFactor must be in (0, 1)")
	case c.Burst < 1 || c.IncreaseStep < 0:
		return errors.New("throttle: Burst must be >= 1 and IncreaseStep >= 0")
	}
	return nil
}

// Permit is one granted delivery slot. Done must be called exactly once.
type Permit struct {
	l      *Limiter
	domain string
	once   sync.Once
}

// TryAcquire takes a connection slot and a token for domain without
// blocking. If either is unavailable it returns nil and a suggested wait.
//
// Not blocking is deliberate: a worker goroutine waiting on a slow domain
// is a goroutine not delivering to any other domain.
func (l *Limiter) TryAcquire(domain string) (*Permit, time.Duration) {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := l.clock.Now()
	b := l.get(domain, now)
	b.lastUsed = now
	if b.inflight >= l.cfg.MaxConcurrent {
		return nil, l.cfg.BusyRetry
	}
	if b.tokens < 1 {
		return nil, b.untilToken()
	}
	b.tokens--
	b.inflight++
	return &Permit{l: l, domain: domain}, 0
}

// Done releases the slot and feeds the attempt's signal into AIMD.
func (p *Permit) Done(s Signal) {
	p.once.Do(func() { p.l.done(p.domain, s) })
}

func (l *Limiter) done(domain string, s Signal) {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := l.clock.Now()
	b := l.get(domain, now)
	b.inflight--
	b.lastUsed = now
	switch s {
	case Throttled:
		// One decrease per cooldown, like TCP's one decrease per RTT: five
		// connections hitting the same 421 at once is one signal, not five,
		// and should halve the rate once, not divide it by 32.
		if now.Sub(b.lastDecrease) < l.cfg.DecreaseCooldown {
			return
		}
		b.rate = max(l.cfg.MinRate, b.rate*l.cfg.DecreaseFactor)
		b.tokens = min(b.tokens, 0) // stop sending now, not after the burst drains
		b.lastDecrease, b.lastIncrease = now, now
		b.decreases++
	case Success:
		// Additive in time, not per message: per-message increases would
		// grow the rate in proportion to itself, which is exponential.
		if now.Sub(b.lastIncrease) < l.cfg.IncreaseInterval {
			return
		}
		b.rate = min(l.cfg.MaxRate, b.rate+l.cfg.IncreaseStep)
		b.lastIncrease = now
	}
}

// Blocked lists domains that cannot take a delivery right now, so the
// worker can skip their messages when claiming instead of claiming and
// handing them back over and over. Cost is O(domains) per call.
func (l *Limiter) Blocked() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := l.clock.Now()
	var out []string
	for d, b := range l.domains {
		b.refill(now, l.capacity(b))
		if b.inflight >= l.cfg.MaxConcurrent || b.tokens < 1 {
			out = append(out, d)
		}
	}
	sort.Strings(out)
	return out
}

// Sweep forgets idle domains so the map does not grow forever. A forgotten
// domain that had been slowed down starts again at InitialRate, exactly as
// after a restart.
func (l *Limiter) Sweep() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := l.clock.Now()
	n := 0
	for d, b := range l.domains {
		if b.inflight == 0 && now.Sub(b.lastUsed) >= l.cfg.IdleTTL {
			delete(l.domains, d)
			n++
		}
	}
	return n
}

// Stats is a snapshot of one domain, for metrics and tests.
type Stats struct {
	Rate      float64
	Tokens    float64
	InFlight  int
	Decreases int
}

func (l *Limiter) Stats(domain string) (Stats, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	b, ok := l.domains[domain]
	if !ok {
		return Stats{}, false
	}
	b.refill(l.clock.Now(), l.capacity(b))
	return Stats{Rate: b.rate, Tokens: b.tokens, InFlight: b.inflight, Decreases: b.decreases}, true
}

func (l *Limiter) get(domain string, now time.Time) *bucket {
	b, ok := l.domains[domain]
	if !ok {
		b = &bucket{rate: l.cfg.InitialRate, refilled: now, lastIncrease: now, lastUsed: now}
		b.tokens = l.capacity(b)
		l.domains[domain] = b
	}
	b.refill(now, l.capacity(b))
	return b
}

// capacity is the bucket size: Burst, but never more than ten seconds of
// the current rate (and at least one message). Without the second limit, a
// domain slowed to 1/minute could still take a burst of 10 after idling.
func (l *Limiter) capacity(b *bucket) float64 {
	return max(1, min(l.cfg.Burst, b.rate/6))
}

func (b *bucket) refill(now time.Time, capacity float64) {
	if el := now.Sub(b.refilled); el > 0 {
		b.tokens = min(capacity, b.tokens+el.Minutes()*b.rate)
		b.refilled = now
	}
	if b.tokens > capacity { // capacity shrank after a decrease
		b.tokens = capacity
	}
}

func (b *bucket) untilToken() time.Duration {
	need := 1 - b.tokens
	return time.Duration(need / b.rate * float64(time.Minute))
}
