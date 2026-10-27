// Package backoff decides when a deferred message is retried, and when to
// stop retrying.
package backoff

import (
	"fmt"
	"math/rand/v2"
	"time"
)

// Policy is exponential backoff with full jitter above a floor:
//
//	ceiling(n) = min(Max, Base * 2^(n-1))      n = attempts made so far
//	delay(n)   = Min + rand[0,1) * (ceiling(n) - Min)
//
// Full jitter spreads retries uniformly over the window, so messages that
// failed together (a domain outage, a burst of 421s) do not come back
// together and recreate the burst. The floor keeps a retry from landing
// seconds after a greylisting or throttling reply, when it would almost
// certainly fail again. Unverified: greylisting windows of a few minutes are
// common folklore (Postgrey defaults to 5 minutes), not a measured number.
type Policy struct {
	Min    time.Duration // floor for every delay
	Base   time.Duration // ceiling after the first attempt
	Max    time.Duration // cap on the ceiling
	MaxAge time.Duration // total time from enqueue before giving up

	// Rand returns a float in [0, 1). Nil means math/rand/v2.
	Rand func() float64
}

func Default() Policy {
	return Policy{Min: time.Minute, Base: 5 * time.Minute, Max: 4 * time.Hour, MaxAge: 72 * time.Hour}
}

func (p Policy) Validate() error {
	if p.Min <= 0 || p.Base < p.Min || p.Max < p.Base || p.MaxAge <= 0 {
		return fmt.Errorf("backoff: need 0 < min <= base <= max and max_age > 0, got %+v", p)
	}
	return nil
}

// Decision is what to do after a transient failure.
type Decision struct {
	GiveUp bool
	At     time.Time     // next attempt time, if not giving up
	Delay  time.Duration // At - now
}

// Next decides after the attempt-th transient failure (1-based) of a message
// enqueued at created.
//
// A message gives up once it is MaxAge old. If the jittered delay would
// overshoot the deadline, the retry is pulled in to land exactly on it, so
// every message gets a final attempt at MaxAge instead of failing early
// because its next slot happened to fall past the line.
func (p Policy) Next(attempt int, created, now time.Time) Decision {
	deadline := created.Add(p.MaxAge)
	if !now.Before(deadline) {
		return Decision{GiveUp: true}
	}
	d := p.Delay(attempt)
	at := now.Add(d)
	if at.After(deadline) {
		at, d = deadline, deadline.Sub(now)
	}
	return Decision{At: at, Delay: d}
}

// Delay is the jittered delay after the attempt-th failure.
func (p Policy) Delay(attempt int) time.Duration {
	c := p.Ceiling(attempt)
	r := rand.Float64
	if p.Rand != nil {
		r = p.Rand
	}
	return p.Min + time.Duration(r()*float64(c-p.Min))
}

// Ceiling is the upper bound of Delay(attempt).
func (p Policy) Ceiling(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	c := p.Base
	for i := 1; i < attempt && c < p.Max; i++ {
		c *= 2 // loop instead of a shift: cannot overflow, stops at Max
	}
	return min(c, p.Max)
}
