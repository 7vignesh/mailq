// Package clock lets time-dependent code run against a fake clock in tests.
package clock

import (
	"sync"
	"time"
)

type Clock interface {
	Now() time.Time
}

type Real struct{}

// Now truncates to microseconds, Postgres's timestamp precision, so a time
// read back from the database compares equal to the one written.
func (Real) Now() time.Time { return time.Now().UTC().Truncate(time.Microsecond) }

// Fake is a manually advanced clock, safe for concurrent use.
type Fake struct {
	mu  sync.Mutex
	now time.Time
}

func NewFake(t time.Time) *Fake { return &Fake{now: t.UTC().Truncate(time.Microsecond)} }

func (f *Fake) Now() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.now
}

func (f *Fake) Advance(d time.Duration) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.now = f.now.Add(d)
}

func (f *Fake) Set(t time.Time) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.now = t.UTC().Truncate(time.Microsecond)
}
