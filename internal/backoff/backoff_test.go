package backoff

import (
	"math/rand/v2"
	"testing"
	"time"

	"github.com/7vignesh/mailq/internal/clock"
)

func fixed(v float64) func() float64 { return func() float64 { return v } }

func TestCeiling(t *testing.T) {
	p := Default() // base 5m, max 4h
	tests := []struct {
		attempt int
		want    time.Duration
	}{
		{0, 5 * time.Minute}, {1, 5 * time.Minute}, {2, 10 * time.Minute}, {3, 20 * time.Minute},
		{4, 40 * time.Minute}, {5, 80 * time.Minute}, {6, 160 * time.Minute},
		{7, 4 * time.Hour}, {8, 4 * time.Hour}, {1000, 4 * time.Hour}, // capped, no overflow
	}
	for _, tt := range tests {
		if got := p.Ceiling(tt.attempt); got != tt.want {
			t.Errorf("Ceiling(%d) = %v, want %v", tt.attempt, got, tt.want)
		}
	}
}

func TestDelayJitterBounds(t *testing.T) {
	p := Default()
	tests := []struct {
		name    string
		rnd     float64
		attempt int
		want    time.Duration
	}{
		{"lowest draw hits the floor", 0, 5, time.Minute},
		{"middle draw", 0.5, 2, time.Minute + 9*time.Minute/2},
		{"highest draw stays under ceiling", 0.999999, 3, 20 * time.Minute},
	}
	for _, tt := range tests {
		p.Rand = fixed(tt.rnd)
		got := p.Delay(tt.attempt)
		if d := got - tt.want; d < -time.Second || d > time.Second {
			t.Errorf("%s: Delay(%d) = %v, want ~%v", tt.name, tt.attempt, got, tt.want)
		}
	}

	// Real randomness: every draw within [Min, Ceiling), and actually spread.
	p.Rand = rand.New(rand.NewPCG(1, 2)).Float64
	for attempt := 1; attempt <= 10; attempt++ {
		lo, hi := time.Duration(1<<62), time.Duration(0)
		for range 2000 {
			d := p.Delay(attempt)
			if d < p.Min || d >= p.Ceiling(attempt) {
				t.Fatalf("attempt %d: delay %v outside [%v, %v)", attempt, d, p.Min, p.Ceiling(attempt))
			}
			lo, hi = min(lo, d), max(hi, d)
		}
		if spread := hi - lo; spread < (p.Ceiling(attempt)-p.Min)*9/10 {
			t.Errorf("attempt %d: draws span only %v of a %v window", attempt, spread, p.Ceiling(attempt)-p.Min)
		}
	}
}

func TestNextGivesUpAtMaxAge(t *testing.T) {
	p := Default()
	p.Rand = fixed(0.5)
	created := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	tests := []struct {
		name    string
		age     time.Duration
		giveUp  bool
		atLimit bool // retry clamped onto the deadline
	}{
		{"young", time.Minute, false, false},
		{"just under deadline, clamped", 72*time.Hour - time.Minute, false, true},
		{"exactly at deadline", 72 * time.Hour, true, false},
		{"past deadline", 100 * time.Hour, true, false},
	}
	for _, tt := range tests {
		d := p.Next(8, created, created.Add(tt.age))
		if d.GiveUp != tt.giveUp {
			t.Errorf("%s: GiveUp = %v, want %v", tt.name, d.GiveUp, tt.giveUp)
		}
		if tt.atLimit && !d.At.Equal(created.Add(p.MaxAge)) {
			t.Errorf("%s: At = %v, want the deadline", tt.name, d.At)
		}
	}
}

// Simulates one message that fails every attempt, driven by a fake clock.
func TestFullScheduleWithFakeClock(t *testing.T) {
	created := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	tests := []struct {
		name         string
		rnd          float64
		wantAttempts int
	}{
		// Always the ceiling: attempts 1-7 at 0, 5m, 15m, ... 5h15m, then
		// every 4h up to attempt 23 at 69h15m, then attempt 24 clamped to 72h.
		{"max draws", 0.999999999, 24},
		// Always the floor: one attempt per minute from minute 0 to 4320.
		{"min draws", 0, 72*60 + 1},
	}
	for _, tt := range tests {
		p := Default()
		p.Rand = fixed(tt.rnd)
		clk := clock.NewFake(created)
		attempts := 0
		for {
			attempts++
			d := p.Next(attempts, created, clk.Now())
			if d.GiveUp {
				break
			}
			if d.Delay < 0 || d.At.Before(clk.Now()) {
				t.Fatalf("%s: retry scheduled in the past", tt.name)
			}
			clk.Set(d.At)
			if attempts > 10000 {
				t.Fatalf("%s: never gave up", tt.name)
			}
		}
		if attempts != tt.wantAttempts {
			t.Errorf("%s: %d attempts before giving up, want %d", tt.name, attempts, tt.wantAttempts)
		}
		if age := clk.Now().Sub(created); age != p.MaxAge {
			t.Errorf("%s: final attempt at age %v, want exactly %v", tt.name, age, p.MaxAge)
		}
	}
}

func TestValidate(t *testing.T) {
	bad := []Policy{
		{},
		{Min: time.Minute, Base: time.Second, Max: time.Hour, MaxAge: time.Hour},
		{Min: time.Second, Base: time.Hour, Max: time.Minute, MaxAge: time.Hour},
		{Min: time.Second, Base: time.Second, Max: time.Second, MaxAge: 0},
	}
	for _, p := range bad {
		if p.Validate() == nil {
			t.Errorf("Validate(%+v) = nil, want error", p)
		}
	}
	if err := Default().Validate(); err != nil {
		t.Errorf("Default invalid: %v", err)
	}
}
