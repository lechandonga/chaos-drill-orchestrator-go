// Package clock provides an injectable clock so that engine logic and tests
// never depend on real wall-clock waiting.
package clock

import (
	"sync"
	"time"
)

// Clock is the subset of time-related functionality the engine needs.
type Clock interface {
	Now() time.Time
	NewTimer(d time.Duration) Timer
}

// Timer mirrors time.Timer without embedding it.
type Timer interface {
	C() <-chan time.Time
	Stop() bool
}

// Real is backed by the standard library time package.
type Real struct{}

func (Real) Now() time.Time { return time.Now() }

func (Real) NewTimer(d time.Duration) Timer { return &realTimer{t: time.NewTimer(d)} }

type realTimer struct{ t *time.Timer }

func (r *realTimer) C() <-chan time.Time { return r.t.C }
func (r *realTimer) Stop() bool          { return r.t.Stop() }

// Fake is a manually advanced clock. Timers never fire spontaneously; they
// fire when Advance moves the fake time past their deadline.
type Fake struct {
	mu     sync.Mutex
	now    time.Time
	seq    int64
	timers map[int64]*fakeTimer
}

type fakeTimer struct {
	id       int64
	deadline time.Time
	ch       chan time.Time
	fired    bool
	stopped  bool
	clock    *Fake
}

func (t *fakeTimer) C() <-chan time.Time { return t.ch }

func (t *fakeTimer) Stop() bool {
	t.clock.mu.Lock()
	defer t.clock.mu.Unlock()
	if t.fired || t.stopped {
		return false
	}
	t.stopped = true
	delete(t.clock.timers, t.id)
	return true
}

func NewFake(start time.Time) *Fake {
	return &Fake{now: start, timers: make(map[int64]*fakeTimer)}
}

// Pending reports how many timers are registered but not yet fired or
// stopped. Tests use it to synchronize: wait until the worker is parked on
// the fake clock before advancing, so no advance is ever "lost" to a timer
// that had not registered yet.
func (f *Fake) Pending() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.timers)
}

func (f *Fake) Now() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.now
}

func (f *Fake) NewTimer(d time.Duration) Timer {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.seq++
	now := f.now
	t := &fakeTimer{
		id:       f.seq,
		deadline: now.Add(d),
		ch:       make(chan time.Time, 1),
		clock:    f,
	}
	// Timers created at or after their deadline fire immediately, matching
	// time.NewTimer with non-positive duration; otherwise an Advance that
	// wins the race against timer registration would hang the waiter.
	if d <= 0 || !t.deadline.After(now) {
		t.fired = true
		t.ch <- now
		return t
	}
	f.timers[t.id] = t
	return t
}

// Advance moves the clock forward by d, firing every due timer at most once.
// Due timers are fired in deadline order; each receives a buffered value so
// the goroutine waiting on it is never blocked on the clock goroutine.
func (f *Fake) Advance(d time.Duration) {
	f.mu.Lock()
	f.now = f.now.Add(d)
	type due struct {
		at time.Time
		t  *fakeTimer
	}
	var pending []due
	for _, t := range f.timers {
		if !t.stopped && !t.fired && !t.deadline.After(f.now) {
			pending = append(pending, due{t.deadline, t})
		}
	}
	// Order fires by deadline (stable via id) so overlapping timers are
	// deterministic in tests.
	for i := 1; i < len(pending); i++ {
		for j := i; j > 0; j-- {
			a, b := pending[j-1], pending[j]
			if a.at.After(b.at) || (a.at.Equal(b.at) && a.t.id > b.t.id) {
				pending[j-1], pending[j] = b, a
			} else {
				break
			}
		}
	}
	for _, p := range pending {
		t := p.t
		t.fired = true
		delete(f.timers, t.id)
		t.ch <- p.at
	}
	f.mu.Unlock()

	// Give woken goroutines a chance to make progress under -race without any
	// real experiment durations being waited on.
	if len(pending) > 0 {
		yield()
	}
}
