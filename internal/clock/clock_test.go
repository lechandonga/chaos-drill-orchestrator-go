package clock

import (
	"testing"
	"time"
)

func TestFakeTimerFiresOnAdvance(t *testing.T) {
	clk := NewFake(time.Unix(0, 0))
	tm := clk.NewTimer(50 * time.Millisecond)
	clk.Advance(49 * time.Millisecond)
	select {
	case <-tm.C():
		t.Fatal("timer fired early")
	default:
	}
	clk.Advance(time.Millisecond)
	select {
	case <-tm.C():
	default:
		t.Fatal("timer did not fire at deadline")
	}
	if got := clk.Pending(); got != 0 {
		t.Fatalf("pending after fire = %d, want 0", got)
	}
}

func TestFakeTimerImmediateWhenDeadlinePassed(t *testing.T) {
	clk := NewFake(time.Unix(0, 0))
	clk.Advance(100 * time.Millisecond)
	// A timer created after the clock passed its deadline must fire at once,
	// otherwise a waiter racing an Advance would hang forever.
	tm := clk.NewTimer(0)
	select {
	case <-tm.C():
	default:
		t.Fatal("late-registered timer did not fire immediately")
	}
}

func TestFakeStop(t *testing.T) {
	clk := NewFake(time.Unix(0, 0))
	tm := clk.NewTimer(10 * time.Millisecond)
	if !tm.Stop() {
		t.Fatal("Stop returned false for a pending timer")
	}
	clk.Advance(20 * time.Millisecond)
	select {
	case <-tm.C():
		t.Fatal("stopped timer fired")
	default:
	}
}
