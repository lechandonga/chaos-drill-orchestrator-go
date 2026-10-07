package engine

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/lechandonga/chaos-drill-orchestrator-go/internal/clock"
	"github.com/lechandonga/chaos-drill-orchestrator-go/internal/experiment"
)

// TestConcurrentConflictReject verifies the "never overwrite" rule: with the
// reject policy, concurrent experiments competing for one target produce one
// winner and explicit conflict rejections for the rest.
func TestConcurrentConflictReject(t *testing.T) {
	clk := clock.NewFake(time.Unix(1000, 0))
	reg := newRegistry()
	e := newTestEngine(t, reg, newWAL(t), clk, ConflictReject)

	const n = 8
	var wg sync.WaitGroup
	var mu sync.Mutex
	var accepted, rejected int
	start := make(chan struct{})

	for i := range n {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			spec := latencySpec("exp-reject-"+itoa(i), []string{"svc-a"}, 50, 200, 1)
			err := e.Start(context.Background(), spec)
			mu.Lock()
			defer mu.Unlock()
			if err == nil {
				accepted++
				return
			}
			var rej *experiment.RejectError
			if errors.As(err, &rej) && rej.Code == ConflictCode {
				rejected++
				return
			}
			t.Errorf("unexpected start error: %v", err)
		}(i)
	}
	close(start)

	// Give contending goroutines time to race for the target lock.
	eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return accepted+rejected == n
	})
	wg.Wait()

	if accepted != 1 || rejected != n-1 {
		t.Fatalf("accepted=%d rejected=%d, want exactly 1 and %d", accepted, rejected, n-1)
	}

	// Exactly one fault is active on svc-a and it is the winner's.
	tgt, _ := reg.Get("svc-a")
	eventually(t, func() bool {
		f := tgt.ActiveFault()
		return f != nil && f.Kind == "latency_delay" && f.Intensity == 50
	})

	waitParked(t, clk, 1)
	clk.Advance(time.Second)
}

// TestConcurrentConflictQueue verifies FIFO queueing: overlapping
// experiments all run, strictly one at a time per target, later ones wait
// instead of overwriting the earlier fault.
func TestConcurrentConflictQueue(t *testing.T) {
	clk := clock.NewFake(time.Unix(1000, 0))
	reg := newRegistry()
	e := newTestEngine(t, reg, newWAL(t), clk, ConflictQueue)

	s1 := latencySpec("q-1", []string{"svc-a"}, 50, 100, 1)
	s2 := latencySpec("q-2", []string{"svc-a"}, 80, 100, 1)
	s3 := latencySpec("q-3", []string{"svc-a"}, 30, 100, 1)

	if err := e.Start(context.Background(), s1); err != nil {
		t.Fatalf("start q-1: %v", err)
	}
	started2 := make(chan error, 1)
	started3 := make(chan error, 1)
	go func() { started2 <- e.Start(context.Background(), s2) }()

	// q-2 must block in the queue while q-1 owns svc-a.
	eventually(t, func() bool { return e.queuedCount() == 1 })
	// Only now enqueue q-3, guaranteeing FIFO order q-2 then q-3.
	go func() { started3 <- e.Start(context.Background(), s3) }()
	eventually(t, func() bool { return e.queuedCount() == 2 })

	// Run q-1 to completion; the queue wakes in FIFO order.
	waitParked(t, clk, 1)
	clk.Advance(150 * time.Millisecond)
	eventually(t, func() bool {
		r, _ := e.Status("q-1")
		return r.Phase.Terminal()
	})
	var err2 error
	eventually(t, func() bool {
		select {
		case err2 = <-started2:
			return true
		default:
			return false
		}
	})
	if err2 != nil {
		t.Fatalf("queued q-2: %v", err2)
	}

	// While q-2 runs, svc-a carries q-2's intensity (80), never q-3's (30).
	tgt, _ := reg.Get("svc-a")
	eventually(t, func() bool {
		f := tgt.ActiveFault()
		return f != nil && f.Intensity == 80
	})

	waitParked(t, clk, 1)
	clk.Advance(150 * time.Millisecond)
	eventually(t, func() bool {
		r, _ := e.Status("q-2")
		return r.Phase.Terminal()
	})
	var err3 error
	eventually(t, func() bool {
		select {
		case err3 = <-started3:
			return true
		default:
			return false
		}
	})
	if err3 != nil {
		t.Fatalf("queued q-3: %v", err3)
	}
	eventually(t, func() bool {
		f := tgt.ActiveFault()
		return f != nil && f.Intensity == 30
	})

	waitParked(t, clk, 1)
	clk.Advance(150 * time.Millisecond)
	eventually(t, func() bool {
		r, _ := e.Status("q-3")
		return r.Phase == experiment.PhaseCompleted
	})

	// Non-overlapping experiments run concurrently regardless of the queue.
	if f := tgt.ActiveFault(); f != nil {
		t.Fatalf("svc-a left fault active: %+v", f)
	}
}

// TestBlastRadiusEnforced proves the engine never affects more targets at
// once than the spec permits (admission already bounds that by the hard cap).
func TestBlastRadiusEnforced(t *testing.T) {
	clk := clock.NewFake(time.Unix(1000, 0))
	reg := newRegistry()
	e := newTestEngine(t, reg, newWAL(t), clk, ConflictReject)

	spec := latencySpec("wave", []string{"svc-a", "svc-b", "svc-c", "svc-d"}, 50, 100, 2)
	if err := e.Start(context.Background(), spec); err != nil {
		t.Fatal(err)
	}

	all := []string{"svc-a", "svc-b", "svc-c", "svc-d"}
	activeFaults := func() map[string]bool {
		m := make(map[string]bool)
		for _, id := range all {
			tg, _ := reg.Get(id)
			m[id] = tg.ActiveFault() != nil
		}
		return m
	}

	// First wave (a,b) injects immediately; c,d must stay untouched, and the
	// count of simultaneously faulted targets must never pass the cap.
	eventually(t, func() bool {
		m := activeFaults()
		return m["svc-a"] && m["svc-b"]
	})
	m := activeFaults()
	if m["svc-c"] || m["svc-d"] {
		t.Fatalf("second wave injected early: %v", m)
	}

	// Two waves of 50ms each; sample in 10ms steps. At every instant the
	// number of faulted targets must be <= blast radius (2).
	for wave := 0; wave < 2; wave++ {
		for step := range 5 {
			waitParked(t, clk, 1)
			clk.Advance(10 * time.Millisecond)
			n := 0
			for _, on := range activeFaults() {
				if on {
					n++
				}
			}
			if n > 2 {
				t.Fatalf("wave %d step %d: %d targets faulted, blast radius is 2", wave, step, n)
			}
		}
		// After wave 1, a,b are cleaned before wave 2 (c,d) injects.
		if wave == 0 {
			eventually(t, func() bool {
				mm := activeFaults()
				return !mm["svc-a"] && !mm["svc-b"]
			})
			eventually(t, func() bool {
				mm := activeFaults()
				return mm["svc-c"] && mm["svc-d"]
			})
		}
	}

	eventually(t, func() bool {
		rr, _ := e.Status("wave")
		return rr.Phase == experiment.PhaseCompleted
	})
	for _, id := range all {
		tg, _ := reg.Get(id)
		if tg.ActiveFault() != nil {
			t.Fatalf("target %s still faulted after completion", id)
		}
	}
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var b []byte
	for i > 0 {
		b = append([]byte{byte('0' + i%10)}, b...)
		i /= 10
	}
	return string(b)
}
