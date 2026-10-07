package engine

import (
	"context"
	"testing"
	"time"

	"github.com/lechandonga/chaos-drill-orchestrator-go/internal/clock"
	"github.com/lechandonga/chaos-drill-orchestrator-go/internal/experiment"
)

// TestSteadyAbortConsecutive verifies that jitter does not abort: two breach
// rounds out of the required three streak must reset, while three
// consecutive breach rounds trigger exactly one automatic abort and full
// cleanup.
func TestSteadyAbortConsecutive(t *testing.T) {
	clk := clock.NewFake(time.Unix(1000, 0))
	reg := newRegistry()
	e := newTestEngine(t, reg, newWAL(t), clk, ConflictReject)

	spec := latencySpec("steady", []string{"svc-a"}, 50, 1000, 1)
	if err := e.Start(context.Background(), spec); err != nil {
		t.Fatal(err)
	}
	tgt, _ := reg.Get("svc-a")

	// Wait for injection; injected latency 20+50=70 < 200, healthy.
	eventually(t, func() bool { return tgt.ActiveFault() != nil })

	// Observation script consumed strictly in probe order: breach, breach,
	// healthy (streak resets), then three consecutive breaches which trip the
	// automatic abort. The exhausted tail stays healthy.
	script := newScriptedProbe(30, 500, 500, 30, 500, 500, 500)
	tgt.SetProbeHook(script.hook)

	for range 10 {
		if stepUntil(t, clk, 10*time.Millisecond, func() bool {
			rr, _ := e.Status("steady")
			return rr.Phase.Terminal()
		}) {
			break
		}
	}

	r, _ := e.Status("steady")
	if !r.Aborted || r.EndReason != experiment.EndSteadyAbort {
		t.Fatalf("want steady_violation abort, got aborted=%v reason=%s (probes=%d streak=%d)",
			r.Aborted, r.EndReason, script.count(), r.Consecutive)
	}
	if r.Phase != experiment.PhaseCompleted {
		t.Fatalf("phase = %s, want completed after cleanup", r.Phase)
	}
	if tgt.ActiveFault() != nil {
		t.Fatal("fault left active after automatic abort")
	}
}

// TestAbortOnce verifies manual abort is a single-shot, idempotent signal
// and a concurrent duplicate abort cannot cause a second cleanup pass or
// flip the end reason.
func TestAbortOnce(t *testing.T) {
	clk := clock.NewFake(time.Unix(1000, 0))
	reg := newRegistry()
	e := newTestEngine(t, reg, newWAL(t), clk, ConflictReject)

	spec := latencySpec("manual", []string{"svc-a", "svc-b"}, 50, 1000, 2)
	if err := e.Start(context.Background(), spec); err != nil {
		t.Fatal(err)
	}
	tgt, _ := reg.Get("svc-a")
	eventually(t, func() bool { return tgt.ActiveFault() != nil })

	for range 5 {
		if err := e.Abort("manual"); err != nil && err != errAlreadyAborted {
			t.Fatalf("abort: %v", err)
		}
	}

	eventually(t, func() bool {
		r, _ := e.Status("manual")
		return r.Phase.Terminal()
	})

	r, _ := e.Status("manual")
	if r.EndReason != experiment.EndManualAbort {
		t.Fatalf("reason = %s, want manual_abort", r.EndReason)
	}
	for _, id := range []string{"svc-a", "svc-b"} {
		tg, _ := reg.Get(id)
		if tg.ActiveFault() != nil {
			t.Fatalf("target %s left faulted after abort", id)
		}
		ts := r.Targets[id]
		if !ts.Reverted || ts.RevertAttempt != 1 {
			t.Fatalf("target %s revert state = %+v, want single clean revert", id, ts)
		}
	}
}

// TestHaltTrip verifies the tighter halt trip-wire also requires consecutive
// breaches and records its own end reason.
func TestHaltTrip(t *testing.T) {
	clk := clock.NewFake(time.Unix(1000, 0))
	reg := newRegistry()
	e := newTestEngine(t, reg, newWAL(t), clk, ConflictReject)

	spec := latencySpec("halt", []string{"svc-a"}, 50, 1000, 1)
	spec.Halt = &experiment.Threshold{Metric: "latency_ms", Op: "gt", Value: 150}
	if err := e.Start(context.Background(), spec); err != nil {
		t.Fatal(err)
	}
	tgt, _ := reg.Get("svc-a")
	eventually(t, func() bool { return tgt.ActiveFault() != nil })

	// 160 trips halt (latency > 150) but stays under steady (> 200).
	tgt.SetProbeHook(func(string, float64) float64 { return 160 })
	for range 6 {
		if stepUntil(t, clk, 10*time.Millisecond, func() bool {
			rr, _ := e.Status("halt")
			return rr.Phase.Terminal()
		}) {
			break
		}
	}
	eventually(t, func() bool {
		r, _ := e.Status("halt")
		return r.Phase.Terminal()
	})
	r, _ := e.Status("halt")
	if r.EndReason != experiment.EndHaltTrip {
		t.Fatalf("reason = %s, want halt_tripped", r.EndReason)
	}
}
