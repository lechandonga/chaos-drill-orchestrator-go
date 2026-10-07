package engine

import (
	"context"
	"testing"
	"time"

	"github.com/lechandonga/chaos-drill-orchestrator-go/internal/clock"
	"github.com/lechandonga/chaos-drill-orchestrator-go/internal/experiment"
)

// TestCrashRecoveryWithInjectedFaults: the "process" is killed after faults
// were injected (engine dropped, cleanup never runs), while the registry
// keeps the dangling faults and the WAL keeps the snapshots. Restart
// replays the WAL and reverts every dangling fault.
func TestCrashRecoveryWithInjectedFaults(t *testing.T) {
	clk := clock.NewFake(time.Unix(1000, 0))
	reg := newRegistry()
	dir := t.TempDir()

	wal := newWALAt(t, dir)
	e, err := New(testConfig(ConflictReject), reg, wal, clk, testLogger(t))
	if err != nil {
		t.Fatal(err)
	}

	spec := latencySpec("crash", []string{"svc-a", "svc-b", "svc-c"}, 50, 1000, 3)
	if err := e.Start(context.Background(), spec); err != nil {
		t.Fatal(err)
	}
	// Wait until the whole blast (all three targets) is injected, so the
	// crash snapshot records the complete set of dangling faults.
	eventually(t, func() bool {
		r, _ := e.Status("crash")
		return len(r.InjectedIDs) == len(spec.TargetIDs)
	})
	e.StopWithoutWait()
	e.WaitIdle()
	_ = wal.Close()

	// Restart with shared registry (external state) and replayed WAL.
	wal2 := newWALAt(t, dir)
	recovered, err := New(testConfig(ConflictReject), reg, wal2, clk, testLogger(t))
	if err != nil {
		t.Fatalf("restart: %v", err)
	}
	t.Cleanup(recovered.Close)

	eventually(t, func() bool {
		r, err := recovered.Status("crash")
		return err == nil && r.Phase.Terminal()
	})

	r, _ := recovered.Status("crash")
	if r.Phase != experiment.PhaseCompleted {
		t.Fatalf("recovered phase = %s, want completed", r.Phase)
	}
	for _, id := range spec.TargetIDs {
		tg, _ := reg.Get(id)
		if tg.ActiveFault() != nil {
			t.Fatalf("target %s left dangling after recovery", id)
		}
	}
}

// TestCrashRecoveryBeforeInjection resumes a run that crashed before any
// fault was injected: the experiment safely continues to completion.
func TestCrashRecoveryBeforeInjection(t *testing.T) {
	clk := clock.NewFake(time.Unix(2000, 0))
	reg := newRegistry()
	dir := t.TempDir()

	run := experiment.NewRun(latencySpec("early", []string{"svc-a"}, 50, 100, 1), clk.Now())
	wal := newWALAt(t, dir)
	if err := wal.Append(run); err != nil {
		t.Fatal(err)
	}
	_ = wal.Close()

	wal2 := newWALAt(t, dir)
	e2, err := New(testConfig(ConflictReject), reg, wal2, clk, testLogger(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(e2.Close)

	waitParked(t, clk, 1)
	clk.Advance(150 * time.Millisecond)
	eventually(t, func() bool {
		r, err := e2.Status("early")
		return err == nil && r.Phase == experiment.PhaseCompleted
	})
	tgt, _ := reg.Get("svc-a")
	if tgt.ActiveFault() != nil {
		t.Fatal("fault leaked after resumed experiment completed")
	}
}

// TestRevertFailureRetryAndIdempotency: flaky reverts are retried per target
// up to the attempt limit; healthy targets finish independently; duplicate
// reverts are successful no-ops.
func TestRevertFailureRetryAndIdempotency(t *testing.T) {
	clk := clock.NewFake(time.Unix(3000, 0))
	reg := newRegistry()
	reg.FailNextReverts("svc-a", 2) // fails twice, third attempt succeeds

	e := newTestEngine(t, reg, newWAL(t), clk, ConflictReject)
	spec := latencySpec("flaky", []string{"svc-a", "svc-b"}, 50, 100, 2)
	if err := e.Start(context.Background(), spec); err != nil {
		t.Fatal(err)
	}

	tgtA, _ := reg.Get("svc-a")
	tgtB, _ := reg.Get("svc-b")
	eventually(t, func() bool { return tgtA.ActiveFault() != nil && tgtB.ActiveFault() != nil })

	waitParked(t, clk, 1)
	for range 40 {
		if stepUntil(t, clk, 10*time.Millisecond, func() bool {
			rr, _ := e.Status("flaky")
			return rr.Phase.Terminal()
		}) {
			break
		}
	}
	eventually(t, func() bool {
		r, _ := e.Status("flaky")
		return r.Phase.Terminal()
	})

	r, _ := e.Status("flaky")
	if r.Phase != experiment.PhaseCompleted {
		t.Fatalf("phase = %s, want completed after retries", r.Phase)
	}
	if ts := r.Targets["svc-a"]; ts.RevertAttempt != 3 || !ts.Reverted {
		t.Fatalf("svc-a = %+v, want 3 attempts then reverted", ts)
	}
	if ts := r.Targets["svc-b"]; ts.RevertAttempt != 1 || !ts.Reverted {
		t.Fatalf("svc-b = %+v, want 1 clean attempt", ts)
	}

	ok, err := tgtA.Revert(reg)
	if err != nil || ok {
		t.Fatalf("duplicate revert = (%v,%v), want (false,nil)", ok, err)
	}
	if tgtA.ActiveFault() != nil || tgtB.ActiveFault() != nil {
		t.Fatal("fault still active after completion")
	}
	if err := e.RetryCleanup("flaky"); err != nil {
		t.Fatalf("retry of clean run should be no-op: %v", err)
	}
}

// TestRevertExhaustedThenManualRetry: a permanently failing target marks the
// run Failed without blocking other targets; after the failure clears,
// RetryCleanup finishes just that target and completes the run.
func TestRevertExhaustedThenManualRetry(t *testing.T) {
	clk := clock.NewFake(time.Unix(4000, 0))
	reg := newRegistry()
	reg.FailNextReverts("svc-a", 10)

	e := newTestEngine(t, reg, newWAL(t), clk, ConflictReject)
	spec := latencySpec("stuck", []string{"svc-a", "svc-b"}, 50, 50, 2)
	if err := e.Start(context.Background(), spec); err != nil {
		t.Fatal(err)
	}

	// 100ms wave observation + up to 2 retry gaps of 5ms; step under parked
	// synchronization so each tick is observed exactly once.
	for range 30 {
		if stepUntil(t, clk, 10*time.Millisecond, func() bool {
			r, _ := e.Status("stuck")
			return r.Phase.Terminal()
		}) {
			break
		}
	}
	eventually(t, func() bool {
		r, _ := e.Status("stuck")
		return r.Phase.Terminal()
	})

	r, _ := e.Status("stuck")
	if r.Phase != experiment.PhaseFailed || r.EndReason != experiment.EndRevertRetryEx {
		t.Fatalf("phase=%s reason=%s, want failed/revert_retry_exhausted", r.Phase, r.EndReason)
	}
	if ts := r.Targets["svc-b"]; !ts.Reverted {
		t.Fatal("healthy target svc-b dragged down by svc-a cleanup failure")
	}
	if ts := r.Targets["svc-a"]; ts.Reverted || ts.LastError == "" {
		t.Fatalf("svc-a should be dirty with an error, got %+v", ts)
	}

	reg.FailNextReverts("svc-a", -10) // operator clears the simulated failure
	if err := e.RetryCleanup("stuck"); err != nil {
		t.Fatal(err)
	}
	r, _ = e.Status("stuck")
	if r.Phase != experiment.PhaseCompleted {
		t.Fatalf("after retry phase = %s, want completed", r.Phase)
	}
	tgt, _ := reg.Get("svc-a")
	if tgt.ActiveFault() != nil {
		t.Fatal("svc-a fault survived successful retry")
	}
}
