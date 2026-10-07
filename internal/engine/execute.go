package engine

import (
	"fmt"
	"sync"
	"time"

	"github.com/lechandonga/chaos-drill-orchestrator-go/internal/experiment"
	"github.com/lechandonga/chaos-drill-orchestrator-go/internal/target"
)

// execute is the full lifecycle for one admitted experiment:
//
//  1. inject in waves capped by Spec.BlastRadius (engine-enforced),
//  2. observe steady/halt invariants every ObservationInterval, requiring
//     BreachConsecutive consecutive breach rounds before aborting,
//  3. on duration end or abort, revert every injected target independently
//     with bounded retries; one target's cleanup failure never blocks
//     another's.
func (e *Engine) execute(run *experiment.Run, abortCh <-chan struct{}) {
	spec := run.Spec

	if !e.enterPhase(run, experiment.PhaseRunning) {
		// Manual abort raced ahead of execution: clean whatever exists.
		e.cleanAll(run)
		e.finish(run)
		return
	}

	stopReason := e.runWaves(run, abortCh)
	if e.killed() {
		// Hard termination: do not clean or overwrite state. The persisted
		// snapshot stays authoritative; restart recovery reverts faults.
		return
	}

	lk := e.runLock(spec.ID)
	lk.Lock()
	switch stopReason {
	case experiment.EndSteadyAbort, experiment.EndHaltTrip:
		if !run.AbortOnce {
			run.AbortOnce = true
			run.Aborted = true
			run.EndReason = stopReason
		}
	case experiment.EndNormal:
		if run.EndReason == "" {
			run.EndReason = experiment.EndNormal
		}
	}
	lk.Unlock()
	_ = e.persist(run)

	e.log.Info("experiment stopping; cleaning targets",
		"experiment", spec.ID, "reason", stopReason, "injected", len(run.InjectedIDs))

	e.cleanAll(run)

	e.finish(run)
}

// runWaves orchestrates sequential blast-radius waves: inject a wave of at
// most spec.BlastRadius targets, observe it for a slice of the declared
// duration, revert it, then move to the next wave. Consequently the number of
// simultaneously faulted targets can never exceed the blast radius, which was
// itself bounded by the engine hard cap during admission.
func (e *Engine) runWaves(run *experiment.Run, abortCh <-chan struct{}) experiment.EndReason {
	spec := run.Spec
	ids := spec.SortedTargetIDs()

	for start := 0; start < len(ids); start += spec.BlastRadius {
		end := start + spec.BlastRadius
		if end > len(ids) {
			end = len(ids)
		}
		wave := ids[start:end]

		if err := e.injectWave(run, wave, start/spec.BlastRadius); err != nil {
			e.log.Error("injection failed", "experiment", spec.ID, "err", err)
			return experiment.EndManualAbort
		}

		reason := e.observeWave(run, wave, abortCh)

		if e.killed() {
			// The process is terminating hard: do not clean up here; the
			// next incarnation reconciles injected faults from the WAL.
			return experiment.EndManualAbort
		}

		// Each wave is reverted before the next begins; this also makes the
		// steady-state window per wave explicit.
		e.cleanTargets(run, wave)
		_ = e.persist(run)

		if reason != experiment.EndNormal {
			return reason
		}
	}
	return experiment.EndNormal
}

func (e *Engine) injectWave(run *experiment.Run, wave []string, waveIndex int) error {
	spec := run.Spec
	for _, id := range wave {
		t, err := e.registry.Get(id)
		if err != nil {
			return err
		}
		if err := t.Inject(target.Fault{Kind: spec.FaultKind, Intensity: spec.Intensity}); err != nil {
			// Defensive: ownership table should have prevented contention.
			return fmt.Errorf("inject %s: %w", id, err)
		}
		lk := e.runLock(spec.ID)
		lk.Lock()
		run.MarkInjected(id, e.clk.Now())
		lk.Unlock()
		if err := e.persist(run); err != nil {
			return err
		}
		e.log.Info("fault injected", "experiment", spec.ID, "target", id, "wave_index", waveIndex)
	}
	return nil
}

// enterPhase moves injecting -> running; if a manual abort raced ahead of
// injection the run is already marked aborted and observation is skipped.
func (e *Engine) enterPhase(run *experiment.Run, phase experiment.Phase) bool {
	lk := e.runLock(run.Spec.ID)
	lk.Lock()
	defer lk.Unlock()
	if run.Aborted && run.EndReason == experiment.EndManualAbort {
		return false
	}
	run.Phase = phase
	run.UpdatedAt = e.clk.Now()
	_ = e.wal.Append(run)
	return true
}

// observeWave polls one wave until its slice of the duration elapses, a
// manual abort arrives, or the steady/halt invariant is breached for the
// required number of consecutive rounds. A single jittery round resets the
// streak: transient noise cannot trigger an abort.
func (e *Engine) observeWave(run *experiment.Run, wave []string, abortCh <-chan struct{}) experiment.EndReason {
	spec := run.Spec
	totalWaves := (len(spec.TargetIDs) + spec.BlastRadius - 1) / spec.BlastRadius
	waveSlice := spec.Duration / time.Duration(totalWaves)
	waveDeadline := e.clk.Now().Add(waveSlice)

	for {

		remaining := waveDeadline.Sub(e.clk.Now())
		if remaining <= 0 {
			return experiment.EndNormal
		}
		wait := e.cfg.ObservationInterval
		if wait > remaining {
			wait = remaining
		}
		timer := e.clk.NewTimer(wait)
		select {
		case <-abortCh:
			timer.Stop()
			return experiment.EndManualAbort
		case <-timer.C():
		case <-e.stopChan():
			// Hard process-kill semantics: stop immediately without cleanup;
			// crash recovery owns the injected faults after restart.
			timer.Stop()
			return experiment.EndManualAbort
		}

		if !e.clk.Now().Before(waveDeadline) {
			return experiment.EndNormal
		}

		breach, halt := e.probeTargets(run, wave)
		lk := e.runLock(spec.ID)
		lk.Lock()
		// Either the steady boundary or the tighter halt trip-wire advances
		// the consecutive streak; a fully healthy round resets it. Halt is
		// reported only when the halt wire (not merely steady) was crossed.
		if breach || halt {
			run.Consecutive++
		} else {
			run.Consecutive = 0
		}
		trip := run.Consecutive >= spec.BreachConsecutive
		lk.Unlock()

		if breach || halt {
			e.log.Info("steady breach observed", "experiment", spec.ID,
				"consecutive", run.Consecutive, "required", spec.BreachConsecutive, "halt", halt)
		}
		if trip {
			if halt {
				return experiment.EndHaltTrip
			}
			return experiment.EndSteadyAbort
		}
	}
}

// probeTargets checks the given wave. The round is a breach if any target
// violates steady; halt is true if any target crosses the tighter trip-wire.
func (e *Engine) probeTargets(run *experiment.Run, ids []string) (breach bool, halt bool) {
	spec := run.Spec
	for _, id := range ids {
		t, err := e.registry.Get(id)
		if err != nil {
			continue
		}
		v := t.Probe(spec.Steady.Metric)
		if spec.Steady.Breached(v) {
			breach = true
		}
		if spec.Halt != nil && spec.Halt.Breached(t.Probe(spec.Halt.Metric)) {
			halt = true
		}
	}
	return
}

// cleanAll reverts every injected target concurrently. Each target is
// retried independently up to MaxRevertAttempts; a permanently failing target
// is recorded on its TargetState while the rest finish successfully.
func (e *Engine) cleanAll(run *experiment.Run) {
	if e.setPhase(run, experiment.PhaseCleaning) {
		_ = e.persist(run)
	}

	e.cleanTargets(run, append([]string(nil), run.InjectedIDs...))
	_ = e.persist(run)
}

// cleanTargets reverts the given targets concurrently and independently.
// Each target is retried up to MaxRevertAttempts; one target's cleanup
// failure never blocks another's.
func (e *Engine) cleanTargets(run *experiment.Run, ids []string) {
	var wg sync.WaitGroup
	for _, id := range ids {
		wg.Add(1)
		go func(id string) {
			defer wg.Done()
			e.cleanOne(run, id)
		}(id)
	}
	wg.Wait()
}

// cleanOne retries one target's revert with backoff. Revert is idempotent at
// the target level (second revert is a successful no-op), so a retry after a
// response-lost crash or a duplicated cleanup cannot double-undo.
func (e *Engine) cleanOne(run *experiment.Run, id string) {
	for attempt := 1; attempt <= e.cfg.MaxRevertAttempts; attempt++ {
		t, err := e.registry.Get(id)
		changed := false
		if err == nil {
			changed, err = t.Revert(e.registry)
		}

		lk := e.runLock(run.Spec.ID)
		lk.Lock()
		ts := run.Targets[id]
		if changed {
			ts.RevertAttempt = attempt
		}
		if err != nil {
			ts.LastError = err.Error()
			lk.Unlock()
			e.log.Warn("revert failed; will retry", "experiment", run.Spec.ID,
				"target", id, "attempt", attempt, "err", err)
			_ = e.persist(run) // persist takes the run lock itself
			if attempt < e.cfg.MaxRevertAttempts {
				timer := e.clk.NewTimer(e.cfg.RevertRetryInterval)
				<-timer.C()
			}
			continue
		}

		run.MarkReverted(id, e.clk.Now())
		lk.Unlock()
		e.log.Info("target restored", "experiment", run.Spec.ID,
			"target", id, "attempts", attempt)
		return
	}
	e.log.Error("target revert exhausted retries",
		"experiment", run.Spec.ID, "target", id, "attempts", e.cfg.MaxRevertAttempts)
}

func (e *Engine) setPhase(run *experiment.Run, phase experiment.Phase) bool {
	lk := e.runLock(run.Spec.ID)
	lk.Lock()
	defer lk.Unlock()
	if run.Phase == phase {
		return false
	}
	run.Phase = phase
	run.UpdatedAt = e.clk.Now()
	_ = e.wal.Append(run)
	return true
}

// finish marks the terminal phase. Completed requires every injected target
// restored. If anything is still dirty the run is kept non-terminal as
// Failed with an explicit reason, and RetryCleanup can be driven later —
// dangling faults are never reported as success.
func (e *Engine) finish(run *experiment.Run) {
	lk := e.runLock(run.Spec.ID)
	lk.Lock()
	if run.AllReverted() {
		run.Phase = experiment.PhaseCompleted
		if run.EndReason == "" {
			run.EndReason = experiment.EndNormal
		}
	} else {
		run.Phase = experiment.PhaseFailed
		run.EndReason = experiment.EndRevertRetryEx
	}
	run.FinishedAt = e.clk.Now()
	run.UpdatedAt = run.FinishedAt
	lk.Unlock()
	_ = e.persist(run)
	e.log.Info("experiment finished", "experiment", run.Spec.ID,
		"phase", run.Phase, "reason", run.EndReason, "aborted", run.Aborted)
}

// RetryCleanup re-attempts revert for targets still marked dirty after an
// earlier cleanup run. It is safe to call repeatedly.
func (e *Engine) RetryCleanup(experimentID string) error {
	e.mu.Lock()
	run := e.runs[experimentID]
	e.mu.Unlock()
	if run == nil {
		return fmt.Errorf("experiment %q not found", experimentID)
	}
	var dirty []string
	lk := e.runLock(experimentID)
	lk.Lock()
	activeCleanup := !run.Phase.Terminal()
	for _, ts := range run.Targets {
		if ts.Injected && !ts.Reverted {
			dirty = append(dirty, ts.ID)
		}
	}
	lk.Unlock()
	if activeCleanup {
		// The owning execution goroutine is still running its own cleanup;
		// driving a second cleanup concurrently would race on the run state.
		return fmt.Errorf("experiment %q cleanup is still in progress", experimentID)
	}
	if len(dirty) == 0 {
		return nil
	}
	// Wait for the owning goroutine to fully release the run before we take
	// over cleanup, so two finish() calls can never overlap.
	e.waitDone(experimentID)

	var wg sync.WaitGroup
	for _, id := range dirty {
		wg.Add(1)
		go func(id string) {
			defer wg.Done()
			e.cleanOne(run, id)
		}(id)
	}
	wg.Wait()
	e.finish(run)
	return nil
}
