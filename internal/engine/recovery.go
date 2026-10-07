package engine

import (
	"runtime"

	"github.com/lechandonga/chaos-drill-orchestrator-go/internal/experiment"
)

func yieldScheduler() {
	for range 3 {
		runtime.Gosched()
	}
}

// recoverAll reconciles WAL snapshots with the live registry after restart.
//
// Injected faults live inside the (in-memory) controlled registry, so tests
// that simulate a crash hand the same registry to the new engine — the
// registry plays the role of the external system whose state survives a
// controller restart. Rules:
//
//   - terminal runs are kept as-is (cleanup already completed or the run was
//     explicitly failed for exhausted retries);
//   - non-terminal runs with injected targets enter Recovering -> Cleaning
//     and all their faults are reverted (safe "undo rather than continue"
//     choice); ownership is reclaimed for the duration of cleanup;
//   - non-terminal runs with no injection yet are resumed: ownership is
//     reclaimed and a fresh execution goroutine starts from injection.
func (e *Engine) recoverAll() error {
	for _, run := range e.runs {
		if run.Phase.Terminal() {
			continue
		}

		e.mu.Lock()
		for _, ts := range run.Targets {
			if ts.Injected {
				e.owner[ts.ID] = run.Spec.ID
			}
		}
		ch := make(chan struct{}, 1)
		e.abortCh[run.Spec.ID] = ch
		done := make(chan struct{})
		e.doneCh[run.Spec.ID] = done
		e.mu.Unlock()

		hadInjection := len(run.InjectedIDs) > 0
		if hadInjection {
			run.Phase = experiment.PhaseRecovering
			// Recovery always chooses full cleanup: a half-applied drill is
			// never continued blindly. The run ends Completed when every
			// injected target is restored, Failed only if retries run out.
			run.EndReason = ""
			e.log.Warn("recovering experiment: reverting injected faults",
				"experiment", run.Spec.ID, "injected", run.InjectedIDs)
			e.cleanAll(run)
			e.finish(run)
			e.releaseTargets(run.Spec.ID)
			continue
		}

		e.log.Info("recovering experiment: resuming before injection",
			"experiment", run.Spec.ID)
		run.Phase = experiment.PhasePending
		now := e.clk.Now()
		run.StartedAt = now
		run.EndsAt = now.Add(run.Spec.Duration)
		if err := e.persist(run); err != nil {
			return err
		}
		run.Phase = experiment.PhaseInjecting
		e.wg.Add(1)
		go func(r *experiment.Run, c chan struct{}) {
			defer e.wg.Done()
			defer close(done)
			e.execute(r, c)
			e.releaseTargets(r.Spec.ID)
		}(run, ch)
	}
	return nil
}
