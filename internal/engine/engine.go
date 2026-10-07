// Package engine is the local fault-drill orchestration engine. It owns
// admission, enforced blast-radius caps, conflict policy across concurrent
// experiments, steady-state observation with automatic abort, idempotent
// per-target cleanup with retries, and crash recovery from a write-ahead log.
package engine

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/lechandonga/chaos-drill-orchestrator-go/internal/clock"
	"github.com/lechandonga/chaos-drill-orchestrator-go/internal/experiment"
	"github.com/lechandonga/chaos-drill-orchestrator-go/internal/store"
	"github.com/lechandonga/chaos-drill-orchestrator-go/internal/target"
)

// ConflictPolicy decides what happens when two experiments need the same
// target while one still has a fault injected there.
type ConflictPolicy string

const (
	// ConflictReject refuses the new experiment immediately. A fault is
	// never overwritten by a later writer.
	ConflictReject ConflictPolicy = "reject"
	// ConflictQueue makes the new experiment wait in FIFO order until the
	// target is released, or until its context is cancelled.
	ConflictQueue ConflictPolicy = "queue"
)

// ConflictCode marks a refused start due to target contention.
const ConflictCode experiment.RejectCode = "TARGET_CONFLICT"

// Config holds engine-wide safety parameters.
type Config struct {
	HardBlastCap        int           // engine-forced upper bound on concurrent affected targets
	ObservationInterval time.Duration // probe cadence while a wave is active
	MaxRevertAttempts   int           // per-target cleanup attempts before it is flagged
	RevertRetryInterval time.Duration // delay between cleanup retries
	Conflict            ConflictPolicy
}

func DefaultConfig() Config {
	return Config{
		HardBlastCap:        3,
		ObservationInterval: 100 * time.Millisecond,
		MaxRevertAttempts:   3,
		RevertRetryInterval: 50 * time.Millisecond,
		Conflict:            ConflictReject,
	}
}

type waiter struct {
	id       string
	targets  map[string]bool
	sequence int
	aborted  bool
	wake     chan struct{} // replaced for every park under e.mu
}

var errAlreadyAborted = fmt.Errorf("experiment already aborted")

// Engine runs experiments against an in-process target registry.
type Engine struct {
	cfg      Config
	registry *target.Registry
	wal      *store.WAL
	clk      clock.Clock
	log      *slog.Logger

	mu      sync.Mutex
	runs    map[string]*experiment.Run
	owner   map[string]string // targetID -> experimentID currently allowed to inject
	runMu   map[string]*sync.Mutex
	waiters []*waiter
	ticket  int
	abortCh map[string]chan struct{}
	stopCh  chan struct{} // closed to model hard process termination
	doneCh  map[string]chan struct{}
	wg      sync.WaitGroup
	closed  bool
}

// New constructs an engine and replays the WAL. Recovered experiments are
// either safely resumed (nothing injected yet) or reconciled into cleanup
// (faults already injected are reverted), so no target is left dangling.
func New(cfg Config, reg *target.Registry, wal *store.WAL, clk clock.Clock, log *slog.Logger) (*Engine, error) {
	if cfg.HardBlastCap < 1 {
		return nil, fmt.Errorf("HardBlastCap must be >= 1")
	}
	if cfg.Conflict == "" {
		cfg.Conflict = ConflictReject
	}
	e := &Engine{
		cfg:      cfg,
		registry: reg,
		wal:      wal,
		clk:      clk,
		log:      log,
		runs:     make(map[string]*experiment.Run),
		owner:    make(map[string]string),
		runMu:    make(map[string]*sync.Mutex),
		abortCh:  make(map[string]chan struct{}),
		stopCh:   make(chan struct{}),
		doneCh:   make(map[string]chan struct{}),
	}

	recovered, err := wal.Replay()
	if err != nil {
		return nil, err
	}
	for _, run := range recovered {
		e.runs[run.Spec.ID] = run
		e.runMu[run.Spec.ID] = &sync.Mutex{}
	}
	if err := e.recoverAll(); err != nil {
		return nil, err
	}
	return e, nil
}

// Validate runs admission without starting anything.
func (e *Engine) Validate(s experiment.Spec) error {
	return s.Validate(e.registry.Exists, e.cfg.HardBlastCap)
}

// Start admits an experiment and, after acquiring its targets under the
// conflict policy, launches its execution goroutine. Under ConflictQueue the
// call blocks (FIFO) until the targets are free or ctx is cancelled.
func (e *Engine) Start(ctx context.Context, s experiment.Spec) error {
	if err := s.Validate(e.registry.Exists, e.cfg.HardBlastCap); err != nil {
		return err
	}

	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return fmt.Errorf("engine closed")
	}
	if _, exists := e.runs[s.ID]; exists {
		e.mu.Unlock()
		return &experiment.RejectError{Code: "DUPLICATE_EXPERIMENT", Reason: "experiment id already exists: " + s.ID}
	}

	w := &waiter{
		id:       s.ID,
		targets:  make(map[string]bool, len(s.TargetIDs)),
		sequence: e.ticket,
	}
	for _, id := range s.TargetIDs {
		w.targets[id] = true
	}
	e.ticket++

	if err := e.acquireLocked(ctx, w); err != nil {
		e.mu.Unlock()
		return err
	}

	now := e.clk.Now()
	run := experiment.NewRun(s, now)
	run.StartedAt = now
	run.EndsAt = now.Add(s.Duration)
	run.Phase = experiment.PhaseInjecting
	e.runs[s.ID] = run
	e.runMu[s.ID] = &sync.Mutex{}
	ch := make(chan struct{}, 1)
	e.abortCh[s.ID] = ch
	done := make(chan struct{})
	e.doneCh[s.ID] = done
	if e.stopCh == nil {
		e.stopCh = make(chan struct{})
	}
	e.wg.Add(1)
	e.mu.Unlock()

	if err := e.persist(run); err != nil {
		return err
	}
	e.log.Info("experiment admitted and started", "experiment", s.ID, "targets", s.TargetIDs,
		"fault", s.FaultKind, "intensity", s.Intensity, "blast_radius", s.BlastRadius,
		"duration", s.Duration)

	go func() {
		defer e.wg.Done()
		defer close(done)
		e.execute(run, ch)
		e.releaseTargets(s.ID)
	}()
	return nil
}

// acquireLocked implements the conflict policy. Caller holds e.mu.
func (e *Engine) acquireLocked(ctx context.Context, w *waiter) error {
	conflict := func() string {
		// FIFO: only the head of the queue may acquire. A queued newcomer
		// whose targets are free still waits its turn, so order cannot be
		// bypassed by lock scheduling.
		if len(e.waiters) > 0 && e.waiters[0] != w {
			return "not at head of FIFO queue"
		}
		for id := range w.targets {
			if owner, busy := e.owner[id]; busy {
				return "target " + id + " held by " + owner
			}
		}
		return ""
	}

	for {
		if reason := conflict(); reason != "" {
			if e.cfg.Conflict == ConflictReject {
				return &experiment.RejectError{Code: ConflictCode, Reason: reason}
			}
			e.parkWaiter(w)
		} else {
			for id := range w.targets {
				e.owner[id] = w.id
			}
			e.removeWaiter(w)
			return nil
		}

		// parkWaiter installed a fresh channel and queue position under the
		// same lock, so a release cannot be missed.
		e.mu.Unlock()
		select {
		case <-w.wake:
			e.mu.Lock()
		case <-ctx.Done():
			e.mu.Lock()
			w.aborted = true
			e.removeWaiter(w)
			e.mu.Unlock()
			return fmt.Errorf("acquire targets cancelled: %w", ctx.Err())
		}
	}
}

// parkWaiter records w in the FIFO queue and gives it a fresh wake channel.
// A previously closed wake channel (spurious wake after losing contention)
// is replaced, so the next park really blocks until a genuine release.
func (e *Engine) parkWaiter(w *waiter) {
	if !containsWaiter(e.waiters, w) {
		e.waiters = append(e.waiters, w)
	}
	w.wake = make(chan struct{})
}

func containsWaiter(ws []*waiter, w *waiter) bool {
	for _, x := range ws {
		if x == w {
			return true
		}
	}
	return false
}

func (e *Engine) removeWaiter(w *waiter) {
	out := e.waiters[:0]
	for _, x := range e.waiters {
		if x != w {
			out = append(out, x)
		}
	}
	e.waiters = out
}

func (e *Engine) releaseTargets(expID string) {
	e.mu.Lock()
	for id, owner := range e.owner {
		if owner == expID {
			delete(e.owner, id)
		}
	}
	// Only the queue head can acquire; wake it and let it re-evaluate. When
	// it releases it wakes the next head, so the queue drains in FIFO order.
	// Wake every parked waiter. The head acquires; the rest re-evaluate and
	// park again on fresh channels. Closing all channels (rather than only
	// the head) is required because the head might be waiting on targets the
	// releasing experiment did not hold, in which case it should not advance.
	for _, w := range e.waiters {
		close(w.wake)
	}
	e.mu.Unlock()
}

// queuedCount reports how many Start calls are parked in the conflict queue.
// It is intended for deterministic test synchronization (no real sleeps).
func (e *Engine) queuedCount() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return len(e.waiters)
}

// persist writes a snapshot under the run's own mutex. Callers must not
// already hold that mutex.
func (e *Engine) persist(run *experiment.Run) error {
	lk := e.runLock(run.Spec.ID)
	lk.Lock()
	defer lk.Unlock()
	run.UpdatedAt = e.clk.Now()
	return e.wal.Append(run)
}

func (e *Engine) runLock(id string) *sync.Mutex {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.runMu[id]
}

// Abort requests a single abort of a running experiment. It is idempotent in
// the sense that only the first call wins; later calls report no-op.
func (e *Engine) Abort(id string) error {
	e.mu.Lock()
	run := e.runs[id]
	ch := e.abortCh[id]
	e.mu.Unlock()
	if run == nil {
		return fmt.Errorf("experiment %q not found", id)
	}
	locked := e.runLock(id)
	locked.Lock()
	if run.AbortOnce {
		locked.Unlock()
		return errAlreadyAborted
	}
	run.AbortOnce = true
	run.Aborted = true
	if run.EndReason == "" {
		run.EndReason = experiment.EndManualAbort
	}
	locked.Unlock()
	if ch != nil {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
	_ = e.persist(run)
	e.log.Info("manual abort requested", "experiment", id)
	return nil
}

// Status returns a snapshot copy of one experiment's runtime state.
func (e *Engine) Status(id string) (*experiment.Run, error) {
	e.mu.Lock()
	run := e.runs[id]
	e.mu.Unlock()
	if run == nil {
		return nil, fmt.Errorf("experiment %q not found", id)
	}
	lk := e.runLock(id)
	lk.Lock()
	defer lk.Unlock()
	cp := *run
	return &cp, nil
}

// Close stops accepting experiments and waits for in-flight ones.
func (e *Engine) Close() {
	e.mu.Lock()
	first := !e.closed
	if first {
		e.closed = true
		close(e.stopCh)
	}
	e.mu.Unlock()
	if !first {
		return
	}
	e.wg.Wait()
	_ = e.wal.Close()
}

// stopWithoutWait marks the engine closed and closes the WAL without
// waiting for experiment goroutines. It models a hard process kill in the
// demo: the controller is gone but injected faults survive in the registry.
func (e *Engine) StopWithoutWait() {
	e.mu.Lock()
	if !e.closed {
		e.closed = true
		close(e.stopCh)
	}
	e.mu.Unlock()
	_ = e.wal.Close()
}

// stopChan exposes the termination signal to the execution loop.
func (e *Engine) stopChan() <-chan struct{} {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.stopCh
}

func (e *Engine) killed() bool {
	select {
	case <-e.stopCh:
		return true
	default:
		return false
	}
}

// WaitIdle blocks until every experiment goroutine has stopped. It is used
// after StopWithoutWait to model a process that has fully exited before a
// new engine instance replays its WAL.
func (e *Engine) WaitIdle() { e.wg.Wait() }

// waitDone blocks until the owning goroutine of experiment id fully exits.
func (e *Engine) waitDone(id string) bool {
	e.mu.Lock()
	done := e.doneCh[id]
	e.mu.Unlock()
	if done == nil {
		return false
	}
	<-done
	return true
}
