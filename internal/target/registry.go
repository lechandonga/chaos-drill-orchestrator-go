// Package target implements in-process "controlled targets" standing in for
// real services. A target carries metrics a probe can read and at most one
// active injected fault. Everything lives in memory and is safe for
// concurrent use; no network or real process is involved.
package target

import (
	"errors"
	"fmt"
	"sync"
)

// ErrNotFound is returned when an experiment names a target the registry
// does not know about.
var ErrNotFound = errors.New("target not found")

// ErrFaultActive is returned by Inject when the target already carries a
// fault. The engine normally prevents this through its conflict policy;
// seeing it means a conflict slipped through and is surfaced explicitly.
var ErrFaultActive = errors.New("target already has an active fault")

// Fault describes what is injected into a target.
type Fault struct {
	Kind      string
	Intensity int
}

// Target is a single controlled service instance.
type Target struct {
	ID       string
	baseline map[string]float64

	mu        sync.Mutex
	active    *Fault
	history   []Fault
	probeHook func(metric string, base float64) float64
}

// Registry is the shared catalogue of controlled targets.
type Registry struct {
	mu sync.Mutex // only guards the maps below; per-target state has its own lock

	targets map[string]*Target

	// revertFailures makes the next n Revert calls on a given target fail,
	// simulating flaky cleanup so tests can exercise per-target retries.
	revertFailures map[string]int
}

func NewRegistry() *Registry {
	return &Registry{
		targets:        make(map[string]*Target),
		revertFailures: make(map[string]int),
	}
}

// Register creates a target with baseline probe values.
func (r *Registry) Register(id string, baseline map[string]float64) *Target {
	r.mu.Lock()
	defer r.mu.Unlock()
	cp := make(map[string]float64, len(baseline))
	for k, v := range baseline {
		cp[k] = v
	}
	t := &Target{ID: id, baseline: cp}
	r.targets[id] = t
	return t
}

// Get returns ErrNotFound for unknown ids rather than nil-and-ok, so callers
// cannot accidentally proceed with a nil target.
func (r *Registry) Get(id string) (*Target, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	t, ok := r.targets[id]
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrNotFound, id)
	}
	return t, nil
}

// Exists reports whether id is registered (used by admission validation).
func (r *Registry) Exists(id string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	_, ok := r.targets[id]
	return ok
}

// IDs returns the sorted? no — unsorted snapshot of registered ids.
func (r *Registry) IDs() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	ids := make([]string, 0, len(r.targets))
	for id := range r.targets {
		ids = append(ids, id)
	}
	return ids
}

// FailNextReverts arranges for the next n Revert calls on id to return an
// error. The fault stays active on failure so a later retry can succeed.
func (r *Registry) FailNextReverts(id string, n int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.revertFailures[id] += n
	if r.revertFailures[id] <= 0 {
		delete(r.revertFailures, id)
	}
}

func (r *Registry) consumeRevertFailure(id string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	n := r.revertFailures[id]
	if n <= 0 {
		return false
	}
	if n == 1 {
		delete(r.revertFailures, id)
	} else {
		r.revertFailures[id] = n - 1
	}
	return true
}

// Inject applies f to the target. A second fault while one is active is an
// error: faults never silently overwrite each other.
func (t *Target) Inject(f Fault) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.active != nil {
		return fmt.Errorf("%w: target %q carries %s", ErrFaultActive, t.ID, t.active.Kind)
	}
	fc := f
	t.active = &fc
	t.history = append(t.history, f)
	return nil
}

// Revert removes the active fault. It is idempotent: reverting a target with
// no active fault succeeds and returns false, so a duplicated cleanup can
// never report a spurious failure.
func (t *Target) Revert(registry *Registry) (bool, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.active == nil {
		return false, nil
	}
	if registry.consumeRevertFailure(t.ID) {
		return false, fmt.Errorf("simulated revert failure on target %q", t.ID)
	}
	t.active = nil
	return true, nil
}

// ActiveFault returns a copy of the active fault or nil.
func (t *Target) ActiveFault() *Fault {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.active == nil {
		return nil
	}
	fc := *t.active
	return &fc
}

// Probe returns the current value of metric, applying the active fault's
// effect on top of the baseline. Unknown metrics read as 0.
func (t *Target) Probe(metric string) float64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	v := t.baseline[metric]
	if t.active == nil {
		if t.probeHook != nil {
			v = t.probeHook(metric, v)
		}
		return v
	}
	switch t.active.Kind {
	case "latency_delay":
		if metric == "latency_ms" {
			v += float64(t.active.Intensity)
		}
	case "error_injection":
		if metric == "error_rate" {
			v += float64(t.active.Intensity)
		}
	case "cpu_stress":
		if metric == "cpu_pct" {
			v += float64(t.active.Intensity)
		}
	}
	if t.probeHook != nil {
		v = t.probeHook(metric, v)
	}
	return v
}

// SetProbeHook installs a transient observation transform used to simulate
// metric jitter or external degradation in tests. Pass nil to remove it.
func (t *Target) SetProbeHook(fn func(metric string, base float64) float64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.probeHook = fn
}
