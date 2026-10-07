package experiment

import (
	"time"
)

// Phase is the state-machine phase of an experiment.
//
//	Pending -> Injecting -> Running -> Cleaning -> Completed
//	                      |          |
//	                      +----------+-- Aborting (steady/halt/manual)
//
// On process restart any non-terminal phase is recovered into Recovering,
// which becomes Cleaning if any fault was already injected, otherwise the
// experiment is safely resumed from Pending.
type Phase string

const (
	PhasePending    Phase = "pending"
	PhaseInjecting  Phase = "injecting"
	PhaseRunning    Phase = "running"
	PhaseAborting   Phase = "aborting"
	PhaseCleaning   Phase = "cleaning"
	PhaseRecovering Phase = "recovering"
	PhaseCompleted  Phase = "completed"
	PhaseFailed     Phase = "failed" // admitted but aborted before/without injection
)

// Terminal reports whether no more transitions are possible.
func (p Phase) Terminal() bool {
	return p == PhaseCompleted || p == PhaseFailed
}

// EndReason explains how an experiment stopped.
type EndReason string

const (
	EndNormal        EndReason = "duration_elapsed"
	EndSteadyAbort   EndReason = "steady_violation"
	EndHaltTrip      EndReason = "halt_tripped"
	EndManualAbort   EndReason = "manual_abort"
	EndRevertRetryEx EndReason = "revert_retry_exhausted"
)

// TargetState tracks one target inside an experiment.
type TargetState struct {
	ID            string `json:"id"`
	Injected      bool   `json:"injected"`
	Reverted      bool   `json:"reverted"`
	RevertAttempt int    `json:"revert_attempts"`
	LastError     string `json:"last_error,omitempty"`
}

// Run is the mutable runtime state persisted to the write-ahead log.
type Run struct {
	Spec        Spec                    `json:"spec"`
	Phase       Phase                   `json:"phase"`
	InjectedIDs []string                `json:"injected_ids"`
	Targets     map[string]*TargetState `json:"targets"`
	Consecutive int                     `json:"consecutive_breaches"`
	Aborted     bool                    `json:"aborted"`
	AbortOnce   bool                    `json:"abort_once"`
	EndReason   EndReason               `json:"end_reason,omitempty"`
	StartedAt   time.Time               `json:"started_at,omitempty"`
	EndsAt      time.Time               `json:"ends_at,omitempty"`
	FinishedAt  time.Time               `json:"finished_at,omitempty"`
	UpdatedAt   time.Time               `json:"updated_at"`
}

// NewRun builds runtime state for an admitted spec.
func NewRun(s Spec, now time.Time) *Run {
	r := &Run{
		Spec:        s,
		Phase:       PhasePending,
		InjectedIDs: nil,
		Targets:     make(map[string]*TargetState, len(s.TargetIDs)),
		UpdatedAt:   now,
	}
	for _, id := range s.SortedTargetIDs() {
		r.Targets[id] = &TargetState{ID: id}
	}
	return r
}

// MarkInjected records a successful injection exactly once per target.
func (r *Run) MarkInjected(id string, now time.Time) {
	ts := r.Targets[id]
	if ts == nil || ts.Injected {
		return
	}
	ts.Injected = true
	r.InjectedIDs = append(r.InjectedIDs, id)
	r.UpdatedAt = now
}

// MarkReverted records a successful cleanup exactly once per target.
func (r *Run) MarkReverted(id string, now time.Time) {
	if ts := r.Targets[id]; ts != nil && ts.Injected && !ts.Reverted {
		ts.Reverted = true
		ts.LastError = ""
		r.UpdatedAt = now
	}
}

// AllReverted is true only when every target that was injected is clean.
func (r *Run) AllReverted() bool {
	for _, ts := range r.Targets {
		if ts.Injected && !ts.Reverted {
			return false
		}
	}
	return true
}
