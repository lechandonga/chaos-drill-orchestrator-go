// Package experiment defines the declarative experiment model, admission
// validation with distinct rejection reasons, and experiment status state.
package experiment

import (
	"fmt"
	"sort"
	"strings"
	"time"
)

// Supported fault kinds and their inclusive intensity bounds. Limits live in
// one place so admission docs, code and error messages cannot drift.
var intensityLimits = map[string][2]int{
	// latency in milliseconds added to the baseline
	"latency_delay": {1, 2000},
	// extra error rate in percent (0..100 scale)
	"error_injection": {1, 50},
	// extra CPU load in percent
	"cpu_stress": {1, 90},
}

// MaxFaultIntensity returns the inclusive upper bound for a fault kind.
func MaxFaultIntensity(kind string) (int, bool) {
	b, ok := intensityLimits[kind]
	return b[1], ok
}

// RejectCode classifies why an experiment was refused. Distinct codes mean a
// caller never has to string-match, and no rejection is hidden behind a
// silently applied default.
type RejectCode string

const (
	RejectUnknownFault        RejectCode = "UNKNOWN_FAULT_KIND"
	RejectIntensityOutOfRange RejectCode = "INTENSITY_OUT_OF_RANGE"
	RejectTargetNotFound      RejectCode = "TARGET_NOT_FOUND"
	RejectEmptyTargets        RejectCode = "EMPTY_TARGET_SCOPE"
	RejectDuplicateTargets    RejectCode = "DUPLICATE_TARGET"
	RejectBadDuration         RejectCode = "BAD_DURATION"
	RejectBlastRadiusZero     RejectCode = "BLAST_RADIUS_ZERO"
	RejectBlastRadiusExceeds  RejectCode = "BLAST_RADIUS_EXCEEDS_HARD_CAP"
	RejectThresholdInvalid    RejectCode = "STEADY_THRESHOLD_INVALID"
	RejectHaltNotTighter      RejectCode = "HALT_NOT_STRICTER_THAN_STEADY"
	RejectUnknownProbeMetric  RejectCode = "UNKNOWN_PROBE_METRIC"
	RejectConsecutiveTooSmall RejectCode = "CONSECUTIVE_BREACHES_BELOW_ONE"
)

// RejectError carries a machine-readable code plus a human explanation.
type RejectError struct {
	Code   RejectCode
	Reason string
}

func (e *RejectError) Error() string { return string(e.Code) + ": " + e.Reason }

func reject(code RejectCode, format string, a ...any) *RejectError {
	return &RejectError{Code: code, Reason: fmt.Sprintf(format, a...)}
}

// Threshold compares one probe metric against a value in a fixed direction.
type Threshold struct {
	Metric string  `json:"metric"`
	Op     string  `json:"op"` // "gt" or "lt"
	Value  float64 `json:"value"`
}

// Spec is the declarative definition of one chaos experiment.
type Spec struct {
	ID          string        `json:"id"`
	TargetIDs   []string      `json:"target_ids"`
	FaultKind   string        `json:"fault_kind"`
	Intensity   int           `json:"intensity"`
	Duration    time.Duration `json:"duration"`
	BlastRadius int           `json:"blast_radius"` // max targets affected at once; engine-enforced

	// Steady: all thresholds must hold; BreachConsecutive consecutive failed
	// observation rounds are required before an abort (jitter protection).
	Steady            Threshold `json:"steady"`
	BreachConsecutive int       `json:"breach_consecutive"`

	// Halt is an optional stricter trip-wire evaluated every round.
	Halt *Threshold `json:"halt,omitempty"`
}

// knownProbeMetrics lists the metrics controlled targets expose.
var knownProbeMetrics = map[string]bool{
	"latency_ms": true,
	"error_rate": true,
	"cpu_pct":    true,
}

// Validate performs admission. targetExists lets the package stay free of an
// import on the target registry; pass Registry.Exists. Every rejection gets a
// distinct RejectCode; nothing invalid is repaired via defaulting.
func (s Spec) Validate(targetExists func(string) bool, hardBlastCap int) error {
	if strings.TrimSpace(s.ID) == "" {
		return reject(RejectThresholdInvalid, "experiment id must not be empty")
	}

	bounds, ok := intensityLimits[s.FaultKind]
	if !ok {
		return reject(RejectUnknownFault, "fault kind %q is not supported", s.FaultKind)
	}
	if s.Intensity < bounds[0] || s.Intensity > bounds[1] {
		return reject(RejectIntensityOutOfRange,
			"intensity %d for %q must be within [%d,%d]", s.Intensity, s.FaultKind, bounds[0], bounds[1])
	}

	if len(s.TargetIDs) == 0 {
		return reject(RejectEmptyTargets, "experiment must declare at least one target")
	}
	seen := make(map[string]bool, len(s.TargetIDs))
	for _, id := range s.TargetIDs {
		if seen[id] {
			return reject(RejectDuplicateTargets, "target %q listed more than once", id)
		}
		seen[id] = true
		if !targetExists(id) {
			return reject(RejectTargetNotFound, "target %q does not exist in the registry", id)
		}
	}

	if s.Duration <= 0 {
		return reject(RejectBadDuration, "duration must be positive, got %s", s.Duration)
	}

	if s.BlastRadius <= 0 {
		return reject(RejectBlastRadiusZero, "blast radius must be >= 1, got %d", s.BlastRadius)
	}
	if s.BlastRadius > hardBlastCap {
		return reject(RejectBlastRadiusExceeds,
			"blast radius %d exceeds engine hard cap %d", s.BlastRadius, hardBlastCap)
	}

	if err := validateThreshold("steady", s.Steady); err != nil {
		return err
	}
	if s.BreachConsecutive < 1 {
		return reject(RejectConsecutiveTooSmall,
			"breach_consecutive must be >= 1, got %d", s.BreachConsecutive)
	}
	if s.Halt != nil {
		if err := validateThreshold("halt", *s.Halt); err != nil {
			return err
		}
		if !haltIsStricter(s.Steady, *s.Halt) {
			return reject(RejectHaltNotTighter,
				"halt condition %s must be strictly tighter than steady %s",
				s.Halt, s.Steady)
		}
	}
	return nil
}

func validateThreshold(name string, th Threshold) error {
	if !knownProbeMetrics[th.Metric] {
		return reject(RejectUnknownProbeMetric,
			"%s threshold references unknown metric %q", name, th.Metric)
	}
	if th.Op != "gt" && th.Op != "lt" {
		return reject(RejectThresholdInvalid,
			"%s threshold op must be \"gt\" or \"lt\", got %q", name, th.Op)
	}
	return nil
}

// haltIsStricter encodes the contradiction check: the halt trip-wire must
// trip on the same metric before the steady boundary is crossed.
func haltIsStricter(steady, halt Threshold) bool {
	if halt.Metric != steady.Metric || halt.Op != steady.Op {
		return false
	}
	switch steady.Op {
	case "gt":
		return halt.Value < steady.Value
	case "lt":
		return halt.Value > steady.Value
	}
	return false
}

// Breached reports whether measured value violates the threshold. Steady
// invariants are stated as "must hold"; a breach means the invariant fails.
func (th Threshold) Breached(value float64) bool {
	switch th.Op {
	case "gt":
		return value > th.Value
	case "lt":
		return value < th.Value
	}
	// unreachable for validated thresholds
	return false
}

func (th Threshold) String() string {
	return fmt.Sprintf("%s %s %.3f", th.Metric, th.Op, th.Value)
}

// SortedTargetIDs returns the declared target scope in deterministic order.
func (s Spec) SortedTargetIDs() []string {
	ids := append([]string(nil), s.TargetIDs...)
	sort.Strings(ids)
	return ids
}
