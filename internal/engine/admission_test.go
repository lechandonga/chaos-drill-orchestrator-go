package engine

import (
	"context"
	"testing"
	"time"

	"github.com/lechandonga/chaos-drill-orchestrator-go/internal/clock"
	"github.com/lechandonga/chaos-drill-orchestrator-go/internal/experiment"
)

// TestAdmission covers the declarative admission business: each malformed
// declaration is rejected with its own reason code, and nothing is silently
// repaired by defaults.
func TestAdmission(t *testing.T) {
	clk := clock.NewFake(time.Unix(1000, 0))
	reg := newRegistry()
	e := newTestEngine(t, reg, newWAL(t), clk, ConflictReject)

	valid := latencySpec("ok", []string{"svc-a"}, 100, 500, 1)

	cases := []struct {
		name string
		mut  func(*experiment.Spec)
		code experiment.RejectCode
	}{
		{"unknown fault kind", func(s *experiment.Spec) { s.FaultKind = "disk_nuke" }, experiment.RejectUnknownFault},
		{"intensity above limit", func(s *experiment.Spec) { s.Intensity = 9999 }, experiment.RejectIntensityOutOfRange},
		{"intensity zero", func(s *experiment.Spec) { s.Intensity = 0 }, experiment.RejectIntensityOutOfRange},
		{"target missing", func(s *experiment.Spec) { s.TargetIDs = []string{"svc-ghost"} }, experiment.RejectTargetNotFound},
		{"empty target scope", func(s *experiment.Spec) { s.TargetIDs = nil }, experiment.RejectEmptyTargets},
		{"duplicate target", func(s *experiment.Spec) { s.TargetIDs = []string{"svc-a", "svc-a"} }, experiment.RejectDuplicateTargets},
		{"zero duration", func(s *experiment.Spec) { s.Duration = 0 }, experiment.RejectBadDuration},
		{"zero blast radius", func(s *experiment.Spec) { s.BlastRadius = 0 }, experiment.RejectBlastRadiusZero},
		{"blast radius over hard cap", func(s *experiment.Spec) { s.BlastRadius = 5 }, experiment.RejectBlastRadiusExceeds},
		{"unknown probe metric", func(s *experiment.Spec) { s.Steady.Metric = "mystery" }, experiment.RejectUnknownProbeMetric},
		{"consecutive below one", func(s *experiment.Spec) { s.BreachConsecutive = 0 }, experiment.RejectConsecutiveTooSmall},
		{"halt looser than steady", func(s *experiment.Spec) {
			loose := experiment.Threshold{Metric: "latency_ms", Op: "gt", Value: 500}
			s.Halt = &loose
		}, experiment.RejectHaltNotTighter},
		{"halt different metric", func(s *experiment.Spec) {
			other := experiment.Threshold{Metric: "cpu_pct", Op: "gt", Value: 50}
			s.Halt = &other
		}, experiment.RejectHaltNotTighter},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := valid
			tc.mut(&s)
			err := e.Start(context.Background(), s)
			if err == nil {
				t.Fatalf("expected rejection %q, experiment was accepted", tc.code)
			}
			var rej *experiment.RejectError
			if !asReject(err, &rej) {
				t.Fatalf("expected *RejectError, got %v", err)
			}
			if rej.Code != tc.code {
				t.Fatalf("reject code = %q, want %q (reason: %s)", rej.Code, tc.code, rej.Reason)
			}
		})
	}

	t.Run("valid declaration accepted", func(t *testing.T) {
		if err := e.Start(context.Background(), valid); err != nil {
			t.Fatalf("valid spec rejected: %v", err)
		}
		advanceParked(t, clk, 600*time.Millisecond)
		eventually(t, func() bool {
			r, _ := e.Status("ok")
			return r != nil && r.Phase.Terminal()
		})
		r, _ := e.Status("ok")
		if r.Phase != experiment.PhaseCompleted {
			t.Fatalf("phase = %s, want completed", r.Phase)
		}
	})
}

func asReject(err error, target **experiment.RejectError) bool {
	for err != nil {
		if r, ok := err.(*experiment.RejectError); ok {
			*target = r
			return true
		}
		if u, ok := err.(interface{ Unwrap() error }); ok {
			err = u.Unwrap()
			continue
		}
		break
	}
	return false
}
