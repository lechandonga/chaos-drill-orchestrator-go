package experiment

import "testing"

func exists(string) bool { return true }

func TestValidateReasons(t *testing.T) {
	base := Spec{
		ID: "x", TargetIDs: []string{"a"}, FaultKind: "latency_delay",
		Intensity: 10, Duration: 1, BlastRadius: 1,
		Steady:            Threshold{Metric: "latency_ms", Op: "gt", Value: 100},
		BreachConsecutive: 2,
	}
	if err := base.Validate(exists, 2); err != nil {
		t.Fatalf("valid spec rejected: %v", err)
	}

	cases := []struct {
		mut  func(*Spec)
		code RejectCode
	}{
		{func(s *Spec) { s.FaultKind = "nope" }, RejectUnknownFault},
		{func(s *Spec) { s.Intensity = 100000 }, RejectIntensityOutOfRange},
		{func(s *Spec) { s.TargetIDs = nil }, RejectEmptyTargets},
		{func(s *Spec) { s.TargetIDs = []string{"a", "a"} }, RejectDuplicateTargets},
		{func(s *Spec) { s.Duration = 0 }, RejectBadDuration},
		{func(s *Spec) { s.BlastRadius = 3 }, RejectBlastRadiusExceeds},
		{func(s *Spec) { s.BreachConsecutive = 0 }, RejectConsecutiveTooSmall},
		{func(s *Spec) {
			h := Threshold{Metric: "latency_ms", Op: "gt", Value: 200}
			s.Halt = &h
		}, RejectHaltNotTighter},
	}
	for _, c := range cases {
		s := base
		c.mut(&s)
		err := s.Validate(exists, 2)
		if err == nil {
			t.Fatalf("mutation accepted, want %s", c.code)
		}
		re, ok := err.(*RejectError)
		if !ok || re.Code != c.code {
			t.Fatalf("err = %v, want code %s", err, c.code)
		}
	}

	if err := base.Validate(func(string) bool { return false }, 2); err == nil {
		t.Fatal("missing target must be rejected")
	}
}

func TestThresholdBreached(t *testing.T) {
	if (Threshold{Op: "gt", Value: 10}).Breached(10) {
		t.Fatal("gt must be strict")
	}
	if !(Threshold{Op: "gt", Value: 10}).Breached(11) {
		t.Fatal("11 > 10 should breach")
	}
	if (Threshold{Op: "lt", Value: 10}).Breached(10) {
		t.Fatal("lt must be strict")
	}
}
