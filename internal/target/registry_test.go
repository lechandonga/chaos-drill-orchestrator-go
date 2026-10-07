package target

import (
	"errors"
	"testing"
)

func TestInjectConflictNotOverwritten(t *testing.T) {
	r := NewRegistry()
	r.Register("x", map[string]float64{"latency_ms": 10})
	tg, _ := r.Get("x")

	if err := tg.Inject(Fault{Kind: "latency_delay", Intensity: 5}); err != nil {
		t.Fatal(err)
	}
	err := tg.Inject(Fault{Kind: "cpu_stress", Intensity: 9})
	if !errors.Is(err, ErrFaultActive) {
		t.Fatalf("second inject err = %v, want ErrFaultActive", err)
	}
	if f := tg.ActiveFault(); f == nil || f.Intensity != 5 {
		t.Fatalf("first fault overwritten: %+v", f)
	}

	changed, err := tg.Revert(r)
	if err != nil || !changed {
		t.Fatalf("revert = (%v,%v)", changed, err)
	}
	changed, err = tg.Revert(r)
	if err != nil || changed {
		t.Fatalf("idempotent revert = (%v,%v), want (false,nil)", changed, err)
	}
}

func TestProbeFaultEffectAndHook(t *testing.T) {
	r := NewRegistry()
	r.Register("x", map[string]float64{"latency_ms": 10})
	tg, _ := r.Get("x")
	if got := tg.Probe("latency_ms"); got != 10 {
		t.Fatalf("baseline = %v", got)
	}
	_ = tg.Inject(Fault{Kind: "latency_delay", Intensity: 40})
	if got := tg.Probe("latency_ms"); got != 50 {
		t.Fatalf("faulted latency = %v, want 50", got)
	}
	tg.SetProbeHook(func(metric string, base float64) float64 { return base + 100 })
	if got := tg.Probe("latency_ms"); got != 150 {
		t.Fatalf("hooked latency = %v, want 150", got)
	}
}

func TestRevertFailureThenSuccess(t *testing.T) {
	r := NewRegistry()
	r.Register("x", nil)
	tg, _ := r.Get("x")
	_ = tg.Inject(Fault{Kind: "latency_delay", Intensity: 1})
	r.FailNextReverts("x", 2)

	for range 2 {
		if _, err := tg.Revert(r); err == nil {
			t.Fatal("expected simulated revert failure")
		}
		if tg.ActiveFault() == nil {
			t.Fatal("fault must stay active while revert fails")
		}
	}
	if changed, err := tg.Revert(r); err != nil || !changed {
		t.Fatalf("third revert = (%v,%v), want success", changed, err)
	}
}

func TestUnknownTarget(t *testing.T) {
	r := NewRegistry()
	if _, err := r.Get("ghost"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("err = %v, want ErrNotFound", err)
	}
}
