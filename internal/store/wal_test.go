package store

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/lechandonga/chaos-drill-orchestrator-go/internal/experiment"
)

func TestAppendAndReplay(t *testing.T) {
	w, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	dir := filepath.Dir(w.Path())
	spec := func(id string) experiment.Spec {
		return experiment.Spec{
			ID: id, TargetIDs: []string{"a"}, FaultKind: "latency_delay",
			Intensity: 1, Duration: time.Second, BlastRadius: 1,
			Steady:            experiment.Threshold{Metric: "latency_ms", Op: "gt", Value: 1},
			BreachConsecutive: 1,
		}
	}
	r1 := experiment.NewRun(spec("e1"), time.Now())
	r2 := experiment.NewRun(spec("e2"), time.Now())
	if err := w.Append(r1); err != nil {
		t.Fatal(err)
	}
	if err := w.Append(r2); err != nil {
		t.Fatal(err)
	}
	r1.Phase = experiment.PhaseRunning
	if err := w.Append(r1); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	// Reopen the SAME file: Open must accept an existing directory.
	w3, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	runs, err := w3.Replay()
	if err != nil {
		t.Fatal(err)
	}
	if len(runs) != 2 {
		t.Fatalf("replayed %d runs, want 2", len(runs))
	}
	if runs["e1"].Phase != experiment.PhaseRunning {
		t.Fatalf("latest snapshot not used: %s", runs["e1"].Phase)
	}
}
