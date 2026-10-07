// Command demo runs a fully in-process end-to-end scenario: it admits and
// executes several chaos experiments against controlled targets, shows a
// steady-state auto-abort, conflict rejection and crash recovery, with no
// network, cluster or external service involved.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"github.com/lechandonga/chaos-drill-orchestrator-go/internal/clock"
	"github.com/lechandonga/chaos-drill-orchestrator-go/internal/engine"
	"github.com/lechandonga/chaos-drill-orchestrator-go/internal/experiment"
	"github.com/lechandonga/chaos-drill-orchestrator-go/internal/store"
	"github.com/lechandonga/chaos-drill-orchestrator-go/internal/target"
)

func main() {
	log := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	dir, err := os.MkdirTemp("", "chaos-demo-")
	must(err)
	defer os.RemoveAll(dir)

	reg := target.NewRegistry()
	for _, id := range []string{"svc-a", "svc-b", "svc-c"} {
		reg.Register(id, map[string]float64{"latency_ms": 20, "error_rate": 0, "cpu_pct": 10})
	}

	clk := clock.Real{}
	wal, err := store.Open(filepath.Join(dir, "wal"))
	must(err)

	cfg := engine.DefaultConfig()
	cfg.HardBlastCap = 2
	cfg.ObservationInterval = 20 * time.Millisecond

	e, err := engine.New(cfg, reg, wal, clk, log)
	must(err)

	// Fast observation cadence for the demo.
	cfg.ObservationInterval = 20 * time.Millisecond
	cfg.RevertRetryInterval = 20 * time.Millisecond

	// 1) A normal bounded experiment on one target.
	normal := experiment.Spec{
		ID:                "demo-normal",
		TargetIDs:         []string{"svc-a"},
		FaultKind:         "latency_delay",
		Intensity:         80,
		Duration:          200 * time.Millisecond,
		BlastRadius:       1,
		Steady:            experiment.Threshold{Metric: "latency_ms", Op: "gt", Value: 500},
		Halt:              ptr(experiment.Threshold{Metric: "latency_ms", Op: "gt", Value: 400}),
		BreachConsecutive: 3,
	}
	must(e.Start(context.Background(), normal))
	waitTerminal(e, "demo-normal")

	// 2) Auto-abort via steady violation: push the probe over the halt
	// threshold for three consecutive rounds.
	bad := experiment.Spec{
		ID:                "demo-abort",
		TargetIDs:         []string{"svc-b"},
		FaultKind:         "latency_delay",
		Intensity:         50,
		Duration:          2 * time.Second,
		BlastRadius:       1,
		Steady:            experiment.Threshold{Metric: "latency_ms", Op: "gt", Value: 300},
		Halt:              ptr(experiment.Threshold{Metric: "latency_ms", Op: "gt", Value: 250}),
		BreachConsecutive: 3,
	}
	must(e.Start(context.Background(), bad))
	tgt, _ := reg.Get("svc-b")
	waitFault(tgt)

	// While svc-b is faulted, a competing experiment is explicitly rejected
	// under the default reject policy (never overwritten).
	conflict := bad
	conflict.ID = "demo-conflict"
	if err := e.Start(context.Background(), conflict); err != nil {
		fmt.Printf("CONFLICT RESULT: %v\n", err)
	} else {
		panic("conflicting experiment should have been rejected")
	}

	tgt.SetProbeHook(func(string, float64) float64 { return 280 })
	waitTerminal(e, "demo-abort")
	tgt.SetProbeHook(nil)

	// 4) Crash recovery: leave faults injected, then construct a new engine
	// over the same registry + WAL directory and confirm cleanup.
	crash := experiment.Spec{
		ID:                "demo-crash",
		TargetIDs:         []string{"svc-c"},
		FaultKind:         "latency_delay",
		Intensity:         40,
		Duration:          time.Second,
		BlastRadius:       1,
		Steady:            experiment.Threshold{Metric: "latency_ms", Op: "gt", Value: 500},
		BreachConsecutive: 3,
	}
	must(e.Start(context.Background(), crash))
	tgtC, _ := reg.Get("svc-c")
	waitFault(tgtC)
	// Simulate a hard process kill: stop the old engine without cleanup.
	// (Close waits for in-flight work only in the live process; here the
	// injected fault intentionally survives in the shared registry.)
	e.StopWithoutWait()
	e.WaitIdle()
	_ = wal.Close()

	wal2, err := store.Open(filepath.Join(dir, "wal"))
	must(err)
	e2, err := engine.New(cfg, reg, wal2, clk, log)
	must(err)
	waitTerminal(e2, "demo-crash")

	r, _ := e2.Status("demo-crash")
	fmt.Printf("\nDEMO RESULT: demo-crash recovered to phase=%s reason=%s\n", r.Phase, r.EndReason)
	for _, id := range []string{"svc-a", "svc-b", "svc-c"} {
		t, _ := reg.Get(id)
		fmt.Printf("target %-5s active_fault=%v\n", id, t.ActiveFault() != nil)
	}

	e2.Close()
}

func ptr[T any](v T) *T { return &v }

func must(err error) {
	if err != nil {
		panic(err)
	}
}

func waitTerminal(e *engine.Engine, id string) {
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		r, err := e.Status(id)
		if err == nil && r.Phase.Terminal() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	panic("timeout waiting for " + id)
}

func waitFault(t *target.Target) {
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if t.ActiveFault() != nil {
			return
		}
		time.Sleep(time.Millisecond)
	}
	panic("fault was never injected")
}
