package engine

import (
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/lechandonga/chaos-drill-orchestrator-go/internal/clock"
	"github.com/lechandonga/chaos-drill-orchestrator-go/internal/experiment"
	"github.com/lechandonga/chaos-drill-orchestrator-go/internal/store"
	"github.com/lechandonga/chaos-drill-orchestrator-go/internal/target"
)

// testConfig is a fast deterministic configuration: observation and retry
// cadence are driven solely by the fake clock, never by real duration.
func testConfig(policy ConflictPolicy) Config {
	cfg := DefaultConfig()
	cfg.HardBlastCap = 4
	cfg.ObservationInterval = 10 * time.Millisecond
	cfg.MaxRevertAttempts = 3
	cfg.RevertRetryInterval = 5 * time.Millisecond
	cfg.Conflict = policy
	return cfg
}

func testLogger(t *testing.T) *slog.Logger {
	t.Helper()
	return slog.New(slog.NewTextHandler(
		os.Stderr,
		// Surface business logs with -v or on failure by swapping to os.Stderr.
		&slog.HandlerOptions{Level: slog.LevelDebug},
	))
}

func newWAL(t *testing.T) *store.WAL {
	t.Helper()
	w, err := store.Open(filepath.Join(t.TempDir(), "wal"))
	if err != nil {
		t.Fatalf("open wal: %v", err)
	}
	return w
}

func newRegistry() *target.Registry {
	reg := target.NewRegistry()
	reg.Register("svc-a", map[string]float64{"latency_ms": 20, "error_rate": 0, "cpu_pct": 10})
	reg.Register("svc-b", map[string]float64{"latency_ms": 20, "error_rate": 0, "cpu_pct": 10})
	reg.Register("svc-c", map[string]float64{"latency_ms": 20, "error_rate": 0, "cpu_pct": 10})
	reg.Register("svc-d", map[string]float64{"latency_ms": 20, "error_rate": 0, "cpu_pct": 10})
	reg.Register("svc-e", map[string]float64{"latency_ms": 20, "error_rate": 0, "cpu_pct": 10})
	return reg
}

func newTestEngine(t *testing.T, reg *target.Registry, wal *store.WAL, clk *clock.Fake, policy ConflictPolicy) *Engine {
	t.Helper()
	e, err := New(testConfig(policy), reg, wal, clk, testLogger(t))
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}
	t.Cleanup(e.Close)
	return e
}

// eventually polls fn a bounded number of times. Used only to wait for
// goroutines woken by a fake-clock advance; no experiment duration is ever
// really slept on.
func eventually(t *testing.T, fn func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("condition never became true")
}

// waitParked blocks until at least n fake timers are registered and waiting,
// i.e. worker goroutines have reached their clock-based wait and will not
// miss the next Advance. It performs real Gosched-style polling only, never
// advancing experiment time.
func waitParked(t *testing.T, clk *clock.Fake, n int) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if clk.Pending() >= n {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("expected %d parked timers, got %d", n, clk.Pending())
}

// advanceParked waits until the worker is parked then advances the clock,
// giving woken goroutines a scheduling beat afterwards.
func advanceParked(t *testing.T, clk *clock.Fake, d time.Duration) {
	t.Helper()
	waitParked(t, clk, 1)
	clk.Advance(d)
	for range 10 {
		runtime.Gosched()
	}
}

// stepUntil waits for the worker to park on the fake clock (or for done to
// become true), then advances by d. It is used for flows where the worker
// may finish before parking.
func stepUntil(t *testing.T, clk *clock.Fake, d time.Duration, done func() bool) bool {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if done() {
			return true
		}
		if clk.Pending() > 0 {
			clk.Advance(d)
			for range 5 {
				runtime.Gosched()
			}
			return done()
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("stepUntil: worker neither parked nor finished")
	return false
}

func latencySpec(id string, ids []string, intensity, durationMS, blast int) experiment.Spec {
	return experiment.Spec{
		ID:                id,
		TargetIDs:         ids,
		FaultKind:         "latency_delay",
		Intensity:         intensity,
		Duration:          time.Duration(durationMS) * time.Millisecond,
		BlastRadius:       blast,
		Steady:            experiment.Threshold{Metric: "latency_ms", Op: "gt", Value: 200},
		BreachConsecutive: 3,
	}
}

// scriptedProbe returns values from a schedule in probe order. It is safe
// for concurrent reads and lets a test describe an exact observation
// sequence (e.g. breach, breach, healthy, breach, breach, breach) without
// racing against the observer goroutine when changing hooks.
type scriptedProbe struct {
	mu     sync.Mutex
	values []float64
	idx    int
	repeat float64 // value returned after the schedule is exhausted
}

func newScriptedProbe(repeat float64, values ...float64) *scriptedProbe {
	return &scriptedProbe{values: values, repeat: repeat}
}

func (s *scriptedProbe) hook(string, float64) float64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.idx < len(s.values) {
		v := s.values[s.idx]
		s.idx++
		return v
	}
	return s.repeat
}

func (s *scriptedProbe) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.idx
}
