package engine

import (
	"runtime"
	"sync"
	"testing"

	"github.com/lechandonga/chaos-drill-orchestrator-go/chaos"
	"github.com/lechandonga/chaos-drill-orchestrator-go/experiment"
	"github.com/lechandonga/chaos-drill-orchestrator-go/observer"
	"github.com/lechandonga/chaos-drill-orchestrator-go/store"
	"github.com/lechandonga/chaos-drill-orchestrator-go/target"
)

// harness 封装进程内受控环境：注册表 + 临时持久化 + 虚拟时钟 + 事件收集。
type harness struct {
	t    *testing.T
	reg  *target.Registry
	st   *store.Store
	clk  *observer.FakeClock
	blog *safeLog
	mu   sync.Mutex
	evs  map[string][]string
}

func newHarness(t *testing.T, targetIDs ...string) *harness {
	t.Helper()
	reg := target.NewRegistry()
	for _, id := range targetIDs {
		reg.MustRegister(id, map[string]string{"id": id})
	}
	st, err := store.OpenTemp()
	if err != nil {
		t.Fatal(err)
	}
	h := &harness{
		t: t, reg: reg, st: st,
		clk:  observer.NewFakeClock(0),
		blog: &safeLog{},
		evs:  map[string][]string{},
	}
	t.Cleanup(func() { h.blog.dump(t) })
	return h
}

func (h *harness) newEngine(opts ...Option) *Engine {
	h.t.Helper()
	all := append([]Option{WithLogger(h.blog), h.eventListener()}, opts...)
	eng, err := New(h.reg, h.st, h.clk, all...)
	if err != nil {
		h.t.Fatal(err)
	}
	return eng
}

func (h *harness) eventListener() Option {
	return WithEventListener(func(ev string, rec experiment.Record) {
		h.mu.Lock()
		h.evs[rec.Spec.ID] = append(h.evs[rec.Spec.ID], ev)
		h.mu.Unlock()
	})
}

// waitFor 协作式等待指定实验出现指定事件，不做真实 sleep。
func (h *harness) waitFor(id, ev string) {
	h.t.Helper()
	for i := 0; i < 200000; i++ {
		h.mu.Lock()
		for _, e := range h.evs[id] {
			if e == ev {
				h.mu.Unlock()
				return
			}
		}
		h.mu.Unlock()
		runtime.Gosched()
	}
	h.t.Fatalf("等待实验 %s 事件 %s 超时", id, ev)
}

func (h *harness) events(id string) []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]string(nil), h.evs[id]...)
}

func containsEvent(list []string, want string) bool {
	for _, e := range list {
		if e == want {
			return true
		}
	}
	return false
}

// tickUntil 逐虚拟秒推进，直到指定实验出现目标事件（最多 maxTick 秒）。
// 它不依赖 goroutine 调度速度：无论 runner 何时登记等待，逐次 tick +
// 协作让步都能让每个 tick 被消费，测试因此在竞态检测下也稳定。
func (h *harness) tickUntil(id, want string, maxTick int) {
	h.t.Helper()
	for i := 0; i < maxTick; i++ {
		if containsEvent(h.events(id), want) {
			return
		}
		h.clk.Tick(1000)
		// 让 runner goroutine 有机会消费本次 tick。
		for j := 0; j < 50; j++ {
			runtime.Gosched()
			if containsEvent(h.events(id), want) {
				return
			}
		}
	}
	h.waitFor(id, want)
}

// baseSpec 生成各字段都显式声明的合法实验（不依赖任何默认值）。
func (h *harness) baseSpec(id string, ids []string) experiment.Spec {
	return experiment.Spec{
		ID:         id,
		Targets:    target.Selector{TargetIDs: ids},
		Fault:      chaos.FaultSpec{Kind: chaos.FaultLatency, LatencyMs: 100},
		DurationMs: 10_000,
		Blast:      experiment.BlastRadius{MaxTargets: 4, MaxPercent: 100},
		Hypothesis: experiment.Hypothesis{
			Name: "延迟健康",
			Probes: []experiment.Condition{{
				Metric: target.MetricLatencyMs, Op: experiment.OpLessThan,
				Threshold: 1000, ConsecutiveTicks: 3,
			}},
		},
		OnConflict: experiment.ConflictReject,
		OnRecover:  experiment.RecoveryContinue,
	}
}
