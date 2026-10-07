package engine

import (
	"errors"
	"testing"

	"github.com/lechandonga/chaos-drill-orchestrator-go/chaos"
	"github.com/lechandonga/chaos-drill-orchestrator-go/experiment"
	"github.com/lechandonga/chaos-drill-orchestrator-go/target"
)

// 业务：稳态违反必须连续达到阈值才中止，单次/间歇抖动（被健康观测打断）
// 不得造成误中止。
func TestSteadyStateJitterDoesNotAbort(t *testing.T) {
	h := newHarness(t, "svc-a")
	eng := h.newEngine()

	// 注入一个本身不会违反阈值的故障（延迟 100，阈值 1000）。
	spec := h.baseSpec("jitter", []string{"svc-a"})
	spec.Hypothesis.Probes[0].ConsecutiveTicks = 3
	if _, err := eng.Submit(spec); err != nil {
		t.Fatal(err)
	}
	h.waitFor("jitter", experiment.EvInjected)
	h.waitFor("jitter", experiment.EvRunnerReady)

	// 用受控目标指标模拟"违反-健康-违反-健康"的抖动：直接改写目标指标
	// 超出阈值、再复位，观测序列永远凑不齐连续 3 次违反。
	tg, _ := h.reg.Get("svc-a")
	violate := func() { tg.SetMetricForTest(target.MetricLatencyMs, 5000) }
	healthy := func() { tg.SetMetricForTest(target.MetricLatencyMs, 20) }

	// 序列：违，健，违，健，违，健 —— 最长连续违反只有 1。
	seq := []func(){violate, healthy, violate, healthy, violate, healthy}
	for _, step := range seq {
		step()
		h.clk.Tick(1000)
		h.waitFor("jitter", experiment.EvSettled)
		rec, _ := eng.Status("jitter")
		if rec.Phase != experiment.PhaseRunning {
			t.Fatalf("抖动期间不应中止，阶段=%s", rec.Phase)
		}
	}
	// 恢复健康后实验继续运行，目标仍在注入（未被误清理）。
	if tg.Lease() != "jitter" {
		t.Fatalf("未中止实验应继续持有目标租约，得到 %q", tg.Lease())
	}
}

// 业务：连续达到阈值后必须自动中止，且同一实验最多中止一次。
func TestSteadyStateConsecutiveTriggersAbortOnce(t *testing.T) {
	h := newHarness(t, "svc-a")
	eng := h.newEngine()

	spec := h.baseSpec("violate", []string{"svc-a"})
	spec.Fault = chaos.FaultSpec{Kind: chaos.FaultLatency, LatencyMs: 800}
	spec.Hypothesis.Probes[0].Threshold = 400
	spec.Hypothesis.Probes[0].ConsecutiveTicks = 3
	if _, err := eng.Submit(spec); err != nil {
		t.Fatal(err)
	}
	h.waitFor("violate", experiment.EvInjected)
	h.waitFor("violate", experiment.EvRunnerReady)

	for i := 0; i < 2; i++ {
		h.clk.Tick(1000)
		h.waitFor("violate", experiment.EvSettled)
		rec, _ := eng.Status("violate")
		if rec.Phase != experiment.PhaseRunning {
			t.Fatalf("前两次连续违反不应中止，阶段=%s", rec.Phase)
		}
	}
	h.clk.Tick(1000)
	h.waitFor("violate", experiment.EvFinished)

	rec, _ := eng.Status("violate")
	if rec.Phase != experiment.PhaseAborted {
		t.Fatalf("第三次连续违反必须中止，得到 %s", rec.Phase)
	}
	if !rec.AbortOnce {
		t.Fatal("中止标记 AbortOnce 必须置位")
	}
	// 同一实验重复中止必须被拒绝。
	if err := eng.Abort("violate", "again"); !errors.Is(err, chaos.ErrAlreadyAborted) {
		t.Fatalf("重复中止应返回 ErrAlreadyAborted，得到 %v", err)
	}
	// 全部目标回到基线。
	tg, _ := h.reg.Get("svc-a")
	if !tg.AtBaseline() {
		t.Fatal("中止撤销后目标必须回到基线")
	}
}

// 业务：止损条件优先级高于稳态，达到止损阈值立即（连续次数按声明）中止。
func TestStopLossFires(t *testing.T) {
	h := newHarness(t, "svc-a")
	eng := h.newEngine()
	spec := h.baseSpec("stop", []string{"svc-a"})
	spec.Fault = chaos.FaultSpec{Kind: chaos.FaultError, ErrorPercent: 60}
	spec.Hypothesis = experiment.Hypothesis{
		Name: "错误率一般可容忍",
		Probes: []experiment.Condition{{
			Metric: target.MetricErrorRate, Op: experiment.OpLessThan,
			Threshold: 0.9, ConsecutiveTicks: 5,
		}},
	}
	spec.StopLoss = &experiment.Condition{
		Metric: target.MetricErrorRate, Op: experiment.OpGreaterThan,
		Threshold: 0.5, ConsecutiveTicks: 1,
	}
	if _, err := eng.Submit(spec); err != nil {
		t.Fatal(err)
	}
	h.waitFor("stop", experiment.EvInjected)
	h.waitFor("stop", experiment.EvRunnerReady)
	h.clk.Tick(1000)
	h.waitFor("stop", experiment.EvFinished)
	rec, _ := eng.Status("stop")
	if rec.Phase != experiment.PhaseAborted {
		t.Fatalf("止损条件应立即中止，得到 %s", rec.Phase)
	}
}

// 业务：手动中止同样受"最多一次"约束。
func TestManualAbortOnce(t *testing.T) {
	h := newHarness(t, "svc-a")
	eng := h.newEngine()
	if _, err := eng.Submit(h.baseSpec("manual", []string{"svc-a"})); err != nil {
		t.Fatal(err)
	}
	h.waitFor("manual", experiment.EvInjected)
	h.waitFor("manual", experiment.EvRunnerReady)
	if err := eng.Abort("manual", "人工叫停"); err != nil {
		t.Fatalf("首次手动中止应成功: %v", err)
	}
	h.waitFor("manual", experiment.EvFinished)
	if err := eng.Abort("manual", "再次叫停"); !errors.Is(err, chaos.ErrAlreadyAborted) {
		t.Fatalf("重复手动中止应 ErrAlreadyAborted，得到 %v", err)
	}
}
