// 本地可运行的演示：不连真实集群，全部目标都是进程内的受控对象。
//
// 运行: go run ./cmd/demo
package main

import (
	"errors"
	"fmt"
	"os"

	"github.com/lechandonga/chaos-drill-orchestrator-go/chaos"
	"github.com/lechandonga/chaos-drill-orchestrator-go/engine"
	"github.com/lechandonga/chaos-drill-orchestrator-go/experiment"
	"github.com/lechandonga/chaos-drill-orchestrator-go/observer"
	"github.com/lechandonga/chaos-drill-orchestrator-go/store"
	"github.com/lechandonga/chaos-drill-orchestrator-go/target"
)

func main() {
	reg := target.NewRegistry()
	reg.MustRegister("svc-a", map[string]string{"tier": "web"})
	reg.MustRegister("svc-b", map[string]string{"tier": "web"})
	reg.MustRegister("svc-c", map[string]string{"tier": "data"})

	fmt.Println("========== 场景1：准入拒绝（原因可区分） ==========")
	admissionDemo(reg)

	fmt.Println("========== 场景2：正常演练，到期后幂等清理 ==========")
	normalDemo(reg)

	fmt.Println("========== 场景3：稳态连续违反触发自动中止 ==========")
	abortDemo(reg)

	fmt.Println("========== 场景4：进程崩溃后重启恢复（continue） ==========")
	recoverDemo(reg)
}

func baseSpec(id string, ids []string) experiment.Spec {
	return experiment.Spec{
		ID:         id,
		Targets:    target.Selector{TargetIDs: ids},
		Fault:      chaos.FaultSpec{Kind: chaos.FaultLatency, LatencyMs: 500},
		DurationMs: 10_000,
		Blast:      experiment.BlastRadius{MaxTargets: 3, MaxPercent: 100},
		Hypothesis: experiment.Hypothesis{
			Name: "延迟保持可接受",
			Probes: []experiment.Condition{{
				Metric: target.MetricLatencyMs, Op: experiment.OpLessThan,
				Threshold: 2000, ConsecutiveTicks: 3,
			}},
		},
		OnConflict: experiment.ConflictReject,
		OnRecover:  experiment.RecoveryContinue,
	}
}

func admissionDemo(reg *target.Registry) {
	dir, _ := store.OpenTemp()
	defer cleanup(dir)
	clk := observer.NewFakeClock(0)
	eng, err := engine.New(reg, dir, clk)
	must(err)

	cases := map[string]experiment.Spec{}
	bad := baseSpec("bad-intensity", []string{"svc-a"})
	bad.Fault = chaos.FaultSpec{Kind: chaos.FaultLatency, LatencyMs: 999_999}
	cases["强度超限"] = bad

	missing := baseSpec("missing-target", []string{"svc-ghost"})
	cases["目标不存在"] = missing

	radius := baseSpec("too-wide", []string{"svc-a", "svc-b", "svc-c"})
	radius.Blast.MaxTargets = 2
	cases["爆炸半径超限"] = radius

	contradiction := baseSpec("contradiction", []string{"svc-a"})
	contradiction.StopLoss = &experiment.Condition{
		Metric: target.MetricLatencyMs, Op: experiment.OpGreaterThan,
		// 稳态要求 latency<2000，止损却在 latency>3000，不存在安全区间。
		Threshold: 3000, ConsecutiveTicks: 1,
	}
	cases["稳态与止损矛盾"] = contradiction

	for name, spec := range cases {
		_, err := eng.Submit(spec)
		fmt.Printf("  %-18s -> 拒绝: %v\n", name, classify(err))
	}
}

func normalDemo(reg *target.Registry) {
	dir, _ := store.OpenTemp()
	defer cleanup(dir)
	clk := observer.NewFakeClock(0)
	events := make(chan string, 64)
	eng, err := engine.New(reg, dir, clk, engine.WithEventListener(func(ev string, _ experiment.Record) {
		events <- ev
	}))
	must(err)

	_, err = eng.Submit(baseSpec("normal", []string{"svc-a"}))
	must(err)
	t, _ := reg.Get("svc-a")
	waitEvent(events, experiment.EvInjected)
	clk.Tick(1000)
	v, _ := t.Metric(target.MetricLatencyMs)
	fmt.Printf("  注入后 svc-a 延迟=%g（基线 20 + 故障 500）\n", v)

	// 重复撤销必须幂等：再跑 2 个 tick 到期后，引擎已自动清理。
	clk.Tick(10_000)
	waitEvent(events, experiment.EvFinished)
	eng.Wait()
	rec, _ := eng.Status("normal")
	fmt.Printf("  结束阶段=%s, svc-a 回到基线=%v\n", rec.Phase, t.AtBaseline())
}

func abortDemo(reg *target.Registry) {
	dir, _ := store.OpenTemp()
	defer cleanup(dir)
	clk := observer.NewFakeClock(0)
	events := make(chan string, 64)
	eng, err := engine.New(reg, dir, clk, engine.WithEventListener(func(ev string, _ experiment.Record) {
		events <- ev
	}))
	must(err)

	spec := baseSpec("abort-me", []string{"svc-b"})
	spec.Fault = chaos.FaultSpec{Kind: chaos.FaultLatency, LatencyMs: 800} // 直接越过阈值 400
	// 本场景使用严格稳态阈值，820ms 延迟连续 3 次违反即中止。
	spec.Hypothesis = experiment.Hypothesis{
		Name: "延迟必须低于 400ms",
		Probes: []experiment.Condition{{
			Metric: target.MetricLatencyMs, Op: experiment.OpLessThan,
			Threshold: 400, ConsecutiveTicks: 3,
		}},
	}
	_, err = eng.Submit(spec)
	must(err)
	waitEvent(events, experiment.EvInjected)

	// 每个 tick 后等到引擎完成该次观测，演示"连续违反计数"：
	// 前两次各自违反但未达阈值 3，不中止；第三次达到阈值才中止。
	clk.Tick(1000)
	waitEvent(events, experiment.EvSettled)
	clk.Tick(1000)
	waitEvent(events, experiment.EvSettled)
	rec, _ := eng.Status("abort-me")
	fmt.Printf("  连续违反 2 次后阶段=%s（抖动未达阈值，不中止）\n", rec.Phase)
	clk.Tick(1000)
	waitEvent(events, experiment.EvFinished)
	eng.Wait()
	rec, _ = eng.Status("abort-me")
	t, _ := reg.Get("svc-b")
	fmt.Printf("  连续违反 3 次后阶段=%s 原因=%q\n", rec.Phase, rec.AbortReason)
	fmt.Printf("  svc-b 回到基线=%v；重复中止结果: %v\n",
		t.AtBaseline(), classify(eng.Abort("abort-me", "again")))
}

func recoverDemo(reg *target.Registry) {
	dir, _ := store.OpenTemp()
	defer cleanup(dir)
	clk := observer.NewFakeClock(1000)
	events := make(chan string, 64)
	eng, err := engine.New(reg, dir, clk, engine.WithEventListener(func(ev string, _ experiment.Record) {
		events <- ev
	}))
	must(err)
	_, err = eng.Submit(baseSpec("crash", []string{"svc-c"}))
	must(err)
	waitEvent(events, experiment.EvInjected)
	clk.Tick(1000)
	t, _ := reg.Get("svc-c")
	v, _ := t.Metric(target.MetricLatencyMs)
	fmt.Printf("  崩溃前 svc-c 延迟=%g 且租约=%s\n", v, t.Lease())

	// 模拟进程退出：停止旧引擎（不清理故障），再用同一 store 启动新引擎。
	eng.Stop()
	eng.Wait()

	clk2 := observer.NewFakeClock(2000)
	events2 := make(chan string, 64)
	eng2, err := engine.New(reg, dir, clk2, engine.WithEventListener(func(ev string, _ experiment.Record) {
		events2 <- ev
	}))
	must(err)
	v, _ = t.Metric(target.MetricLatencyMs)
	fmt.Printf("  新引擎恢复后 svc-c 延迟=%g（故障被重新认领，无悬挂）\n", v)
	clk2.Tick(20_000)
	waitEvent(events2, experiment.EvFinished)
	eng2.Wait()
	rec, _ := eng2.Status("crash")
	fmt.Printf("  恢复推进后阶段=%s, svc-c 基线=%v\n", rec.Phase, t.AtBaseline())
}

func classify(err error) string {
	switch {
	case errors.Is(err, chaos.ErrFaultIntensityInvalid):
		return "强度非法"
	case errors.Is(err, chaos.ErrTargetNotFound):
		return "目标不存在"
	case errors.Is(err, chaos.ErrBlastRadiusExceeded):
		return "爆炸半径超限"
	case errors.Is(err, chaos.ErrFaultSpecContradiction):
		return "声明自相矛盾"
	case errors.Is(err, chaos.ErrAlreadyAborted):
		return "已中止过(ErrAlreadyAborted)"
	default:
		return err.Error()
	}
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}

func waitEvent(ch <-chan string, want string) {
	for ev := range ch {
		if ev == want {
			return
		}
	}
}

func cleanup(s *store.Store) { _ = os.RemoveAll(s.Dir()) }
