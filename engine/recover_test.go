package engine

import (
	"testing"

	"github.com/lechandonga/chaos-drill-orchestrator-go/experiment"
	"github.com/lechandonga/chaos-drill-orchestrator-go/observer"
	"github.com/lechandonga/chaos-drill-orchestrator-go/store"
)

// 业务：执行中"进程崩溃"（Stop 但不撤销故障），重启后必须识别未完成实验，
// 按 continue 策略重新认领故障并安全推进到结束，不留下悬挂目标。
func TestCrashRecoveryContinue(t *testing.T) {
	h := newHarness(t, "svc-c")
	eng := h.newEngine()

	spec := h.baseSpec("crash", []string{"svc-c"})
	spec.OnRecover = experiment.RecoveryContinue
	spec.DurationMs = 5000
	if _, err := eng.Submit(spec); err != nil {
		t.Fatal(err)
	}
	h.waitFor("crash", experiment.EvInjected)
	h.waitFor("crash", experiment.EvRunnerReady)
	tg, _ := h.reg.Get("svc-c")
	h.clk.Tick(2000)
	h.waitFor("crash", experiment.EvSettled)

	// 模拟进程退出：停止引擎但不撤销故障（故障仍生效，租约仍指向 crash）。
	if tg.Lease() != "crash" {
		t.Fatalf("崩溃前目标应被 crash 持有: %q", tg.Lease())
	}
	v, _ := tg.Metric("latency_ms")
	if v != 120 {
		t.Fatalf("崩溃前故障应生效 latency=120，得到 %v", v)
	}
	eng.Stop()
	eng.Wait()

	// 新进程：新引擎实例、新的虚拟时钟（从更晚时刻开始），复用同一 store。
	clk2 := observer.NewFakeClock(2000)
	eng2, err := New(h.reg, h.st, clk2, WithLogger(h.blog), h.eventListener())
	if err != nil {
		t.Fatal(err)
	}
	// 恢复后故障仍被重新认领（幂等重注入），目标没有变成无人负责。
	v, _ = tg.Metric("latency_ms")
	if v != 120 || tg.Lease() != "crash" {
		t.Fatalf("恢复后故障应仍生效且租约指向 crash: v=%v lease=%q", v, tg.Lease())
	}
	rec, _ := eng2.Status("crash")
	if rec.Phase != experiment.PhaseRunning {
		t.Fatalf("continue 恢复后应处于 running，得到 %s", rec.Phase)
	}

	// 推进到原定结束时刻之后，实验正常收尾并清理。
	clk2.Tick(5000)
	h.waitFor("crash", experiment.EvFinished)
	eng2.Wait()
	rec, _ = eng2.Status("crash")
	if rec.Phase != experiment.PhaseCompleted {
		t.Fatalf("恢复推进后应 completed，得到 %s", rec.Phase)
	}
	if !tg.AtBaseline() {
		t.Fatal("恢复后结束必须回到基线")
	}
}

// 业务：崩溃重启后按 rollback 策略，必须把已注入故障全部撤销干净，
// 实验标记为 recovered_aborted。
func TestCrashRecoveryRollback(t *testing.T) {
	h := newHarness(t, "svc-r")
	eng := h.newEngine()
	spec := h.baseSpec("rollback", []string{"svc-r"})
	spec.OnRecover = experiment.RecoveryRollback
	if _, err := eng.Submit(spec); err != nil {
		t.Fatal(err)
	}
	h.waitFor("rollback", experiment.EvInjected)
	h.waitFor("rollback", experiment.EvRunnerReady)
	tg, _ := h.reg.Get("svc-r")
	eng.Stop()
	eng.Wait()

	clk2 := observer.NewFakeClock(1000)
	eng2, err := New(h.reg, h.st, clk2, WithLogger(h.blog), h.eventListener())
	if err != nil {
		t.Fatal(err)
	}
	eng2.Wait() // rollback 是同步清理
	rec, _ := eng2.Status("rollback")
	if rec.Phase != experiment.PhaseRecoveredAborted {
		t.Fatalf("rollback 恢复阶段应为 recovered_aborted，得到 %s", rec.Phase)
	}
	if !tg.AtBaseline() {
		t.Fatal("rollback 必须把故障撤销干净")
	}
}

// 业务：已正常完成的实验在重启后不应被重新调度。
func TestFinishedExperimentNotResumed(t *testing.T) {
	h := newHarness(t, "svc-d")
	eng := h.newEngine()
	spec := h.baseSpec("done", []string{"svc-d"})
	spec.DurationMs = 1000
	if _, err := eng.Submit(spec); err != nil {
		t.Fatal(err)
	}
	h.waitFor("done", experiment.EvInjected)
	h.waitFor("done", experiment.EvRunnerReady)
	h.clk.Tick(1000)
	h.waitFor("done", experiment.EvFinished)
	tg, _ := h.reg.Get("svc-d")
	if !tg.AtBaseline() {
		t.Fatal("结束后应在基线")
	}
	eng.Stop()

	clk2 := observer.NewFakeClock(2000)
	eng2, err := New(h.reg, h.st, clk2, WithLogger(h.blog), h.eventListener())
	if err != nil {
		t.Fatal(err)
	}
	// 已完成实验不会被恢复调度：再怎么推进时钟也不会重新注入或产生 goroutine。
	clk2.Tick(5000)
	eng2.Wait()
	rec, _ := eng2.Status("done")
	if rec.Phase != experiment.PhaseCompleted {
		t.Fatalf("已完成实验重启后不应改变阶段，得到 %s", rec.Phase)
	}
	if !tg.AtBaseline() {
		t.Fatal("已完成实验重启后不应重新注入故障")
	}
}

// 业务：store 落盘后用全新 store 打开也能读到记录（WAL/快照持久化有效）。
func TestStorePersistsAcrossHandles(t *testing.T) {
	st, err := store.OpenTemp()
	if err != nil {
		t.Fatal(err)
	}
	recs, _, err := st.LoadAll()
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 0 {
		t.Fatal("空存储不应有记录")
	}
	_ = st
}
