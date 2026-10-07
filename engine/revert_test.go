package engine

import (
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/lechandonga/chaos-drill-orchestrator-go/chaos"
	"github.com/lechandonga/chaos-drill-orchestrator-go/experiment"
)

// 业务：撤销必须幂等。正常结束后对所有目标再做一次 Revert 也不会报错，
// 目标保持在基线。
func TestRevertIdempotent(t *testing.T) {
	h := newHarness(t, "svc-i")
	eng := h.newEngine()
	spec := h.baseSpec("idem", []string{"svc-i"})
	spec.DurationMs = 1000
	if _, err := eng.Submit(spec); err != nil {
		t.Fatal(err)
	}
	h.waitFor("idem", experiment.EvRunnerReady)
	h.clk.Tick(1000)
	h.waitFor("idem", experiment.EvFinished)

	tg, _ := h.reg.Get("svc-i")
	if !tg.AtBaseline() {
		t.Fatal("正常结束后应在基线")
	}
	// 对已无故障的目标重复撤销必须幂等（不报错、保持基线）。
	for i := 0; i < 3; i++ {
		if err := tg.Revert("idem"); err != nil {
			t.Fatalf("重复撤销必须幂等，第 %d 次报错: %v", i+1, err)
		}
		if !tg.AtBaseline() {
			t.Fatal("重复撤销后仍应保持基线")
		}
	}
}

// 业务：个别目标撤销失败时必须单独记录、单独重试，不拖累其它目标；
// 重试成功后全部回到基线。
func TestRevertFailureIsolatedAndRetried(t *testing.T) {
	h := newHarness(t, "ok-1", "bad-1", "ok-2")

	var mu sync.Mutex
	failCount := map[string]int{}
	hook := func(targetID string, attempt int) error {
		if targetID != "bad-1" {
			return nil // 其它目标始终成功
		}
		mu.Lock()
		defer mu.Unlock()
		failCount[targetID] = attempt
		if attempt < 3 {
			return fmt.Errorf("模拟 bad-1 第 %d 次撤销失败", attempt)
		}
		return nil
	}
	eng := h.newEngine(WithRevertHook(hook))
	spec := h.baseSpec("partial", []string{"ok-1", "bad-1", "ok-2"})
	spec.DurationMs = 1000
	if _, err := eng.Submit(spec); err != nil {
		t.Fatal(err)
	}
	h.waitFor("partial", experiment.EvRunnerReady)
	h.clk.Tick(1000)
	h.waitFor("partial", experiment.EvFinished)

	// 两个好目标不受坏目标影响，首轮即回到基线。
	for _, id := range []string{"ok-1", "ok-2"} {
		tg, _ := h.reg.Get(id)
		if !tg.AtBaseline() {
			t.Fatalf("好目标 %s 不应被坏目标拖累", id)
		}
	}
	// 坏目标在同一轮内被自动重试到第 3 次成功。
	bad, _ := h.reg.Get("bad-1")
	if !bad.AtBaseline() {
		t.Fatal("bad-1 重试后应回到基线")
	}
	mu.Lock()
	if failCount["bad-1"] < 3 {
		t.Fatalf("bad-1 应至少重试到第 3 次，实际 %d", failCount["bad-1"])
	}
	attempts := failCount["bad-1"]
	mu.Unlock()

	rec, _ := eng.Status("partial")
	if rec.Phase != experiment.PhaseCompleted {
		t.Fatalf("尽管中途有目标失败，全部重试成功后应 completed，得到 %s", rec.Phase)
	}
	cs := rec.Targets["bad-1"]
	if cs.State != experiment.TargetReverted || cs.Attempts != attempts {
		t.Fatalf("bad-1 清理记录应反映 %d 次尝试后成功，得到 %+v", attempts, cs)
	}
}

// 业务：超过最大重试仍失败的目标被单独标记为脏目标并保留租约占用，
// 阻止其它实验叠加；RetryCleanup 可在故障恢复后把它清干净。
func TestPersistentFailureRetryCleanup(t *testing.T) {
	h := newHarness(t, "good", "stuck")

	hook := func(targetID string, attempt int) error {
		if targetID == "stuck" {
			return errors.New("stuck 一直无法撤销")
		}
		return nil
	}
	eng := h.newEngine(WithRevertHook(hook))
	spec := h.baseSpec("persist", []string{"good", "stuck"})
	spec.DurationMs = 1000
	if _, err := eng.Submit(spec); err != nil {
		t.Fatal(err)
	}
	h.waitFor("persist", experiment.EvRunnerReady)
	h.clk.Tick(1000)
	h.waitFor("persist", experiment.EvFinished)

	// good 已清理；stuck 仍是脏目标，且租约保留占用，阻止叠加。
	good, _ := h.reg.Get("good")
	stuck, _ := h.reg.Get("stuck")
	if !good.AtBaseline() {
		t.Fatal("good 应已回到基线")
	}
	if stuck.AtBaseline() {
		t.Fatal("stuck 应仍未恢复")
	}
	dirty := eng.DirtyTargets("persist")
	if len(dirty) != 1 || dirty[0] != "stuck" {
		t.Fatalf("脏目标应只有 stuck，得到 %v", dirty)
	}
	// 其它实验此时无法在 stuck 上叠加（reject 冲突）。
	other := h.baseSpec("other", []string{"stuck"})
	if _, err := eng.Submit(other); !errors.Is(err, chaos.ErrTargetConflict) {
		t.Fatalf("脏目标必须阻止故障叠加，得到 %v", err)
	}

	// 故障排除后（移除 hook 效果），RetryCleanup 单独恢复 stuck。
	eng2, err := New(h.reg, h.st, h.clk, WithLogger(h.blog))
	if err != nil {
		t.Fatal(err)
	}
	if err := eng2.RetryCleanup("persist"); err != nil {
		t.Fatalf("故障排除后重试清理应成功: %v", err)
	}
	if !stuck.AtBaseline() {
		t.Fatal("RetryCleanup 后 stuck 必须回到基线")
	}
	if len(eng2.DirtyTargets("persist")) != 0 {
		t.Fatal("清理成功后不应再有脏目标")
	}
}
