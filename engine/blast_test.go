package engine

import (
	"testing"

	"github.com/lechandonga/chaos-drill-orchestrator-go/experiment"
	"github.com/lechandonga/chaos-drill-orchestrator-go/target"
)

// 业务：爆炸半径由引擎强制——即使调用方声明了 3 个目标，只要超过
// max_targets 或 max_percent，实验在准入阶段就被拒绝，任何故障都不注入。
func TestBlastRadiusEnforced(t *testing.T) {
	reg := target.NewRegistry()
	for _, id := range []string{"a", "b", "c", "d"} {
		reg.MustRegister(id, nil)
	}
	h := &harness{t: t, reg: reg}
	_ = h
	// 直接对纯函数做强制校验，避免依赖引擎装配。
	all := reg.All()
	three := []*target.Target{all[0], all[1], all[2]}

	if err := enforceBlastRadius(experiment.BlastRadius{MaxTargets: 3, MaxPercent: 100}, three, all); err != nil {
		t.Fatalf("3 目标 / max_targets=3 应允许: %v", err)
	}
	if err := enforceBlastRadius(experiment.BlastRadius{MaxTargets: 2, MaxPercent: 100}, three, all); err == nil {
		t.Fatal("3 目标超过 max_targets=2 必须被引擎拒绝")
	}
	// 3/4=75%，max_percent=50 必须拒绝；=75 允许。
	if err := enforceBlastRadius(experiment.BlastRadius{MaxTargets: 4, MaxPercent: 50}, three, all); err == nil {
		t.Fatal("75% 目标超过 max_percent=50 必须拒绝")
	}
	if err := enforceBlastRadius(experiment.BlastRadius{MaxTargets: 4, MaxPercent: 75}, three, all); err != nil {
		t.Fatalf("75%% 在 max_percent=75 边界应允许: %v", err)
	}
}

// 业务：标签选择器命中的目标同样受爆炸半径约束（不能绕过显式 ID 上限）。
func TestBlastRadiusByLabel(t *testing.T) {
	reg := target.NewRegistry()
	reg.MustRegister("web-1", map[string]string{"tier": "web"})
	reg.MustRegister("web-2", map[string]string{"tier": "web"})
	reg.MustRegister("db-1", map[string]string{"tier": "db"})
	got, err := reg.Resolve(target.Selector{Labels: map[string]string{"tier": "web"}})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("标签选择器应命中 2 个目标，得到 %d", len(got))
	}
	if err := enforceBlastRadius(experiment.BlastRadius{MaxTargets: 1, MaxPercent: 100}, got, reg.All()); err == nil {
		t.Fatal("标签选择器命中 2 目标但 max_targets=1，必须拒绝")
	}
}
