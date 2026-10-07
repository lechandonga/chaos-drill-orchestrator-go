package engine

import (
	"errors"
	"testing"

	"github.com/lechandonga/chaos-drill-orchestrator-go/chaos"
	"github.com/lechandonga/chaos-drill-orchestrator-go/experiment"
	"github.com/lechandonga/chaos-drill-orchestrator-go/target"
)

// 业务：声明式实验的准入校验必须区分拒绝原因，不得用默认值放行。
func TestAdmissionRejectionReasons(t *testing.T) {
	h := newHarness(t, "svc-a", "svc-b", "svc-c")
	eng := h.newEngine()

	cases := []struct {
		name    string
		mutate  func(s *experiment.Spec)
		wantErr error
	}{
		{
			name:    "强度超限被拒绝",
			mutate:  func(s *experiment.Spec) { s.Fault.LatencyMs = 999_999 },
			wantErr: chaos.ErrFaultIntensityInvalid,
		},
		{
			name:    "未显式声明强度被拒绝(不靠零值放行)",
			mutate:  func(s *experiment.Spec) { s.Fault = chaos.FaultSpec{Kind: chaos.FaultLatency} },
			wantErr: chaos.ErrFaultIntensityInvalid,
		},
		{
			name:    "未知故障类型被拒绝",
			mutate:  func(s *experiment.Spec) { s.Fault.Kind = "blackhole-network" },
			wantErr: chaos.ErrFaultKindUnknown,
		},
		{
			name:    "目标不存在被拒绝",
			mutate:  func(s *experiment.Spec) { s.Targets = target.Selector{TargetIDs: []string{"ghost"}} },
			wantErr: chaos.ErrTargetNotFound,
		},
		{
			name: "目标数超过 max_targets 被拒绝",
			mutate: func(s *experiment.Spec) {
				s.Targets = target.Selector{TargetIDs: []string{"svc-a", "svc-b", "svc-c"}}
				s.Blast.MaxTargets = 2
			},
			wantErr: chaos.ErrBlastRadiusExceeded,
		},
		{
			name: "目标比例超过 max_percent 被拒绝",
			mutate: func(s *experiment.Spec) {
				s.Targets = target.Selector{TargetIDs: []string{"svc-a", "svc-b"}}
				s.Blast.MaxPercent = 33 // 2/3 ≈ 66% > 33%
			},
			wantErr: chaos.ErrBlastRadiusExceeded,
		},
		{
			name: "稳态与止损阈值自相矛盾被拒绝",
			mutate: func(s *experiment.Spec) {
				s.StopLoss = &experiment.Condition{
					Metric: target.MetricLatencyMs, Op: experiment.OpGreaterThan,
					Threshold: 2000, ConsecutiveTicks: 1,
				}
			},
			wantErr: chaos.ErrFaultSpecContradiction,
		},
		{
			name: "稳态与止损之间存在安全区间时不矛盾",
			mutate: func(s *experiment.Spec) {
				s.StopLoss = &experiment.Condition{
					Metric: target.MetricLatencyMs, Op: experiment.OpGreaterThan,
					Threshold: 500, ConsecutiveTicks: 1,
				}
			},
			wantErr: nil,
		},
		{
			name:    "持续时长未声明被拒绝",
			mutate:  func(s *experiment.Spec) { s.DurationMs = 0 },
			wantErr: chaos.ErrSpecInvalid,
		},
		{
			name: "连续阈值未显式声明被拒绝(抖动参数不得隐式)",
			mutate: func(s *experiment.Spec) {
				s.Hypothesis.Probes[0].ConsecutiveTicks = 0
			},
			wantErr: chaos.ErrHypothesisInvalid,
		},
		{
			name:    "非法冲突策略被拒绝",
			mutate:  func(s *experiment.Spec) { s.OnConflict = "overwrite" },
			wantErr: chaos.ErrConflictPolicyInvalid,
		},
		{
			name:    "非法恢复策略被拒绝",
			mutate:  func(s *experiment.Spec) { s.OnRecover = "guess" },
			wantErr: chaos.ErrRecoveryPolicyInvalid,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			spec := h.baseSpec(tc.name, []string{"svc-a"})
			tc.mutate(&spec)
			_, err := eng.Submit(spec)
			if tc.wantErr == nil {
				if err != nil {
					t.Fatalf("该声明应被接受，却被拒绝: %v", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("期望被拒绝(%v)，但实验被接受", tc.wantErr)
			}
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("拒绝原因不匹配: 得到 %v，期望包装 %v", err, tc.wantErr)
			}
		})
	}
}

// 业务：合法实验可以正常准入，且准入不改变任何目标状态。
func TestAdmissionAccept(t *testing.T) {
	h := newHarness(t, "svc-a")
	eng := h.newEngine()
	spec := h.baseSpec("ok", []string{"svc-a"})
	if _, err := eng.Submit(spec); err != nil {
		t.Fatalf("合法实验不应被拒绝: %v", err)
	}
	h.waitFor("ok", experiment.EvInjected)
	tg, _ := h.reg.Get("svc-a")
	if tg.Lease() != "ok" {
		t.Fatalf("准入并注入后租约应为 ok，得到 %q", tg.Lease())
	}
}
