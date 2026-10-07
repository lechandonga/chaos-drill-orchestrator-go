package target

import (
	"fmt"
	"sync"

	"github.com/lechandonga/chaos-drill-orchestrator-go/chaos"
)

// Target 是进程内的"受控目标"：用一组可观测指标模拟一个真实服务实例，
// 不涉及任何真实网络或外部依赖。
type Target struct {
	ID     string
	Labels map[string]string

	mu       sync.Mutex
	baseline map[string]float64
	current  map[string]float64
	lease    string // 当前占用该目标的实验 ID；空表示空闲
	fault    chaos.FaultSpec
}

func newTarget(id string, labels map[string]string) *Target {
	base := map[string]float64{
		MetricLatencyMs: 20,
		MetricErrorRate: 0,
		MetricCPU:       0.2,
	}
	cur := make(map[string]float64, len(base))
	for k, v := range base {
		cur[k] = v
	}
	return &Target{ID: id, Labels: labels, baseline: base, current: cur}
}

// 指标名约定，实验的稳态假设通过这些名字取值。
const (
	MetricLatencyMs = "latency_ms"
	MetricErrorRate = "error_rate"
	MetricCPU       = "cpu"
)

// Metric 读取当前指标值（被故障改写后即可被观测器看到）。
func (t *Target) Metric(name string) (float64, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	v, ok := t.current[name]
	return v, ok
}

// SetMetricForTest 供测试直接改写指标，模拟观测抖动；不改变租约/基线。
func (t *Target) SetMetricForTest(name string, v float64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.current[name] = v
}

// Baseline 返回实验前基线，清理后必须回到该值。
func (t *Target) Baseline() map[string]float64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := make(map[string]float64, len(t.baseline))
	for k, v := range t.baseline {
		out[k] = v
	}
	return out
}

// AtBaseline 判断目标是否已完全回到实验前状态。
func (t *Target) AtBaseline() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	for k, v := range t.baseline {
		if t.current[k] != v {
			return false
		}
	}
	return t.lease == ""
}

// Inject 在目标上应用故障。同一目标同一时刻只允许一个实验持有租约，
// 冲突绝不会"后写覆盖先写"。重复注入同一租约是幂等的（用于崩溃恢复重放）。
func (t *Target) Inject(leaseID string, f chaos.FaultSpec) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.lease != "" && t.lease != leaseID {
		return fmt.Errorf("%w: 目标 %s 已被实验 %s 占用",
			chaos.ErrTargetConflict, t.ID, t.lease)
	}
	t.lease = leaseID
	t.fault = f
	t.applyLocked(f)
	return nil
}

// Revert 撤销故障。只有持有租约的实验可以撤销；撤销幂等：
// 对没有故障的目标重复撤销不会报错，目标直接保持/回到基线。
func (t *Target) Revert(leaseID string) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.lease != "" && t.lease != leaseID {
		return fmt.Errorf("%w: 目标 %s 由实验 %s 持有，实验 %s 无权撤销",
			chaos.ErrTargetConflict, t.ID, t.lease, leaseID)
	}
	for k, v := range t.baseline {
		t.current[k] = v
	}
	t.fault = chaos.FaultSpec{}
	t.lease = ""
	return nil
}

// Lease 返回当前持有租约的实验 ID（调试/测试用）。
func (t *Target) Lease() string {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.lease
}

func (t *Target) applyLocked(f chaos.FaultSpec) {
	switch f.Kind {
	case chaos.FaultLatency:
		t.current[MetricLatencyMs] = t.baseline[MetricLatencyMs] + float64(f.LatencyMs)
	case chaos.FaultError:
		t.current[MetricErrorRate] = float64(f.ErrorPercent) / 100.0
	case chaos.FaultCPUBurn:
		t.current[MetricCPU] = f.BurnRatio
	}
}

// Fault 返回当前生效的故障描述（无故障时 ok=false）。
func (t *Target) Fault() (chaos.FaultSpec, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.lease == "" {
		return chaos.FaultSpec{}, false
	}
	return t.fault, true
}
