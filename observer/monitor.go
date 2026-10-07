package observer

import (
	"fmt"

	"github.com/lechandonga/chaos-drill-orchestrator-go/experiment"
)

// Violation 描述一次稳态失效或止损触发。
type Violation struct {
	Kind   string // "steady_state" 或 "stop_loss"
	Probe  string
	Value  float64
	Reason string
}

// Monitor 维护每条探针的"连续违反计数"。单次抖动会被下一次健康观测清零，
// 只有连续违反次数达到声明阈值才判定失效——观测抖动不会造成误中止。
type Monitor struct {
	spec    experiment.Spec
	reader  MetricReader
	streaks map[string]int
	aborted bool
}

func NewMonitor(spec experiment.Spec, reader MetricReader) *Monitor {
	return &Monitor{spec: spec, reader: reader, streaks: map[string]int{}}
}

// Observe 执行一次观测。返回的 Violation 每轮至多一个；
// 止损线优先于稳态判定。调用方负责保证一次实验最多消费一次 Violation。
func (m *Monitor) Observe() (*Violation, error) {
	if m.aborted {
		return nil, nil
	}
	if m.spec.StopLoss != nil {
		if v, err := m.checkCondition("stop_loss", *m.spec.StopLoss); err != nil {
			return nil, err
		} else if v != nil {
			m.aborted = true
			return v, nil
		}
	}
	for _, p := range m.spec.Hypothesis.Probes {
		v, err := m.checkCondition("steady_state", p)
		if err != nil {
			return nil, err
		}
		if v != nil {
			m.aborted = true
			return v, nil
		}
	}
	return nil, nil
}

func (m *Monitor) checkCondition(kind string, c experiment.Condition) (*Violation, error) {
	value, ok := m.reader.Read(c.Metric)
	if !ok {
		return nil, fmt.Errorf("观测器无法读取指标 %q", c.Metric)
	}
	key := kind + ":" + c.Metric
	hit := c.Violated(value)
	if kind == "stop_loss" {
		hit = c.Hit(value) // 止损：红线条件字面成立即触发
	}
	if hit {
		m.streaks[key]++
		if m.streaks[key] >= c.ConsecutiveTicks {
			return &Violation{
				Kind:  kind,
				Probe: c.Metric,
				Value: value,
				Reason: fmt.Sprintf("%s 指标 %s=%g 连续 %d 次违反 %s %g",
					kind, c.Metric, value, m.streaks[key], c.Op, c.Threshold),
			}, nil
		}
		return nil, nil
	}
	m.streaks[key] = 0
	return nil, nil
}
