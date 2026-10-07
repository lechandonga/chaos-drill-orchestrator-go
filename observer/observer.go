package observer

import "github.com/lechandonga/chaos-drill-orchestrator-go/target"

// MetricReader 按指标名读取聚合观测值。
type MetricReader interface {
	Read(metric string) (float64, bool)
}

// Aggregator 定义跨目标的聚合方式（默认取最差值：max 或 min），
// 让任一目标的恶化都能被稳态假设看到。
type Aggregator string

const (
	AggMax Aggregator = "max"
	AggMin Aggregator = "min"
)

// TargetGroupReader 对一组受控目标做聚合读取，不涉及真实网络。
type TargetGroupReader struct {
	Targets []*target.Target
	Agg     Aggregator
}

func (r TargetGroupReader) Read(metric string) (float64, bool) {
	var agg float64
	found := false
	for _, t := range r.Targets {
		v, ok := t.Metric(metric)
		if !ok {
			continue
		}
		if !found {
			agg, found = v, true
			continue
		}
		switch r.Agg {
		case AggMin:
			if v < agg {
				agg = v
			}
		default:
			if v > agg {
				agg = v
			}
		}
	}
	return agg, found
}
