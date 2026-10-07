package chaos

import (
	"fmt"
	"strings"
)

// FaultKind 标识一类受控故障。
type FaultKind string

const (
	// FaultLatency 给目标注入延迟（单位：毫秒）。
	FaultLatency FaultKind = "latency"
	// FaultError 按比例让目标的请求失败。
	FaultError FaultKind = "error_rate"
	// FaultCPUBurn 让目标消耗 CPU（0.0~1.0 的占用强度）。
	FaultCPUBurn FaultKind = "cpu_burn"
)

// FaultSpec 描述故障类型与强度。
//
// 强度字段的含义随 Kind 而变，由 Validate 显式校验，
// 引擎不会用零值/默认值偷偷放行任何未声明字段。
type FaultSpec struct {
	Kind FaultKind `json:"kind"`

	// LatencyMs 仅 Kind=latency 时使用，取值 [1, 60000]。
	LatencyMs int `json:"latency_ms,omitempty"`
	// ErrorPercent 仅 Kind=error_rate 时使用，取值 [1, 100]（百分比）。
	ErrorPercent int `json:"error_percent,omitempty"`
	// BurnRatio 仅 Kind=cpu_burn 时使用，取值 (0, 1.0]。
	BurnRatio float64 `json:"burn_ratio,omitempty"`
}

// Validate 对强度做强校验，并区分拒绝原因。
func (f FaultSpec) Validate() error {
	switch f.Kind {
	case FaultLatency:
		if f.LatencyMs <= 0 {
			return fmt.Errorf("%w: latency 必须显式声明 latency_ms 且 > 0", ErrFaultIntensityInvalid)
		}
		if f.LatencyMs > 60000 {
			return fmt.Errorf("%w: latency_ms=%d 超过上限 60000ms", ErrFaultIntensityInvalid, f.LatencyMs)
		}
		if f.ErrorPercent != 0 || f.BurnRatio != 0 {
			return fmt.Errorf("%w: kind=latency 时不得声明 error_percent/burn_ratio", ErrFaultSpecContradiction)
		}
	case FaultError:
		if f.ErrorPercent <= 0 {
			return fmt.Errorf("%w: error_rate 必须显式声明 error_percent 且 > 0", ErrFaultIntensityInvalid)
		}
		if f.ErrorPercent > 100 {
			return fmt.Errorf("%w: error_percent=%d 超过 100", ErrFaultIntensityInvalid, f.ErrorPercent)
		}
		if f.LatencyMs != 0 || f.BurnRatio != 0 {
			return fmt.Errorf("%w: kind=error_rate 时不得声明 latency_ms/burn_ratio", ErrFaultSpecContradiction)
		}
	case FaultCPUBurn:
		if f.BurnRatio <= 0 {
			return fmt.Errorf("%w: cpu_burn 必须显式声明 burn_ratio 且 > 0", ErrFaultIntensityInvalid)
		}
		if f.BurnRatio > 1 {
			return fmt.Errorf("%w: burn_ratio=%.2f 超过 1.0", ErrFaultIntensityInvalid, f.BurnRatio)
		}
		if f.LatencyMs != 0 || f.ErrorPercent != 0 {
			return fmt.Errorf("%w: kind=cpu_burn 时不得声明 latency_ms/error_percent", ErrFaultSpecContradiction)
		}
	default:
		return fmt.Errorf("%w: 未知故障类型 %q（支持: %s）",
			ErrFaultKindUnknown, f.Kind, strings.Join(kindNames(), ", "))
	}
	return nil
}

func kindNames() []string {
	return []string{string(FaultLatency), string(FaultError), string(FaultCPUBurn)}
}

// String 返回人类可读的强度描述，用于日志。
func (f FaultSpec) String() string {
	switch f.Kind {
	case FaultLatency:
		return fmt.Sprintf("latency(%dms)", f.LatencyMs)
	case FaultError:
		return fmt.Sprintf("error_rate(%d%%)", f.ErrorPercent)
	case FaultCPUBurn:
		return fmt.Sprintf("cpu_burn(%.2f)", f.BurnRatio)
	default:
		return string(f.Kind)
	}
}
