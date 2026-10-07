package experiment

import (
	"fmt"

	"github.com/lechandonga/chaos-drill-orchestrator-go/chaos"
	"github.com/lechandonga/chaos-drill-orchestrator-go/target"
)

// Comparison 是稳态假设的比较方向。
type Comparison string

const (
	OpLessThan    Comparison = "lt"
	OpGreaterThan Comparison = "gt"
)

// Condition 是一条可观测条件：metric op threshold。
// 稳态假设用它表达"系统应保持的状态"；止损条件用它表达"必须立即止损的红线"。
type Condition struct {
	Metric    string     `json:"metric"`
	Op        Comparison `json:"op"`
	Threshold float64    `json:"threshold"`
	// ConsecutiveTicks 违反必须连续达到的观测次数，用来吸收抖动；
	// 0 视为非法，必须由调用方显式声明。
	ConsecutiveTicks int `json:"consecutive_ticks"`
}

// Hypothesis 是稳态假设：Probes 全部"成立"才算稳态；
// 任一探针连续违反 ConsecutiveTicks 次即判定稳态失效。
type Hypothesis struct {
	Name   string      `json:"name"`
	Probes []Condition `json:"probes"`
}

// ConflictPolicy 决定同一目标冲突时的处理方式。
type ConflictPolicy string

const (
	// ConflictReject 只要有任一目标被占用，整个实验在提交时被拒绝。
	ConflictReject ConflictPolicy = "reject"
	// ConflictQueue 实验进入 FIFO 队列，等目标全部释放后按排队顺序获取。
	ConflictQueue ConflictPolicy = "queue"
)

// RecoveryPolicy 决定进程重启、发现未完成实验时的处置。
type RecoveryPolicy string

const (
	// RecoveryContinue 重新注入仍缺失的故障并继续推进到原定结束时间。
	RecoveryContinue RecoveryPolicy = "continue"
	// RecoveryRollback 撤销所有已注入故障，实验标记为 recovered_aborted。
	RecoveryRollback RecoveryPolicy = "rollback"
)

// BlastRadius 是爆炸半径硬上限，引擎在注入阶段强制执行：
// 同时受影响的目标数不得超过 MaxTargets，也不得超过注册目标总数的 MaxPercent%。
type BlastRadius struct {
	MaxTargets int `json:"max_targets"`
	MaxPercent int `json:"max_percent"` // 1-100
}

// Spec 是实验的完整声明。所有字段都必须显式给出，引擎不做隐式补默认。
type Spec struct {
	ID         string          `json:"id"`
	Targets    target.Selector `json:"targets"`
	Fault      chaos.FaultSpec `json:"fault"`
	DurationMs int             `json:"duration_ms"`
	Blast      BlastRadius     `json:"blast_radius"`
	Hypothesis Hypothesis      `json:"hypothesis"`
	StopLoss   *Condition      `json:"stop_loss,omitempty"`
	OnConflict ConflictPolicy  `json:"on_conflict"`
	OnRecover  RecoveryPolicy  `json:"on_recover"`
}

// ValidateAdmission 做不依赖运行时注册表的静态准入校验。
func (s Spec) ValidateAdmission() error {
	if s.ID == "" {
		return fmt.Errorf("%w: 实验 ID 不能为空", chaos.ErrSpecInvalid)
	}
	if s.Targets.Empty() {
		return fmt.Errorf("%w: 实验 %s 必须显式声明目标范围", chaos.ErrSpecInvalid, s.ID)
	}
	if err := s.Fault.Validate(); err != nil {
		return err
	}
	if s.DurationMs <= 0 {
		return fmt.Errorf("%w: duration_ms 必须显式声明且 > 0", chaos.ErrSpecInvalid)
	}
	if s.Blast.MaxTargets <= 0 {
		return fmt.Errorf("%w: blast_radius.max_targets 必须显式声明且 > 0", chaos.ErrBlastRadiusExceeded)
	}
	if s.Blast.MaxPercent <= 0 || s.Blast.MaxPercent > 100 {
		return fmt.Errorf("%w: blast_radius.max_percent 取值必须在 [1,100]", chaos.ErrBlastRadiusExceeded)
	}
	if err := validateHypothesis(s.Hypothesis); err != nil {
		return err
	}
	if s.StopLoss != nil {
		if err := validateCondition("止损条件", *s.StopLoss); err != nil {
			return err
		}
		if contradicts(s.Hypothesis, *s.StopLoss) {
			return fmt.Errorf("%w: 稳态阈值与止损阈值自相矛盾，二者之间没有安全区间",
				chaos.ErrFaultSpecContradiction)
		}
	}
	switch s.OnConflict {
	case ConflictReject, ConflictQueue:
	default:
		return fmt.Errorf("%w: on_conflict=%q（允许 reject/queue）",
			chaos.ErrConflictPolicyInvalid, s.OnConflict)
	}
	switch s.OnRecover {
	case RecoveryContinue, RecoveryRollback:
	default:
		return fmt.Errorf("%w: on_recover=%q（允许 continue/rollback）",
			chaos.ErrRecoveryPolicyInvalid, s.OnRecover)
	}
	return nil
}

func validateHypothesis(h Hypothesis) error {
	if h.Name == "" {
		return fmt.Errorf("%w: 稳态假设必须显式命名", chaos.ErrHypothesisInvalid)
	}
	if len(h.Probes) == 0 {
		return fmt.Errorf("%w: 稳态假设至少包含一条探针", chaos.ErrHypothesisInvalid)
	}
	for i, c := range h.Probes {
		if err := validateCondition(fmt.Sprintf("稳态探针[%d]", i), c); err != nil {
			return err
		}
	}
	return nil
}

func validateCondition(where string, c Condition) error {
	if c.Metric == "" {
		return fmt.Errorf("%w: %s 缺少 metric", chaos.ErrHypothesisInvalid, where)
	}
	switch c.Op {
	case OpLessThan, OpGreaterThan:
	default:
		return fmt.Errorf("%w: %s 比较方向 %q 非法（允许 lt/gt）",
			chaos.ErrHypothesisInvalid, where, c.Op)
	}
	if c.ConsecutiveTicks <= 0 {
		return fmt.Errorf("%w: %s consecutive_ticks 必须显式声明且 > 0（不得隐式吸收抖动）",
			chaos.ErrHypothesisInvalid, where)
	}
	return nil
}

// contradicts 判断同一指标上稳态上/下界与止损线之间是否没有安全区间。
// 例如稳态要求 latency<100，止损却在 latency>50，则任意值都会触发其一。
func contradicts(h Hypothesis, stop Condition) bool {
	for _, p := range h.Probes {
		if p.Metric != stop.Metric {
			continue
		}
		// 稳态要求 x < P（上界），止损要求 x > S（下界）：安全区间是 S < x < P。
		// 当 S >= P 时区间为空，才是矛盾。
		if p.Op == OpLessThan && stop.Op == OpGreaterThan && stop.Threshold >= p.Threshold {
			return true
		}
		// 稳态要求 x > P（下界），止损要求 x < S（上界）：安全区间是 P < x < S。
		// 当 S <= P 时区间为空，才是矛盾。
		if p.Op == OpGreaterThan && stop.Op == OpLessThan && stop.Threshold <= p.Threshold {
			return true
		}
	}
	return false
}

// Violated 判断观测值是否违反该条件（违反 = 条件不再成立）。
func (c Condition) Violated(value float64) bool {
	switch c.Op {
	case OpLessThan:
		return !(value < c.Threshold)
	case OpGreaterThan:
		return !(value > c.Threshold)
	default:
		return true
	}
}

// Hit 判断观测值是否"命中"该条件（条件字面成立）。止损线用它：
// 例如 error_rate > 0.5 在观测值 0.6 时命中，应立即止损。
func (c Condition) Hit(value float64) bool {
	switch c.Op {
	case OpLessThan:
		return value < c.Threshold
	case OpGreaterThan:
		return value > c.Threshold
	default:
		return false
	}
}
