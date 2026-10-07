package experiment

import "time"

// Phase 是实验生命周期状态。
type Phase string

const (
	// PhaseQueued 已通过准入并持久化，等待获取全部目标租约（冲突排队）。
	PhaseQueued Phase = "queued"
	// PhaseRunning 已在全部选中目标上注入故障，稳态判定进行中。
	PhaseRunning Phase = "running"
	// PhaseAborting 已触发（且仅触发一次）中止，正在撤销故障。
	PhaseAborting Phase = "aborting"
	// PhaseReverting 持续时间正常结束，正在撤销故障。
	PhaseReverting Phase = "reverting"
	// PhaseCompleted 正常结束且全部目标回到基线。
	PhaseCompleted Phase = "completed"
	// PhaseAborted 稳态/止损触发中止，撤销完成（可能含重试记录）。
	PhaseAborted Phase = "aborted"
	// PhaseRecoveredAborted 重启恢复时按 rollback 策略清理完毕。
	PhaseRecoveredAborted Phase = "recovered_aborted"
	// PhaseRejected 准入阶段被拒绝（仅存于提交返回值，不持久化执行记录）。
	PhaseRejected Phase = "rejected"
)

// TargetState 是单个目标的注入/撤销状态。
type TargetState string

const (
	TargetSelected TargetState = "selected"
	TargetInjected TargetState = "injected"
	TargetReverted TargetState = "reverted"
)

// TargetCleanup 记录单个目标的撤销情况：个别目标撤销失败被单独记录，
// 不拖累其它目标清理，并在后续轮次独立重试。
type TargetCleanup struct {
	TargetID  string      `json:"target_id"`
	State     TargetState `json:"state"`
	Attempts  int         `json:"attempts"`
	LastError string      `json:"last_error,omitempty"`
}

// Event 是持久化的事件流条目（WAL），重启后据此重建状态。
type Event struct {
	Seq          int       `json:"seq"`
	At           time.Time `json:"at"`
	ExperimentID string    `json:"experiment_id"`
	Type         string    `json:"type"`
	Detail       string    `json:"detail,omitempty"`
}

// 事件类型常量。
const (
	EvAdmitted      = "admitted"
	EvLeaseAcquired = "lease_acquired"
	EvInjected      = "injected"
	EvTick          = "steady_tick"
	// EvRunnerReady 表示 runner 已完成注入、建立 tick 等待并进入运行循环，
	// 此后任何 Tick 都不会丢失。测试用它做确定性同步点。
	EvRunnerReady = "runner_ready"
	// EvSettled 表示一轮 tick 观测已全部完成且 runner 已准备好等待下一个 tick，
	// 测试据此做确定性同步（区别于 EvTick 可能在合并区间中途发出）。
	EvSettled      = "steady_settled"
	EvAbort        = "abort"
	EvRevertStart  = "revert_start"
	EvRevertDone   = "revert_done"
	EvRevertFailed = "revert_failed"
	EvFinished     = "finished"
)

// Record 是一个实验的可持久化状态快照。
type Record struct {
	Spec        Spec                      `json:"spec"`
	Phase       Phase                     `json:"phase"`
	Targets     map[string]*TargetCleanup `json:"targets"`
	StartedAtMs int64                     `json:"started_at_ms"` // 注入完成的虚拟时钟
	EndAtMs     int64                     `json:"end_at_ms"`
	AbortOnce   bool                      `json:"abort_once"`
	AbortReason string                    `json:"abort_reason,omitempty"`
	CreatedAt   time.Time                 `json:"created_at"`
	UpdatedAt   time.Time                 `json:"updated_at"`
}

// Clone 返回深拷贝，避免引擎内部 map 被外部读写形成数据竞争。
func (r *Record) Clone() Record {
	cp := *r
	cp.Spec.Targets.TargetIDs = append([]string(nil), r.Spec.Targets.TargetIDs...)
	cp.Spec.Hypothesis.Probes = append([]Condition(nil), r.Spec.Hypothesis.Probes...)
	if r.Spec.StopLoss != nil {
		s := *r.Spec.StopLoss
		cp.Spec.StopLoss = &s
	}
	cp.Targets = make(map[string]*TargetCleanup, len(r.Targets))
	for k, v := range r.Targets {
		c := *v
		cp.Targets[k] = &c
	}
	return cp
}
