package chaos

import "errors"

// 准入与运行期错误全部使用哨兵错误，调用方可用 errors.Is 精确区分拒绝原因，
// 引擎不会用零值默认值把问题"悄悄放行"。
var (
	// ErrSpecInvalid 声明结构不合法（空 ID、空选择器、非法持续时长等）。
	ErrSpecInvalid = errors.New("实验声明非法")
	// ErrFaultKindUnknown 故障类型不在引擎支持范围内。
	ErrFaultKindUnknown = errors.New("未知故障类型")
	// ErrFaultIntensityInvalid 强度缺失、为负或超出该类型的硬上限。
	ErrFaultIntensityInvalid = errors.New("故障强度非法")
	// ErrFaultSpecContradiction 声明自相矛盾（字段与 kind 不匹配、稳态与止损互斥等）。
	ErrFaultSpecContradiction = errors.New("实验声明自相矛盾")
	// ErrTargetNotFound 选择器选中的目标在受控目标注册表中不存在。
	ErrTargetNotFound = errors.New("目标不存在")
	// ErrBlastRadiusExceeded 目标数量超出声明的爆炸半径上限（同时受影响目标数/比例）。
	ErrBlastRadiusExceeded = errors.New("超出爆炸半径上限")
	// ErrHypothesisInvalid 稳态假设或止损条件非法（无指标、阈值方向矛盾等）。
	ErrHypothesisInvalid = errors.New("稳态假设非法")
	// ErrConflictPolicyInvalid 冲突策略字段非法。
	ErrConflictPolicyInvalid = errors.New("冲突策略非法")
	// ErrRecoveryPolicyInvalid 恢复策略字段非法。
	ErrRecoveryPolicyInvalid = errors.New("恢复策略非法")

	// ErrTargetConflict 目标已被另一个进行中的实验占用（策略=reject 时直接返回）。
	ErrTargetConflict = errors.New("目标故障冲突")
	// ErrExperimentNotRunning 对非运行中实验执行了仅运行期允许的操作。
	ErrExperimentNotRunning = errors.New("实验不在运行中")
	// ErrAlreadyAborted 同一实验重复中止：一次实验最多中止一次。
	ErrAlreadyAborted = errors.New("实验已中止过")
)
