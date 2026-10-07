# 故障演练编排引擎（Go，本地受控版）

一个**完全本地运行、无真实集群/无外部依赖**的故障演练编排引擎。真实服务由进程内的
"受控目标"（`target.Target`，用一组可观测指标模拟一个实例）代替，时间由
**虚拟时钟**（`observer.FakeClock`）驱动，不做真实等待、不访问网络。

它覆盖了从**实验声明 → 准入校验 → 并发执行 → 稳态判定/自动中止 → 崩溃恢复 →
幂等撤销**的整条链路，并在并发、崩溃、误配置情况下都给出安全且可解释的结果。

## 快速开始

```bash
# 运行本地演示（准入拒绝 / 正常演练 / 稳态中止 / 崩溃恢复 四个场景）
go run ./cmd/demo

# 运行全部单测
go test ./...

# 竞态检测下运行（并发路径的硬性要求）
CGO_ENABLED=1 go test ./... -race

# 查看每个业务用例名称与覆盖情况
go test ./... -v
```

## 代码结构

| 包 | 职责 |
| --- | --- |
| `chaos` | 故障类型/强度定义、强校验与全部哨兵错误（拒绝原因可区分） |
| `target` | 进程内受控目标、目标注册表、标签/ID 选择器、目标级租约互斥 |
| `experiment` | 实验声明 `Spec`、准入校验、稳态假设/止损条件、状态机与事件定义 |
| `observer` | 虚拟时钟 `FakeClock`、指标聚合读取、连续违反判定器 `Monitor` |
| `store` | 事件 WAL + 原子快照，支撑进程重启后的状态恢复 |
| `engine` | 编排引擎：准入、爆炸半径强制、FIFO 冲突排队、稳态中止、撤销、恢复 |
| `cmd/demo` | 本地可运行的四个业务场景演示 |

## 实验模型与状态流转

实验通过 `experiment.Spec` **声明式**定义，所有关键字段都必须显式给出，
引擎不会用零值/默认值悄悄放行：

- `id`：实验唯一标识；
- `targets`：目标范围（显式 ID 列表和/或标签选择器）；
- `fault`：故障类型与强度（见下）；
- `duration_ms`：持续时长（虚拟毫秒，必须 > 0）；
- `blast_radius`：爆炸半径硬上限 `max_targets` 与 `max_percent`；
- `hypothesis`：稳态假设（一组探针，全部成立才算稳态）；
- `stop_loss`：可选止损红线；
- `on_conflict`：冲突策略 `reject` / `queue`；
- `on_recover`：重启恢复策略 `continue` / `rollback`。

支持的故障（`chaos.FaultSpec`）及强度区间：

| 故障 | 强度字段 | 合法区间 | 越界/缺失处理 |
| --- | --- | --- | --- |
| `latency` | `latency_ms` | `[1, 60000]` | `ErrFaultIntensityInvalid` |
| `error_rate` | `error_percent` | `[1, 100]` | `ErrFaultIntensityInvalid` |
| `cpu_burn` | `burn_ratio` | `(0, 1.0]` | `ErrFaultIntensityInvalid` |

状态机：

```
             准入通过
                │
                ▼
            [queued] ──排队期间被中止──► [aborted]
                │ 获取全部目标租约 + 注入完成
                ▼
            [running] ──持续时间正常结束──► [reverting] ─► [completed]
                │ 稳态连续违反 / 命中止损 / 手动中止（仅一次）
                ▼
            [aborting] ──幂等撤销（失败单独重试）──► [aborted]

   进程重启后：running/aborting 的未完成实验
      ├─ on_recover=continue  → 幂等重注入并继续推进
      └─ on_recover=rollback  → 全部撤销干净 → [recovered_aborted]
```

## 准入校验与拒绝原因

准入在注入**之前**完成，任何不合法声明都会被拒绝，且调用方可用
`errors.Is` 精确区分原因（见 `chaos/errors.go`）：

- `ErrSpecInvalid`：空 ID、空目标选择器、持续时长非法等；
- `ErrFaultKindUnknown`：不支持的故障类型；
- `ErrFaultIntensityInvalid`：强度缺失、为负或超出该类型硬上限；
- `ErrFaultSpecContradiction`：字段与 kind 不匹配、稳态阈值与止损线之间没有安全区间；
- `ErrTargetNotFound`：选择器显式引用了注册表中不存在的目标；
- `ErrBlastRadiusExceeded`：选中目标数/比例超出爆炸半径；
- `ErrHypothesisInvalid`：稳态/止损缺指标、比较方向非法、连续阈值未显式声明；
- `ErrConflictPolicyInvalid` / `ErrRecoveryPolicyInvalid`：策略字段非法。

## 安全上限（爆炸半径）

爆炸半径由**引擎强制执行**，而不是靠调用方自觉。准入阶段
`engine.enforceBlastRadius` 同时校验：

- 同时受影响目标数不得超过 `max_targets`；
- 受影响目标占注册表总目标数的比例不得超过 `max_percent`（向上严格比较，
  即 `selected * 100 > total * max_percent` 即拒绝）。

任一条件不满足，实验在**任何故障注入之前**就被拒绝。

## 冲突处理规则

同一目标同一时刻至多被一个实验占用（目标级租约，`target.Target.Inject`），
绝不"后写覆盖先写"：

- `reject`（默认安全策略）：`Submit` 在引擎锁内**同步预占**全部目标，
  只要任一目标已被占用，立即返回 `ErrTargetConflict`。同步预占堵住了
  "提交成功但异步 goroutine 尚未获取租约"的窗口，因此高并发提交同一批目标时
  也恰好只有一个实验成功。
- `queue`：实验进入 FIFO 队列。排在前面的实验未释放重叠目标时，后来者排队
  等待；目标集互不重叠的实验不受阻塞，可真正并发。先到者结束并释放后，
  排队者按顺序获取目标。

## 稳态判定、抖动与自动中止

- 每个虚拟秒做一次稳态观测（`observer.Monitor`）。
- 每条探针维护"**连续违反计数**"：一次健康观测立即把计数清零，因此单点
  抖动、间歇抖动不会误中止；只有连续违反次数达到声明的 `consecutive_ticks`
  才判定稳态失效（该次数必须显式声明，不允许隐式默认）。
- 止损条件优先级高于稳态；止损在红线条件**字面成立**并连续达到其声明次数时
  触发（例如 `error_rate > 0.5`）。
- 稳态失效、命中止损、观测器异常（按安全侧处理）、手动中止都汇入
  `triggerAbort`；`AbortOnce` 标志与终结阶段保证**同一实验最多中止一次**，
  重复中止返回 `ErrAlreadyAborted`。

## 中止与撤销语义

- 撤销按**目标**独立进行，整体幂等：
  `target.Target.Revert` 对没有故障的目标重复调用也不报错，目标保持基线。
- 个别目标撤销失败被**单独记录**（`TargetCleanup.Attempts/LastError`），
  不拖累其它目标的清理；失败目标在同一轮内按指数无关的固定轮次独立重试
  （最多 5 次）。
- 超过最大重试仍失败的目标进入"脏目标"清单（`Engine.DirtyTargets`），
  其租约**保留占用**，阻止其它实验向悬挂故障叠加；故障排除后可调用
  `RetryCleanup(expID)` 单独把它恢复，成功后才释放租约。
- 正常结束或中止后，成功恢复的目标都回到与实验前一致的基线
  （`Target.AtBaseline`）。

## 崩溃恢复

- 引擎每次状态变更先**追加 WAL 事件**（原子 rename + 目录 fsync），再写入
  **原子快照**（`store`）。进程退出时**不自动撤销**故障，而是保证重启后
  有人接管，杜绝"故障仍生效却无人负责"。
- 新进程构造引擎时（`engine.New`）自动执行恢复：读取快照 + 重放快照序号
  之后的 WAL 事件，识别未完成实验。
- `continue`：对仍缺故障的目标做**幂等重注入**（同一租约重复注入不叠加），
  按原定 `end_at` 继续推进到结束；
- `rollback`：同步撤销全部已注入故障，标记为 `recovered_aborted`；
- 已 `completed/aborted/recovered_aborted` 的实验信任原子快照的终结状态，
  不会被重新调度。

## 本地验证方法

```bash
# 1) 端到端演示：四个业务场景，输出可读结论
go run ./cmd/demo

# 2) 完整单测（含并发、崩溃恢复、撤销重试、幂等）
go test ./... -v

# 3) 竞态检测（并发路径必须稳定通过）
CGO_ENABLED=1 go test ./... -race

# 4) 反复运行确认无偶发时序问题
CGO_ENABLED=1 go test ./... -race -count=10
```

测试不依赖真实等待/网络/外部服务：时间用虚拟时钟按"虚拟秒"推进，
测试通过 `runner_ready` / `steady_settled` / `finished` 等事件做确定性同步，
`-v` 日志可直接看出覆盖的业务、每个业务的用例与通过情况。
