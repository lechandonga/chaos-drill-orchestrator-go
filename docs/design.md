# 设计说明：本地故障演练编排引擎

## 1. 实验模型

一次实验由 `experiment.Spec` 声明式描述：

| 字段 | 含义 |
| --- | --- |
| `ID` | 全局唯一标识，重复提交会被拒绝。 |
| `TargetIDs` | 目标范围；必须非空、无重复、全部在注册表中存在。 |
| `FaultKind` + `Intensity` | 故障类型与强度。类型固定且强度有显式上下限。 |
| `Duration` | 计划持续时长，必须为正。 |
| `BlastRadius` | 同一时刻允许受影响的最大目标数。 |
| `Steady` | 稳态阈值，表达“违约条件”（如 `latency_ms gt 200`）。 |
| `Halt` | 可选的更严格止损阈值，必须与稳态同指标、同方向且更早触发。 |
| `BreachConsecutive` | 触发中止所需的**连续**违约观测轮数，至少为 1。 |

支持的故障及强度区间（定义集中在一处，防止文档与代码漂移）：

| 故障 | 指标效果 | 强度范围 |
| --- | --- | --- |
| `latency_delay` | `latency_ms += 强度`（毫秒） | 1..2000 |
| `error_injection` | `error_rate += 强度`（百分点） | 1..50 |
| `cpu_stress` | `cpu_pct += 强度`（百分点） | 1..90 |

### 准入校验与拒绝码

所有拒绝都返回 `*experiment.RejectError`，携带机器可读 `Code` 与人类可读原因；
**绝不通过默认值悄悄修正非法声明**。

- `UNKNOWN_FAULT_KIND`：故障类型不支持。
- `INTENSITY_OUT_OF_RANGE`：强度越界（区分“未知类型”）。
- `TARGET_NOT_FOUND` / `EMPTY_TARGET_SCOPE` / `DUPLICATE_TARGET`：目标不存在 / 为空 / 重复。
- `BAD_DURATION`：时长非正。
- `BLAST_RADIUS_ZERO` / `BLAST_RADIUS_EXCEEDS_HARD_CAP`：爆炸半径为 0 或超过引擎硬上限。
- `STEADY_THRESHOLD_INVALID` / `UNKNOWN_PROBE_METRIC`：阈值操作符或指标非法。
- `CONSECUTIVE_BREACHES_BELOW_ONE`：连续违约阈值小于 1。
- `HALT_NOT_STRICTER_THAN_STEADY`：止损与稳态不同指标/方向，或不比稳态更早触发（自相矛盾）。
- 引擎层另有 `TARGET_CONFLICT`（并发冲突）与 `DUPLICATE_EXPERIMENT`。

## 2. 状态流转

```
pending ─► injecting ─► running ─► cleaning ─► completed
                │            │
                │            └─(稳态/止损/手动)─► aborting(内部标记) ─► cleaning
                │
重启恢复: recovering ─► cleaning（已有注入：撤销干净）
重启恢复: pending（尚未注入：安全续跑）

清理重试耗尽: failed（仅当仍有目标带故障且撤销失败；可 RetryCleanup 后转为 completed）
```

终态为 `completed`（所有已注入目标均已恢复）或 `failed`（重试耗尽仍有脏目标，
原因 `revert_retry_exhausted`）。引擎永远不会在目标仍带故障时把实验报告为成功。

## 3. 安全上限：爆炸半径

两层强制：

1. 准入阶段：`Spec.BlastRadius` 必须 `>=1` 且不超过 `Config.HardBlastCap`，否则拒绝。
2. 执行阶段：目标被切成大小不超过 `BlastRadius` 的**波次**；注入一波 → 观测该波 →
   撤销该波 → 再进入下一波。因此任意时刻携带故障的目标数不可能超过爆炸半径，
   不依赖调用方自觉。

## 4. 并发与冲突

引擎为每个目标维护“当前持有者（实验 ID）”。同一目标上的故障不可叠加，
目标层二次注入直接返回 `target.ErrFaultActive` 作为兜底防御。

- `reject`（默认）：新实验无法获得全部所需目标时，立即返回 `TARGET_CONFLICT`
  及具体被占目标与持有者。**不会发生后写覆盖先写。**
- `queue`：实验进入 FIFO 队列。只有队首可以获取目标；队首释放时唤醒后继，
  严格按入队顺序执行。等待可被 `context.Context` 取消。

实现上等待通过“持锁安装的一次性唤醒通道”完成，避免了信号丢失与条件变量自死锁。

## 5. 稳态判定与自动中止

- 观测按固定节拍（假时钟下完全确定）对当前波次所有目标探针读数。
- **连续去抖**：仅当连续 `BreachConsecutive` 轮观测违约才中止；任意一个健康轮将计数清零。
  因此单次观测抖动不会误判。
- `Halt` 止损线与 `Steady` 共用连续计数，但跨线原因单独记为 `halt_tripped`；
  普通稳态违约记为 `steady_violation`。
- 手动 `Abort` 记为 `manual_abort`。
- **最多中止一次**：`Run.AbortOnce` 保证并发/重复中止只有第一次生效，重复调用返回幂等错误。

## 6. 撤销语义、失败重试与幂等

- 每个目标独立清理（各自 goroutine），某目标撤销失败不阻塞其它目标。
- 撤销在目标层即幂等：对已恢复目标再次撤销返回 `(changed=false, err=nil)`，
  因此重复清理或“响应丢失后重试”不会产生二次破坏。
- 单个目标最多重试 `MaxRevertAttempts` 次，间隔 `RevertRetryInterval`；
  每次失败都记录在该目标的 `TargetState`（尝试次数、最近错误）。
- 全部成功 → `completed`；仍有脏目标 → `failed`。运维可调用 `RetryCleanup(id)`
  单独补清理，成功后实验转为 `completed`。

## 7. 崩溃恢复（WAL）

- 引擎把**完整快照**（而非增量事件）在每次关键状态变化时追加写入 JSON-lines WAL 并 fsync。
- 重启时按实验 ID 回放“最后一条快照”。日志尾部的半行写入会被忽略，不影响完整前缀。
- 恢复规则（见 `engine.recoverAll`）：
  - 终态实验：保持终态。
  - 非终态且已有故障注入：进入 `recovering → cleaning`，对快照中所有已注入目标执行撤销
    （“宁可全部撤销，也不带病继续”），全部恢复后结束；重试耗尽同样标记为 `failed` 并可事后补清理。
  - 非终态且尚未注入：重置为 `pending` 安全续跑。
- 受控目标注册表代表“比控制器活得更久的外部系统”：进程崩溃只丢掉引擎 goroutine，
  故障仍留在目标上，恢复时据此识别并接管这些**悬挂故障**，杜绝“故障仍生效却无人负责”。

## 8. 时间与可测性

`clock.Clock` 同时提供真实实现与 `clock.Fake`。测试用假时钟手动 `Advance`，
不做任何真实时长等待；测试在推进前等待 worker 挂起（`Fake.Pending()`），
以消除“先推进后注册定时器”的竞态。所有并发路径在 `-race -count=N` 下稳定。

## 9. 本地验证

```bash
go test ./...
CGO_ENABLED=1 go test -race -count=3 ./...
go test -v -race ./internal/engine/   # 查看业务日志
go run ./cmd/demo
```

### 测试覆盖的业务场景

| 测试 | 覆盖业务 |
| --- | --- |
| `TestAdmission` | 13 类非法声明的区分拒绝 + 合法实验正常结束。 |
| `TestConcurrentConflictReject` | 8 个实验并发抢同一目标：恰有 1 个成功，其余显式冲突拒绝，故障不被覆盖。 |
| `TestConcurrentConflictQueue` | FIFO 排队，严格按顺序执行，强度依次为各实验声明值。 |
| `TestBlastRadiusEnforced` | 波次内全程采样，任意时刻受影响目标数不超过爆炸半径。 |
| `TestSteadyAbortConsecutive` | 两次违约后健康轮清零，连续三次违约才中止；中止后清理干净。 |
| `TestHaltTrip` | 更严格止损线独立触发，原因记为 `halt_tripped`。 |
| `TestAbortOnce` | 重复手动中止只生效一次，清理每目标恰好一次。 |
| `TestCrashRecoveryWithInjectedFaults` | 注入后崩溃，重启回放 WAL 并撤销所有悬挂故障。 |
| `TestCrashRecoveryBeforeInjection` | 注入前崩溃，重启安全续跑到完成。 |
| `TestRevertFailureRetryAndIdempotency` | 撤销失败按目标独立重试，成功完成；重复撤销为幂等 no-op。 |
| `TestRevertExhaustedThenManualRetry` | 永久失败标记 `failed` 且不拖累健康目标，故障解除后手动补清理成功。 |
