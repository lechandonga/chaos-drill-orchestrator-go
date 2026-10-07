package engine

import (
	"fmt"

	"github.com/lechandonga/chaos-drill-orchestrator-go/chaos"
	"github.com/lechandonga/chaos-drill-orchestrator-go/experiment"
	"github.com/lechandonga/chaos-drill-orchestrator-go/observer"
	"github.com/lechandonga/chaos-drill-orchestrator-go/target"
)

const maxRevertAttempts = 5

// run 是单个实验的状态机驱动循环，由虚拟时钟 tick 驱动，不做真实等待。
func (e *Engine) run(id string, targets []*target.Target, recovered bool) {
	defer e.wg.Done()

	if err := e.acquire(id, targets); err != nil {
		// 排队期间实验被中止：不获取任何租约，直接终结。
		e.log.Printf("[实验 %s] 排队中止，未注入任何故障: %v", id, err)
		e.finish(id, experiment.PhaseAborted)
		return
	}

	if !recovered {
		e.injectAll(id, targets)
	}

	e.mu.Lock()
	rec := e.records[id]
	if rec.Phase == experiment.PhaseQueued {
		rec.Phase = experiment.PhaseRunning
		if rec.StartedAtMs == 0 {
			rec.StartedAtMs = e.clock.NowMs()
			rec.EndAtMs = rec.StartedAtMs + int64(rec.Spec.DurationMs)
		}
	}
	abortedInQueue := rec.AbortOnce
	e.mu.Unlock()
	_ = e.persist(rec, experiment.EvInjected, fmt.Sprintf("全部 %d 个目标注入完成", len(targets)))

	if abortedInQueue {
		e.log.Printf("[实验 %s] 排队期间已被中止，直接进入撤销", id)
		e.revertAll(id, targets)
		e.finish(id, experiment.PhaseAborted)
		return
	}

	reader := observer.TargetGroupReader{Targets: targets, Agg: observer.AggMax}
	monitor := observer.NewMonitor(e.snapshot(id).Spec, reader)
	ticker, ok := e.clock.(*observer.FakeClock)
	if !ok {
		panic("引擎时钟必须是 observer.FakeClock（不做真实等待）")
	}
	waiter := ticker.NewWaiter()
	e.mu.Lock()
	e.waiters[id] = waiter
	e.mu.Unlock()
	tickStep := int64(1000) // 观测周期：每虚拟秒做一次稳态判定
	lastObserved := e.snapshot(id).StartedAtMs - tickStep
	consumedGen := ticker.Gen() // 已做过稳态观测所对应的 tick 代号

	e.log.Printf("[实验 %s] 进入运行期，计划结束于虚拟时间 %d", id, e.snapshot(id).EndAtMs)
	_ = e.persist(e.mustRecord(id), experiment.EvRunnerReady, "runner 已就绪，tick 不会丢失")
	for {
		e.mu.Lock()
		phase := e.records[id].Phase
		manualAbort := e.records[id].AbortOnce && phase == experiment.PhaseAborting
		endAt := e.records[id].EndAtMs
		e.mu.Unlock()

		if manualAbort {
			e.triggerAbort(id, "手动中止", targets)
			return
		}
		if e.clock.NowMs() >= endAt {
			e.log.Printf("[实验 %s] 持续时间结束，正常收尾", id)
			e.revertAll(id, targets)
			e.finish(id, experiment.PhaseCompleted)
			return
		}

		g, ok := waiter.Wait(consumedGen)
		if !ok {
			select {
			case <-e.stopCh:
				e.log.Printf("[实验 %s] 引擎停止，保持故障并等待重启恢复", id)
				return
			default:
				continue // 被手动中止唤醒
			}
		}

		// 严格只消费本次唤醒所覆盖的代号区间 (consumedGen, g]。
		// 处理期间到达的 tick（代号 > g）本轮不读 Gen 抢跑，下一轮 Wait
		// 的首轮检查会立即返回它，从而杜绝"消费超前导致等待代号错位"。
		for consumedGen < g {
			lastObserved += tickStep
			consumedGen++
			violation, err := monitor.Observe()
			if err != nil {
				// 观测器自身异常不允许"假装健康"：按稳态失效安全中止。
				e.log.Printf("[实验 %s] 观测失败，按安全侧处理: %v", id, err)
				e.triggerAbort(id, "观测器异常: "+err.Error(), targets)
				return
			}
			_ = e.persist(e.mustRecord(id), experiment.EvTick,
				fmt.Sprintf("tick@%d 稳态正常", lastObserved))
			if violation != nil {
				e.triggerAbort(id, violation.Reason, targets)
				return
			}
		}
		// 本轮（可能含多个合并 tick）观测全部落地后发 settled，
		// 调用方可据此确信引擎已回到"等待下一 tick"的稳定点。
		_ = e.persist(e.mustRecord(id), experiment.EvSettled,
			fmt.Sprintf("已观测到 tick@%d，等待下一 tick", lastObserved))
	}
}

// acquire 按 FIFO 顺序获取全部目标租约：排在前面的实验未拿到目标时，
// 后面的实验不能"插队"占用重叠目标；互不重叠的实验不受阻塞。
func (e *Engine) acquire(id string, targets []*target.Target) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	for {
		select {
		case <-e.stopCh:
			return chaos.ErrExperimentNotRunning
		default:
		}
		rec := e.records[id]
		if rec.AbortOnce {
			return chaos.ErrAlreadyAborted
		}
		if e.blockedByEarlierLocked(id) || e.anyTargetBusyLocked(id) {
			e.cond.Wait()
			continue
		}
		for _, t := range targets {
			e.leased[t.ID] = id
		}
		return nil
	}
}

func (e *Engine) blockedByEarlierLocked(id string) bool {
	want := e.want[id]
	for _, earlier := range e.order {
		if earlier == id {
			return false
		}
		rec := e.records[earlier]
		if rec.Phase == experiment.PhaseCompleted ||
			rec.Phase == experiment.PhaseAborted ||
			rec.Phase == experiment.PhaseRecoveredAborted {
			continue
		}
		for tid := range want {
			if _, overlap := e.want[earlier][tid]; overlap {
				return true
			}
		}
	}
	return false
}

func (e *Engine) anyTargetBusyLocked(id string) bool {
	for tid := range e.want[id] {
		if owner, busy := e.leased[tid]; busy && owner != id {
			return true
		}
	}
	return false
}

func (e *Engine) injectAll(id string, targets []*target.Target) {
	spec := e.snapshot(id).Spec
	for _, t := range targets {
		if err := t.Inject(id, spec.Fault); err != nil {
			// 注册表租约已保证互斥；出现冲突属于程序不变量被破坏。
			panic(fmt.Sprintf("目标 %s 注入失败: %v", t.ID, err))
		}
		e.mu.Lock()
		e.records[id].Targets[t.ID].State = experiment.TargetInjected
		e.mu.Unlock()
		_ = e.persist(e.mustRecord(id), experiment.EvLeaseAcquired, "目标 "+t.ID+" 已注入 "+spec.Fault.String())
	}
}

// triggerAbort 保证一次实验只中止一次：monitor 与手动路径在此汇合，
// AbortOnce 已经过持久化，任何重复路径都会被终结状态挡住。
func (e *Engine) triggerAbort(id, reason string, targets []*target.Target) {
	e.mu.Lock()
	rec := e.records[id]
	if rec.Phase != experiment.PhaseAborting {
		rec.Phase = experiment.PhaseAborting
		rec.AbortOnce = true
		rec.AbortReason = reason
	}
	phase := rec.Phase
	e.mu.Unlock()
	if phase != experiment.PhaseAborting {
		return
	}
	_ = e.persist(e.mustRecord(id), experiment.EvAbort, reason)
	e.log.Printf("[实验 %s] 触发自动中止（仅此一次）: %s", id, reason)
	e.revertAll(id, targets)
	e.finish(id, experiment.PhaseAborted)
}

// revertAll 对每个目标独立撤销：撤销幂等，单个目标失败只记录该目标，
// 不影响其它目标；失败目标按轮次重试，最多 maxRevertAttempts 次。
func (e *Engine) revertAll(id string, targets []*target.Target) {
	e.mu.Lock()
	if e.records[id].Phase != experiment.PhaseAborting {
		e.records[id].Phase = experiment.PhaseReverting
	}
	e.mu.Unlock()

	pending := append([]*target.Target(nil), targets...)
	for attempt := 1; attempt <= maxRevertAttempts && len(pending) > 0; attempt++ {
		var failed []*target.Target
		for _, t := range pending {
			if err := e.revertOne(id, t, attempt); err != nil {
				failed = append(failed, t)
				continue
			}
			e.log.Printf("[实验 %s] 目标 %s 已回到基线（第 %d 次尝试）", id, t.ID, attempt)
		}
		pending = failed
	}
	if len(pending) > 0 {
		e.log.Printf("[实验 %s] 警告: %d 个目标在 %d 次尝试后仍未恢复，需人工介入: %v",
			id, len(pending), maxRevertAttempts, idsOf(pending))
	}
}

func (e *Engine) revertOne(id string, t *target.Target, attempt int) error {
	e.hookMu.Lock()
	hook := e.revertHook
	e.hookMu.Unlock()

	e.mu.Lock()
	cs := e.records[id].Targets[t.ID]
	cs.Attempts = attempt
	e.mu.Unlock()

	if hook != nil {
		if err := hook(t.ID, attempt); err != nil {
			e.recordRevertFailure(id, t, attempt, err)
			return err
		}
	}
	if err := t.Revert(id); err != nil {
		e.recordRevertFailure(id, t, attempt, err)
		return err
	}
	e.mu.Lock()
	cs.State = experiment.TargetReverted
	cs.LastError = ""
	e.mu.Unlock()
	_ = e.persist(e.mustRecord(id), experiment.EvRevertDone,
		fmt.Sprintf("目标 %s 撤销成功（第 %d 次）", t.ID, attempt))
	return nil
}

func (e *Engine) recordRevertFailure(id string, t *target.Target, attempt int, cause error) {
	e.mu.Lock()
	cs := e.records[id].Targets[t.ID]
	cs.LastError = cause.Error()
	e.mu.Unlock()
	_ = e.persist(e.mustRecord(id), experiment.EvRevertFailed,
		fmt.Sprintf("目标 %s 第 %d 次撤销失败: %v（不影响其它目标，将重试）", t.ID, attempt, cause))
}

func (e *Engine) finish(id string, phase experiment.Phase) {
	e.mu.Lock()
	rec := e.records[id]
	rec.Phase = phase
	for tid := range e.want[id] {
		if owner, ok := e.leased[tid]; ok && owner == id {
			cs, tracked := rec.Targets[tid]
			// 撤销失败的目标保留租约占用：禁止其它实验向"悬挂故障"目标叠加，
			// 它必须通过 RetryCleanup 单独恢复后才会释放。
			if tracked && cs.State != experiment.TargetReverted {
				continue
			}
			delete(e.leased, tid)
		}
	}
	delete(e.want, id)
	for i, v := range e.order {
		if v == id {
			e.order = append(e.order[:i], e.order[i+1:]...)
			break
		}
	}
	e.cond.Broadcast()
	e.mu.Unlock()
	_ = e.persist(rec, experiment.EvFinished, "终结阶段="+string(phase))
	e.log.Printf("[实验 %s] 结束: phase=%s", id, phase)
}

func (e *Engine) mustRecord(id string) *experiment.Record {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.records[id]
}

func idsOf(ts []*target.Target) []string {
	out := make([]string, 0, len(ts))
	for _, t := range ts {
		out = append(out, t.ID)
	}
	return out
}
