package engine

import (
	"fmt"

	"github.com/lechandonga/chaos-drill-orchestrator-go/experiment"
)

// recover 在引擎构造时运行：识别上一个进程未完成的实验，
// 依据声明的 on_recover 策略安全地继续推进，或把已注入故障全部撤销干净。
func (e *Engine) recover() error {
	records, events, err := e.store.LoadAll()
	if err != nil {
		return fmt.Errorf("恢复失败: %w", err)
	}

	e.mu.Lock()
	for id, rec := range records {
		r := rec
		e.records[id] = &r
	}
	e.replayEventsLocked(events)
	e.mu.Unlock()

	var toRun []string
	var toRollback []string

	e.mu.Lock()
	for id, rec := range e.records {
		switch rec.Phase {
		case experiment.PhaseCompleted, experiment.PhaseAborted, experiment.PhaseRecoveredAborted:
			// 已终结实验不重新调度；其租约状态在退出前已释放。
			continue
		case experiment.PhaseQueued, experiment.PhaseRunning,
			experiment.PhaseAborting, experiment.PhaseReverting:
			if rec.Spec.OnRecover == experiment.RecoveryRollback &&
				rec.Phase != experiment.PhaseQueued {
				toRollback = append(toRollback, id)
			} else {
				toRun = append(toRun, id)
			}
		}
	}
	// 排队顺序保持稳定（按创建顺序）。
	e.mu.Unlock()

	for _, id := range toRollback {
		if err := e.recoverRollback(id); err != nil {
			return err
		}
	}
	for _, id := range toRun {
		if err := e.recoverResume(id); err != nil {
			return err
		}
	}
	return nil
}

// replayEventsLocked 依据 WAL 修正快照可能落后于进程退出点的状态，
// 保证恢复出的阶段与"最后一条已落盘事件"一致。
func (e *Engine) replayEventsLocked(events []experiment.Event) {
	byExp := map[string][]experiment.Event{}
	for _, ev := range events {
		byExp[ev.ExperimentID] = append(byExp[ev.ExperimentID], ev)
	}
	for id, rec := range e.records {
		// 快照若已是终结阶段，直接信任：快照原子写入保证终结状态完整落盘。
		// （WAL 只重放快照序号之后的事件，已并入旧快照的 finished 事件
		// 不会重复出现，不能据此把已终结记录回退为运行中。）
		switch rec.Phase {
		case experiment.PhaseCompleted, experiment.PhaseAborted,
			experiment.PhaseRecoveredAborted:
			continue
		}
		aborted := false
		finished := false
		for _, ev := range byExp[id] {
			switch ev.Type {
			case experiment.EvAbort:
				aborted = true
			case experiment.EvFinished:
				finished = true
			}
		}
		if finished {
			continue
		}
		// 以快照中的 per-target 状态为权威依据判定注入进度，
		// 再用 WAL 中的 abort/finished 事件修正快照可能落后于崩溃点的阶段。
		anyInjected := false
		for _, cs := range rec.Targets {
			if cs.State == experiment.TargetInjected {
				anyInjected = true
			}
		}
		switch {
		case anyInjected && aborted:
			rec.Phase = experiment.PhaseAborting
		case anyInjected:
			rec.Phase = experiment.PhaseRunning
		default:
			rec.Phase = experiment.PhaseQueued
		}
		rec.AbortOnce = aborted
	}
}

func (e *Engine) recoverResume(id string) error {
	rec := e.records[id]
	targets, err := e.registry.Resolve(rec.Spec.Targets)
	if err != nil {
		return fmt.Errorf("恢复实验 %s 时解析目标失败: %w", id, err)
	}

	e.mu.Lock()
	want := map[string]struct{}{}
	for _, t := range targets {
		want[t.ID] = struct{}{}
	}
	e.want[id] = want
	e.order = append(e.order, id)
	alreadyInjected := rec.Phase == experiment.PhaseRunning || rec.Phase == experiment.PhaseAborting
	e.mu.Unlock()

	if alreadyInjected {
		// continue 策略：对仍缺故障的目标幂等重注入；目标已持有本实验租约时
		// Inject 是幂等的，不会产生叠加故障。
		for _, t := range targets {
			if err := t.Inject(id, rec.Spec.Fault); err != nil {
				return fmt.Errorf("恢复实验 %s 重注入目标 %s 失败: %w", id, t.ID, err)
			}
		}
		if rec.StartedAtMs == 0 {
			rec.StartedAtMs = e.clock.NowMs()
			rec.EndAtMs = rec.StartedAtMs + int64(rec.Spec.DurationMs)
		}
		e.log.Printf("[恢复] 实验 %s 按 continue 策略继续推进（%d 个目标已幂等重注入）",
			id, len(targets))
	} else {
		e.log.Printf("[恢复] 实验 %s 重启前仍在排队，重新进入 FIFO 队列", id)
	}

	e.wg.Add(1)
	go e.run(id, targets, alreadyInjected)
	return nil
}

func (e *Engine) recoverRollback(id string) error {
	rec := e.records[id]
	targets, err := e.registry.Resolve(rec.Spec.Targets)
	if err != nil {
		return fmt.Errorf("恢复实验 %s 时解析目标失败: %w", id, err)
	}
	e.log.Printf("[恢复] 实验 %s 按 rollback 策略撤销全部故障", id)
	e.revertAll(id, targets)

	e.mu.Lock()
	rec.Phase = experiment.PhaseRecoveredAborted
	delete(e.want, id)
	e.mu.Unlock()
	_ = e.persist(rec, experiment.EvFinished, "恢复后已安全撤销")
	return nil
}
