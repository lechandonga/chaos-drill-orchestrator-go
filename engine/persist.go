package engine

import (
	"time"

	"github.com/lechandonga/chaos-drill-orchestrator-go/experiment"
)

// persist 先追加 WAL 事件，再写快照。引擎崩溃时至少有事件落盘；
// 重启恢复时事件+快照合并即可还原到一致状态。
func (e *Engine) persist(rec *experiment.Record, evType, detail string) error {
	if rec == nil {
		return nil
	}
	ev := experiment.Event{
		At:           time.Now(),
		ExperimentID: rec.Spec.ID,
		Type:         evType,
		Detail:       detail,
	}
	if err := e.store.AppendEvent(ev); err != nil {
		return err
	}
	e.mu.Lock()
	rec.UpdatedAt = time.Now()
	snap := make(map[string]experiment.Record, len(e.records))
	for id, r := range e.records {
		snap[id] = r.Clone()
	}
	clone := rec.Clone()
	e.mu.Unlock()
	if err := e.store.SaveSnapshot(snap); err != nil {
		return err
	}
	e.notify(evType, clone)
	return nil
}
