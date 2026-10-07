package store

import (
	"testing"
	"time"

	"github.com/lechandonga/chaos-drill-orchestrator-go/experiment"
)

// 业务：事件追加 + 快照原子写入后，重新打开存储能完整还原记录与事件，
// 这是崩溃恢复识别未完成实验的基础。
func TestPersistAndReopen(t *testing.T) {
	st, err := OpenTemp()
	if err != nil {
		t.Fatal(err)
	}
	dir := st.Dir()

	rec := experiment.Record{
		Phase: experiment.PhaseRunning,
		Spec:  experiment.Spec{ID: "exp-1"},
		Targets: map[string]*experiment.TargetCleanup{
			"svc-a": {TargetID: "svc-a", State: experiment.TargetInjected},
		},
		CreatedAt: time.Now(),
	}
	if err := st.AppendEvent(experiment.Event{ExperimentID: "exp-1", Type: experiment.EvInjected}); err != nil {
		t.Fatal(err)
	}
	if err := st.SaveSnapshot(map[string]experiment.Record{"exp-1": rec}); err != nil {
		t.Fatal(err)
	}

	// 用同一目录重新打开（模拟新进程）。
	st2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	recs, events, err := st2.LoadAll()
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 1 || recs["exp-1"].Phase != experiment.PhaseRunning {
		t.Fatalf("快照应还原 running 记录，得到 %+v", recs)
	}
	if len(events) != 0 {
		// 快照已包含该事件之前的状态；快照序号之后无新事件，故不重复。
		t.Fatalf("快照之后不应有未并入事件，得到 %d", len(events))
	}

	// 快照之后再追加事件，重开后应作为未并入事件读出（供恢复重放）。
	if err := st2.AppendEvent(experiment.Event{ExperimentID: "exp-1", Type: experiment.EvAbort}); err != nil {
		t.Fatal(err)
	}
	st3, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	_, events, err = st3.LoadAll()
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 || events[0].Type != experiment.EvAbort {
		t.Fatalf("应读到快照之后的 abort 事件，得到 %+v", events)
	}
}
