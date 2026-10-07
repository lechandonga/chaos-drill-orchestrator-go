package engine

import (
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/lechandonga/chaos-drill-orchestrator-go/chaos"
	"github.com/lechandonga/chaos-drill-orchestrator-go/experiment"
	"github.com/lechandonga/chaos-drill-orchestrator-go/observer"
	"github.com/lechandonga/chaos-drill-orchestrator-go/store"
	"github.com/lechandonga/chaos-drill-orchestrator-go/target"
)

// Logger 是引擎使用的最小日志接口，测试可接入 t.Logf。
type Logger interface {
	Printf(format string, args ...any)
}

// RevertHook 模拟撤销失败：返回非 nil 即表示该目标本次撤销失败。
// attempt 从 1 开始计数，测试可借此实现"前两次失败、之后成功"。
type RevertHook func(targetID string, attempt int) error

// Engine 是故障演练编排引擎。所有并发控制都在进程内完成，
// 不依赖真实集群；崩溃恢复依赖注入的 store 与受控目标注册表。
type Engine struct {
	registry *target.Registry
	store    *store.Store
	clock    observer.Clock
	log      Logger

	mu      sync.Mutex
	records map[string]*experiment.Record
	order   []string // FIFO 排队顺序，只记录未终结实验
	want    map[string]map[string]struct{}
	leased  map[string]string // targetID -> experimentID
	cond    *sync.Cond

	wg sync.WaitGroup

	hookMu     sync.Mutex
	revertHook RevertHook

	stopCh   chan struct{}
	stopOnce sync.Once

	waiters map[string]*observer.Waiter

	listenerMu sync.Mutex
	listeners  []func(evType string, rec experiment.Record)
}

// Option 用于装配引擎。
type Option func(*Engine)

func WithLogger(l Logger) Option { return func(e *Engine) { e.log = l } }

// WithRevertHook 注入撤销失败钩子（测试模拟个别目标撤销失败）。
func WithRevertHook(h RevertHook) Option {
	return func(e *Engine) { e.revertHook = h }
}

// WithEventListener 注册状态事件回调（每个终结/关键事件触发），
// 测试借此确定性等待，不依赖真实 sleep。
func WithEventListener(f func(evType string, rec experiment.Record)) Option {
	return func(e *Engine) {
		e.listenerMu.Lock()
		defer e.listenerMu.Unlock()
		e.listeners = append(e.listeners, f)
	}
}

// New 装配引擎并立即执行崩溃恢复：store 中未终结的实验会被识别，
// 按其声明的 on_recover 策略继续推进或撤销干净，绝不留下悬挂故障。
func New(reg *target.Registry, st *store.Store, clock observer.Clock, opts ...Option) (*Engine, error) {
	e := &Engine{
		registry: reg,
		store:    st,
		clock:    clock,
		log:      log.Default(),
		records:  map[string]*experiment.Record{},
		want:     map[string]map[string]struct{}{},
		leased:   map[string]string{},
		stopCh:   make(chan struct{}),
		waiters:  map[string]*observer.Waiter{},
	}
	e.cond = sync.NewCond(&e.mu)
	for _, o := range opts {
		o(e)
	}
	if err := e.recover(); err != nil {
		return nil, err
	}
	return e, nil
}

// Submit 执行完整准入校验并启动实验。拒绝时返回可被 errors.Is 识别的原因。
func (e *Engine) Submit(spec experiment.Spec) (*experiment.Record, error) {
	if err := spec.ValidateAdmission(); err != nil {
		return nil, err
	}
	targets, err := e.registry.Resolve(spec.Targets)
	if err != nil {
		return nil, err
	}
	if err := enforceBlastRadius(spec.Blast, targets, e.registry.All()); err != nil {
		return nil, err
	}

	e.mu.Lock()
	if _, exists := e.records[spec.ID]; exists {
		e.mu.Unlock()
		return nil, fmt.Errorf("%w: 实验 ID %s 已存在", chaos.ErrSpecInvalid, spec.ID)
	}
	// 冲突预检：reject 策略下任何目标被占用都直接拒绝，绝不后写覆盖。
	if spec.OnConflict == experiment.ConflictReject {
		for _, t := range targets {
			if owner, busy := e.leased[t.ID]; busy {
				e.mu.Unlock()
				return nil, fmt.Errorf("%w: 目标 %s 正被实验 %s 注入故障",
					chaos.ErrTargetConflict, t.ID, owner)
			}
		}
		// 关键：在持锁状态下立即同步预占全部目标。否则从"提交返回"到
		// 异步 goroutine 真正获取租约之间存在窗口，并发提交会全部通过预检
		// 造成故障叠加。预占后其它 reject 提交立刻被拒。
		for _, t := range targets {
			e.leased[t.ID] = spec.ID
		}
	}
	rec := &experiment.Record{
		Spec:      spec,
		Phase:     experiment.PhaseQueued,
		Targets:   map[string]*experiment.TargetCleanup{},
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}
	want := map[string]struct{}{}
	for _, t := range targets {
		rec.Targets[t.ID] = &experiment.TargetCleanup{TargetID: t.ID, State: experiment.TargetSelected}
		want[t.ID] = struct{}{}
	}
	e.records[spec.ID] = rec
	e.want[spec.ID] = want
	e.order = append(e.order, spec.ID)
	e.mu.Unlock()

	if err := e.persist(rec, experiment.EvAdmitted, "准入通过，进入队列"); err != nil {
		return nil, err
	}
	e.log.Printf("[实验 %s] 已准入: 故障=%s 目标数=%d 冲突策略=%s 恢复策略=%s",
		spec.ID, spec.Fault, len(targets), spec.OnConflict, spec.OnRecover)

	e.wg.Add(1)
	go e.run(spec.ID, targets, false)
	return e.snapshot(spec.ID), nil
}

// enforceBlastRadius 由引擎强制执行爆炸半径：同时受影响目标数不得超过
// max_targets，也不得超过注册表目标总数的 max_percent%。
func enforceBlastRadius(b experiment.BlastRadius, selected, all []*target.Target) error {
	n := len(selected)
	if n > b.MaxTargets {
		return fmt.Errorf("%w: 选中 %d 个目标，超过 max_targets=%d",
			chaos.ErrBlastRadiusExceeded, n, b.MaxTargets)
	}
	total := len(all)
	if n*100 > total*b.MaxPercent {
		return fmt.Errorf("%w: 选中 %d/%d 个目标，超过 max_percent=%d%%",
			chaos.ErrBlastRadiusExceeded, n, total, b.MaxPercent)
	}
	return nil
}

// Abort 手动请求中止实验。一次实验最多中止一次，重复中止返回 ErrAlreadyAborted。
func (e *Engine) Abort(id, reason string) error {
	e.mu.Lock()
	rec, ok := e.records[id]
	if !ok {
		e.mu.Unlock()
		return fmt.Errorf("%w: 实验 %s 不存在", chaos.ErrExperimentNotRunning, id)
	}
	if rec.Phase != experiment.PhaseQueued && rec.Phase != experiment.PhaseRunning {
		e.mu.Unlock()
		return fmt.Errorf("%w: 实验 %s 当前阶段=%s", chaos.ErrAlreadyAborted, id, rec.Phase)
	}
	if rec.AbortOnce {
		e.mu.Unlock()
		return fmt.Errorf("%w: 实验 %s", chaos.ErrAlreadyAborted, id)
	}
	rec.AbortOnce = true
	rec.AbortReason = reason
	if rec.Phase == experiment.PhaseRunning {
		rec.Phase = experiment.PhaseAborting
	}
	e.cond.Broadcast()
	e.mu.Unlock()
	_ = e.persist(rec, experiment.EvAbort, "手动中止: "+reason)
	if w, ok := e.waiters[id]; ok {
		w.StopExternal()
	}
	e.log.Printf("[实验 %s] 收到中止请求: %s", id, reason)
	return nil
}

// Status 返回实验状态快照（深拷贝，并发安全）。
func (e *Engine) Status(id string) (experiment.Record, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	rec, ok := e.records[id]
	if !ok {
		return experiment.Record{}, fmt.Errorf("%w: 实验 %s 不存在", chaos.ErrExperimentNotRunning, id)
	}
	return rec.Clone(), nil
}

// Stop 停止调度循环（用于进程退出）。已注入故障不会被自动撤销——
// 持久化保证重启后由恢复逻辑负责，避免"故障仍生效却无人负责"。
func (e *Engine) Stop() {
	e.stopOnce.Do(func() {
		close(e.stopCh)
		e.mu.Lock()
		e.cond.Broadcast()
		ws := make([]*observer.Waiter, 0, len(e.waiters))
		for _, w := range e.waiters {
			ws = append(ws, w)
		}
		e.mu.Unlock()
		if fc, ok := e.clock.(*observer.FakeClock); ok {
			fc.Stop()
		}
		for _, w := range ws {
			w.StopExternal()
		}
	})
}

// RetryCleanup 对终结实验中仍未回到基线的目标单独重试撤销。
// 已恢复的目标不受影响（撤销幂等）；全部目标恢复后返回 nil。
func (e *Engine) RetryCleanup(id string) error {
	e.mu.Lock()
	rec, ok := e.records[id]
	if !ok {
		e.mu.Unlock()
		return fmt.Errorf("%w: 实验 %s 不存在", chaos.ErrExperimentNotRunning, id)
	}
	var pending []string
	for tid, cs := range rec.Targets {
		if cs.State != experiment.TargetReverted {
			pending = append(pending, tid)
		}
	}
	e.mu.Unlock()

	var targets []*target.Target
	for _, tid := range pending {
		t, ok := e.registry.Get(tid)
		if !ok {
			continue
		}
		targets = append(targets, t)
	}
	if len(targets) == 0 {
		return nil
	}
	e.log.Printf("[实验 %s] 对 %d 个未恢复目标发起独立重试", id, len(targets))
	e.revertAll(id, targets)

	e.mu.Lock()
	defer e.mu.Unlock()
	for _, t := range targets {
		if rec.Targets[t.ID].State == experiment.TargetReverted {
			if owner, busy := e.leased[t.ID]; busy && owner == id {
				delete(e.leased, t.ID)
			}
		}
	}
	for _, cs := range rec.Targets {
		if cs.State != experiment.TargetReverted {
			return fmt.Errorf("%w: 目标 %s 仍未恢复: %s",
				chaos.ErrTargetConflict, cs.TargetID, cs.LastError)
		}
	}
	e.cond.Broadcast()
	return nil
}

// DirtyTargets 返回实验终结后仍未回到基线的目标 ID（供运维巡检）。
func (e *Engine) DirtyTargets(id string) []string {
	e.mu.Lock()
	defer e.mu.Unlock()
	rec, ok := e.records[id]
	if !ok {
		return nil
	}
	var out []string
	for _, cs := range rec.Targets {
		if cs.State != experiment.TargetReverted {
			out = append(out, cs.TargetID)
		}
	}
	return out
}

// Wait 阻塞到所有实验 goroutine 退出（恢复路径的同步撤销也在内）。
func (e *Engine) Wait() { e.wg.Wait() }

func (e *Engine) notify(evType string, rec experiment.Record) {
	e.listenerMu.Lock()
	l := append([]func(string, experiment.Record){}, e.listeners...)
	e.listenerMu.Unlock()
	for _, f := range l {
		f(evType, rec)
	}
}

func (e *Engine) snapshot(id string) *experiment.Record {
	e.mu.Lock()
	defer e.mu.Unlock()
	cp := e.records[id].Clone()
	return &cp
}
