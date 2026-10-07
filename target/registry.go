package target

import (
	"fmt"
	"sort"
	"sync"

	"github.com/lechandonga/chaos-drill-orchestrator-go/chaos"
)

// Selector 用显式 ID 列表和/或标签选择目标。
type Selector struct {
	TargetIDs []string          `json:"target_ids,omitempty"`
	Labels    map[string]string `json:"labels,omitempty"`
}

func (s Selector) empty() bool {
	return len(s.TargetIDs) == 0 && len(s.Labels) == 0
}

// Empty 判断选择器是否未声明任何范围。
func (s Selector) Empty() bool { return s.empty() }

// Registry 是受控目标的注册表，实验准入时必须在此解析选择器，
// 目标不存在直接拒绝，绝不允许凭空创建。
type Registry struct {
	mu      sync.RWMutex
	targets map[string]*Target
}

func NewRegistry() *Registry {
	return &Registry{targets: map[string]*Target{}}
}

// Register 注册一个受控目标，重复 ID 返回错误。
func (r *Registry) Register(id string, labels map[string]string) (*Target, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if id == "" {
		return nil, fmt.Errorf("%w: 目标 ID 不能为空", chaos.ErrSpecInvalid)
	}
	if _, ok := r.targets[id]; ok {
		return nil, fmt.Errorf("目标 %s 已注册", id)
	}
	cp := make(map[string]string, len(labels))
	for k, v := range labels {
		cp[k] = v
	}
	t := newTarget(id, cp)
	r.targets[id] = t
	return t, nil
}

// MustRegister 用于测试/演示装配，重复注册直接 panic。
func (r *Registry) MustRegister(id string, labels map[string]string) *Target {
	t, err := r.Register(id, labels)
	if err != nil {
		panic(err)
	}
	return t
}

// Get 按 ID 取目标。
func (r *Registry) Get(id string) (*Target, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	t, ok := r.targets[id]
	return t, ok
}

// All 返回全部目标（ID 有序），供比例类爆炸半径计算使用。
func (r *Registry) All() []*Target {
	r.mu.RLock()
	defer r.mu.RUnlock()
	ids := make([]string, 0, len(r.targets))
	for id := range r.targets {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	out := make([]*Target, 0, len(ids))
	for _, id := range ids {
		out = append(out, r.targets[id])
	}
	return out
}

// Resolve 解析选择器：任何显式列出但不存在的 ID 都会导致整体拒绝
// （返回 chaos.ErrTargetNotFound），结果按 ID 排序以保证确定性。
func (r *Registry) Resolve(sel Selector) ([]*Target, error) {
	if sel.Empty() {
		return nil, fmt.Errorf("%w: 目标选择器不能为空（必须显式给出 target_ids 或 labels）",
			chaos.ErrSpecInvalid)
	}
	r.mu.RLock()
	defer r.mu.RUnlock()

	chosen := map[string]*Target{}
	for _, id := range sel.TargetIDs {
		t, ok := r.targets[id]
		if !ok {
			return nil, fmt.Errorf("%w: %s", chaos.ErrTargetNotFound, id)
		}
		chosen[id] = t
	}
	for _, t := range r.targets {
		if matchLabels(t.Labels, sel.Labels) {
			chosen[t.ID] = t
		}
	}
	ids := make([]string, 0, len(chosen))
	for id := range chosen {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	out := make([]*Target, 0, len(ids))
	for _, id := range ids {
		out = append(out, chosen[id])
	}
	return out, nil
}

func matchLabels(labels, want map[string]string) bool {
	if len(want) == 0 {
		return false
	}
	for k, v := range want {
		if labels[k] != v {
			return false
		}
	}
	return true
}
