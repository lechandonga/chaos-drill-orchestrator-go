package store

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"

	"github.com/lechandonga/chaos-drill-orchestrator-go/experiment"
)

// Store 是进程重启后识别未完成实验的持久化层：
// 每次状态变更先追加事件（WAL，fsync），再原子替换快照文件。
// 即使在写入中途崩溃，重启也能依据快照+事件重建一致状态。
type Store struct {
	mu       sync.Mutex
	dir      string
	seq      int
	snapPath string
}

type snapshot struct {
	Seq     int                          `json:"seq"`
	Records map[string]experiment.Record `json:"records"`
}

func Open(dir string) (*Store, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("打开存储目录失败: %w", err)
	}
	s := &Store{dir: dir, snapPath: filepath.Join(dir, "snapshot.json")}
	if err := s.load(); err != nil {
		return nil, err
	}
	return s, nil
}

// Memory 返回纯内存存储，供不关心跨进程恢复的测试使用。
func Memory() *Store {
	s, _ := OpenTemp()
	return s
}

// OpenTemp 在临时目录建存储，测试结束可清理。
func OpenTemp() (*Store, error) {
	dir, err := os.MkdirTemp("", "chaos-store-*")
	if err != nil {
		return nil, err
	}
	return Open(dir)
}

// Dir 返回存储目录，便于测试结束后清理。
func (s *Store) Dir() string { return s.dir }

func (s *Store) load() error {
	data, err := os.ReadFile(s.snapPath)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	var sn snapshot
	if err := json.Unmarshal(data, &sn); err != nil {
		return fmt.Errorf("快照损坏: %w", err)
	}
	s.seq = sn.Seq
	// 快照之外可能还有 fsync 过但未并入快照的事件（崩溃在压缩前），
	// 逐条重放保证不丢状态。
	events, err := s.readEventsAfter(sn.Seq)
	if err != nil {
		return err
	}
	for _, ev := range events {
		s.seq = ev.Seq
	}
	return nil
}

func (s *Store) readEventsAfter(after int) ([]experiment.Event, error) {
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return nil, err
	}
	var out []experiment.Event
	for _, e := range entries {
		var seq int
		if _, err := fmt.Sscanf(e.Name(), "event-%d.json", &seq); err != nil || seq <= after {
			continue
		}
		data, err := os.ReadFile(filepath.Join(s.dir, e.Name()))
		if err != nil {
			return nil, err
		}
		var ev experiment.Event
		if err := json.Unmarshal(data, &ev); err != nil {
			return nil, fmt.Errorf("事件 %s 损坏: %w", e.Name(), err)
		}
		out = append(out, ev)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Seq < out[j].Seq })
	return out, nil
}

// LoadAll 返回持久化的全部实验记录（深拷贝）。
func (s *Store) LoadAll() (map[string]experiment.Record, []experiment.Event, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var sn snapshot
	if data, err := os.ReadFile(s.snapPath); err == nil {
		if err := json.Unmarshal(data, &sn); err != nil {
			return nil, nil, fmt.Errorf("快照损坏: %w", err)
		}
	} else if !os.IsNotExist(err) {
		return nil, nil, err
	}
	if sn.Records == nil {
		sn.Records = map[string]experiment.Record{}
	}
	events, err := s.readEventsAfter(sn.Seq)
	if err != nil {
		return nil, nil, err
	}
	out := make(map[string]experiment.Record, len(sn.Records))
	for id, r := range sn.Records {
		out[id] = r.Clone()
	}
	return out, events, nil
}

// AppendEvent 持久化一条事件（崩溃前可见的最后一道保证）。
func (s *Store) AppendEvent(ev experiment.Event) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.seq++
	ev.Seq = s.seq
	data, err := json.MarshalIndent(ev, "", "  ")
	if err != nil {
		return err
	}
	path := filepath.Join(s.dir, fmt.Sprintf("event-%d.json", ev.Seq))
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		return err
	}
	return s.syncDir()
}

// SaveSnapshot 原子写入快照。
func (s *Store) SaveSnapshot(records map[string]experiment.Record) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	sn := snapshot{Seq: s.seq, Records: records}
	data, err := json.MarshalIndent(sn, "", "  ")
	if err != nil {
		return err
	}
	tmp := s.snapPath + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	if err := os.Rename(tmp, s.snapPath); err != nil {
		return err
	}
	return s.syncDir()
}

func (s *Store) syncDir() error {
	d, err := os.Open(s.dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
