package engine

import (
	"fmt"
	"sync"
)

// safeLog 是并发安全的测试日志。引擎 goroutine 生命周期可能略长于
// 单个测试函数，直接调用 testing.T.Logf 会被竞态检测器标记，因此自行加锁。
type safeLog struct {
	mu  sync.Mutex
	buf []string
}

func (s *safeLog) Printf(f string, a ...any) {
	s.mu.Lock()
	s.buf = append(s.buf, fmt.Sprintf(f, a...))
	s.mu.Unlock()
}

type logTB interface{ Log(args ...any) }

func (s *safeLog) dump(t logTB) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, line := range s.buf {
		t.Log(line)
	}
}
