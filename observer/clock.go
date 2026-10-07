package observer

import "sync"

// Clock 抽象时间，测试使用确定性的假时钟，不依赖真实等待。
type Clock interface {
	NowMs() int64
}

// waiterState 是跨多次 Wait 复用的等待登记。waiter 在 NewWaiter 时就
// 登记进时钟列表，因此创建之后的任何 Tick 都不会因"尚未登记"而丢失。
type waiterState struct {
	c    *FakeClock
	mu   sync.Mutex
	cond *sync.Cond
	gen  int64
	stop bool
}

// Waiter 是每个实验 runner 私有的 tick 等待器。
type Waiter struct{ s *waiterState }

// FakeClock 是手动推进的虚拟时钟。
type FakeClock struct {
	mu      sync.Mutex
	now     int64
	gen     int64
	waiters []*waiterState
}

func NewFakeClock(start int64) *FakeClock { return &FakeClock{now: start} }

func (c *FakeClock) NowMs() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

// NewWaiter 在时钟锁内创建并登记等待器，记录当前代号作为初始视角。
func (c *FakeClock) NewWaiter() *Waiter {
	c.mu.Lock()
	defer c.mu.Unlock()
	s := &waiterState{c: c, gen: c.gen}
	s.cond = sync.NewCond(&s.mu)
	c.waiters = append(c.waiters, s)
	return &Waiter{s: s}
}

// stop 标记 waiter 停止并广播（引擎停止 / 实验中止时调用）。
func (w *Waiter) stop() {
	w.s.mu.Lock()
	w.s.stop = true
	w.s.cond.Broadcast()
	w.s.mu.Unlock()
}

// StopExternal 供引擎在手动中止/停止时解除等待。
func (w *Waiter) StopExternal() { w.stop() }

// Wait 阻塞到代号超过 seen；stop 关闭时返回 ok=false。
//
// waiter 创建即登记，Tick 在自己的锁上更新代号并广播，"检查代号"与
// "cond.Wait"在 waiter 锁内原子完成，因此任何 Tick 都不会丢唤醒。
func (w *Waiter) Wait(seen int64) (int64, bool) {
	s := w.s
	s.mu.Lock()
	defer s.mu.Unlock()
	for s.gen <= seen && !s.stop {
		s.cond.Wait()
	}
	return s.gen, !s.stop
}

// Tick 推进 step 毫秒；持锁自增代号，锁外广播全部等待器。
func (c *FakeClock) Tick(step int64) int64 {
	c.mu.Lock()
	c.now += step
	c.gen++
	now, gen := c.now, c.gen
	ws := append([]*waiterState(nil), c.waiters...)
	c.mu.Unlock()
	for _, s := range ws {
		s.mu.Lock()
		if gen > s.gen {
			s.gen = gen
		}
		s.cond.Broadcast()
		s.mu.Unlock()
	}
	return now
}

// Stop 唤醒所有等待器（引擎停止时调用）。
func (c *FakeClock) Stop() {
	c.mu.Lock()
	ws := append([]*waiterState(nil), c.waiters...)
	c.mu.Unlock()
	for _, s := range ws {
		s.mu.Lock()
		s.stop = true
		s.cond.Broadcast()
		s.mu.Unlock()
	}
}

// Gen 返回当前代号。
func (c *FakeClock) Gen() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.gen
}

// Generation 返回当前 tick 代号。
func (c *FakeClock) Generation() int64 { return c.Gen() }
