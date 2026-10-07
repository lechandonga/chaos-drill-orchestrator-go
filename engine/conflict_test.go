package engine

import (
	"errors"
	"runtime"
	"sync"
	"testing"

	"github.com/lechandonga/chaos-drill-orchestrator-go/chaos"
	"github.com/lechandonga/chaos-drill-orchestrator-go/experiment"
)

// 业务：同一目标上互相冲突的故障不得叠加；reject 策略必须直接拒绝后来者，
// 而不是后写覆盖先写。
func TestConflictRejectDoesNotOverwrite(t *testing.T) {
	h := newHarness(t, "svc-a")
	eng := h.newEngine()

	first := h.baseSpec("first", []string{"svc-a"})
	if _, err := eng.Submit(first); err != nil {
		t.Fatal(err)
	}
	h.waitFor("first", experiment.EvInjected)

	second := h.baseSpec("second", []string{"svc-a"})
	_, err := eng.Submit(second)
	if !errors.Is(err, chaos.ErrTargetConflict) {
		t.Fatalf("重叠故障应被拒绝为 ErrTargetConflict，得到 %v", err)
	}

	tg, _ := h.reg.Get("svc-a")
	if tg.Lease() != "first" {
		t.Fatalf("先写实验必须仍持有目标，得到租约 %q", tg.Lease())
	}
	fault, ok := tg.Fault()
	if !ok || fault.LatencyMs != 100 {
		t.Fatalf("故障被覆盖或叠加: %+v ok=%v", fault, ok)
	}
}

// 业务：不相交目标集的多个实验可以真正并发运行（爆炸半径相互独立）。
func TestConcurrentNonOverlappingExperiments(t *testing.T) {
	h := newHarness(t, "svc-a", "svc-b")
	eng := h.newEngine()

	if _, err := eng.Submit(h.baseSpec("exp-a", []string{"svc-a"})); err != nil {
		t.Fatal(err)
	}
	if _, err := eng.Submit(h.baseSpec("exp-b", []string{"svc-b"})); err != nil {
		t.Fatal(err)
	}
	h.waitFor("exp-a", experiment.EvInjected)
	h.waitFor("exp-b", experiment.EvInjected)

	ta, _ := h.reg.Get("svc-a")
	tb, _ := h.reg.Get("svc-b")
	if ta.Lease() != "exp-a" || tb.Lease() != "exp-b" {
		t.Fatalf("两个不相交实验应并发持有目标: %q %q", ta.Lease(), tb.Lease())
	}
}

// 业务：queue 策略下重叠实验进入 FIFO 队列，先到者运行、后来者排队；
// 先到者结束后后来者才获取目标，顺序严格不乱。
func TestConflictQueueFIFO(t *testing.T) {
	h := newHarness(t, "svc-a")
	eng := h.newEngine()

	first := h.baseSpec("first", []string{"svc-a"})
	first.DurationMs = 3000
	if _, err := eng.Submit(first); err != nil {
		t.Fatal(err)
	}
	h.waitFor("first", experiment.EvInjected)

	second := h.baseSpec("second", []string{"svc-a"})
	second.OnConflict = experiment.ConflictQueue
	second.DurationMs = 1000
	if _, err := eng.Submit(second); err != nil {
		t.Fatalf("queue 策略提交不应报错: %v", err)
	}

	// 推进若干 tick：first 尚未结束时，second 必须仍排队，目标不叠加。
	for i := 0; i < 2; i++ {
		h.clk.Tick(1000)
		runtime.Gosched()
	}
	rec, _ := eng.Status("second")
	if rec.Phase != experiment.PhaseQueued {
		t.Fatalf("先来者未结束，后来者必须排队，得到 %s", rec.Phase)
	}
	tg, _ := h.reg.Get("svc-a")
	if tg.Lease() != "first" {
		t.Fatalf("排队期间目标必须仍属于 first，得到 %q", tg.Lease())
	}

	// first 在 3000ms 结束并清理后，second 才能注入。
	h.clk.Tick(1000)
	h.waitFor("first", experiment.EvFinished)
	h.waitFor("second", experiment.EvInjected)
	if tg.Lease() != "second" {
		t.Fatalf("first 结束后目标应交给 second，得到 %q", tg.Lease())
	}
}

// 业务：高并发提交同一批目标（reject 策略）时，最多一个成功，其余全部被
// 明确拒绝；任何时刻目标上至多一个故障（竞态检测下也稳定）。
func TestConcurrentRacingSubmissions(t *testing.T) {
	h := newHarness(t, "svc-a")
	eng := h.newEngine()

	const n = 16
	var wg sync.WaitGroup
	var mu sync.Mutex
	accepted, rejected := 0, 0
	wg.Add(n)
	for i := 0; i < n; i++ {
		i := i
		go func() {
			defer wg.Done()
			spec := h.baseSpec("race-"+itoa(i), []string{"svc-a"})
			if _, err := eng.Submit(spec); err == nil {
				mu.Lock()
				accepted++
				mu.Unlock()
			} else if errors.Is(err, chaos.ErrTargetConflict) {
				mu.Lock()
				rejected++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if accepted != 1 {
		t.Fatalf("恰好一个实验应被接受，实际 %d（拒绝 %d）", accepted, rejected)
	}
	if rejected != n-1 {
		t.Fatalf("其余 %d 个应冲突拒绝，实际 %d", n-1, rejected)
	}
	// 找到唯一胜出者并等待其完成注入，再校验目标被唯一持有。
	var winner string
	for i := 0; i < n; i++ {
		id := "race-" + itoa(i)
		if rec, err := eng.Status(id); err == nil && rec.Phase != experiment.PhaseRejected {
			winner = id
			break
		}
	}
	if winner == "" {
		t.Fatal("应当存在一个胜出实验")
	}
	h.waitFor(winner, experiment.EvInjected)
	tg, _ := h.reg.Get("svc-a")
	if tg.Lease() == "" {
		t.Fatalf("胜出实验应持有目标租约")
	}
	if tg.Lease() != winner {
		t.Fatalf("目标租约应属于胜出者 %s，得到 %s", winner, tg.Lease())
	}
	fault, ok := tg.Fault()
	if !ok || fault.LatencyMs != 100 {
		t.Fatalf("胜出者故障应唯一生效: %+v %v", fault, ok)
	}
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var b []byte
	for i > 0 {
		b = append([]byte{byte('0' + i%10)}, b...)
		i /= 10
	}
	return string(b)
}
