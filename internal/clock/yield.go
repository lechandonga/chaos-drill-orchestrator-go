package clock

import "runtime"

// yield lets goroutines woken by a fake-timer fire run before the next
// Advance; tests stay deterministic without sleeping for real durations.
func yield() {
	for range 5 {
		runtime.Gosched()
	}
}
