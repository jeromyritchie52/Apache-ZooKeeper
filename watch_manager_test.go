package main

import (
	"fmt"
	"runtime"
	"sync"
	"testing"
)

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// testWatcher is a minimal Watcher implementation used in tests.
type testWatcher struct {
	id int32
}

func (tw *testWatcher) Process(_ WatcherEvent) {}

// generatePaths produces n distinct path strings.
func generatePaths(n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = fmt.Sprintf("/app/%d/config/node-%d/%d", i%100, i%1000, i)
	}
	return out
}

// ---------------------------------------------------------------------------
// Unit tests — WatchManager
// ---------------------------------------------------------------------------

func TestAddWatch(t *testing.T) {
	wm := NewWatchManager()
	w := &testWatcher{id: 1}

	wm.AddWatch("/test", w)
	if got := wm.WatchCount(); got != 1 {
		t.Fatalf("WatchCount = %d, want 1", got)
	}
	if got := wm.WatcherCount(); got != 1 {
		t.Fatalf("WatcherCount = %d, want 1", got)
	}
}

func TestAddWatch_DuplicatePath(t *testing.T) {
	wm := NewWatchManager()
	w1 := &testWatcher{id: 1}
	w2 := &testWatcher{id: 2}

	wm.AddWatch("/test", w1)
	wm.AddWatch("/test", w2)

	if got := wm.WatchCount(); got != 1 {
		t.Fatalf("WatchCount = %d, want 1", got)
	}
	if got := wm.WatcherCount(); got != 2 {
		t.Fatalf("WatcherCount = %d, want 2", got)
	}
}

func TestRemoveWatcher(t *testing.T) {
	wm := NewWatchManager()
	w := &testWatcher{id: 1}

	wm.AddWatch("/a", w)
	wm.AddWatch("/b", w)
	wm.RemoveWatcher(w)

	if got := wm.WatchCount(); got != 0 {
		t.Fatalf("WatchCount after remove = %d, want 0", got)
	}
	if got := wm.WatcherCount(); got != 0 {
		t.Fatalf("WatcherCount after remove = %d, want 0", got)
	}
}

func TestTriggerWatch(t *testing.T) {
	wm := NewWatchManager()
	w := &testWatcher{id: 1}

	wm.AddWatch("/trigger", w)
	triggered := wm.TriggerWatch("/trigger")

	if len(triggered) != 1 {
		t.Fatalf("len(triggered) = %d, want 1", len(triggered))
	}
	if triggered[0] != w {
		t.Fatal("triggered wrong watcher")
	}
	// Path should be fully cleaned up.
	if got := wm.WatchCount(); got != 0 {
		t.Fatalf("WatchCount after trigger = %d, want 0", got)
	}
}

func TestTriggerWatch_MultipleWatchers(t *testing.T) {
	wm := NewWatchManager()
	w1 := &testWatcher{id: 1}
	w2 := &testWatcher{id: 2}

	wm.AddWatch("/shared", w1)
	wm.AddWatch("/shared", w2)
	triggered := wm.TriggerWatch("/shared")

	if len(triggered) != 2 {
		t.Fatalf("len(triggered) = %d, want 2", len(triggered))
	}
	if wm.WatchCount() != 0 {
		t.Fatal("path not cleaned up after trigger")
	}
	// Both watchers are no longer watching anything.
	if wm.WatcherCount() != 0 {
		t.Fatal("watchers not cleaned up after trigger")
	}
}

func TestTriggerWatch_UnknownPath(t *testing.T) {
	wm := NewWatchManager()
	triggered := wm.TriggerWatch("/nonexistent")
	if triggered != nil {
		t.Fatal("expected nil for unknown path")
	}
}

func TestWatcherWatchingMultiplePaths(t *testing.T) {
	wm := NewWatchManager()
	w := &testWatcher{id: 1}

	paths := []string{"/a", "/b", "/c"}
	for _, p := range paths {
		wm.AddWatch(p, w)
	}

	if got := wm.WatchCount(); got != 3 {
		t.Fatalf("WatchCount = %d, want 3", got)
	}
	if got := wm.WatcherCount(); got != 1 {
		t.Fatalf("WatcherCount = %d, want 1", got)
	}

	// Trigger one path — watcher should still be alive for the other two.
	wm.TriggerWatch("/a")
	if got := wm.WatchCount(); got != 2 {
		t.Fatalf("WatchCount after trigger = %d, want 2", got)
	}
	if got := wm.WatcherCount(); got != 1 {
		t.Fatalf("WatcherCount after trigger = %d, want 1", got)
	}
}

func TestPathDeduplication(t *testing.T) {
	wm := NewWatchManager()
	w1 := &testWatcher{id: 1}
	w2 := &testWatcher{id: 2}

	// Same logical path, different string instances.
	p1 := string([]byte("/app/config"))
	p2 := string([]byte("/app/config"))

	wm.AddWatch(p1, w1)
	wm.AddWatch(p2, w2)

	// PathCache should have interned both to the same entry.
	if got := wm.PathCacheSize(); got != 1 {
		t.Fatalf("PathCacheSize = %d, want 1 (dedup failed)", got)
	}

	if got := wm.WatchCount(); got != 1 {
		t.Fatalf("WatchCount = %d, want 1", got)
	}

	triggered := wm.TriggerWatch("/app/config")
	if len(triggered) != 2 {
		t.Fatalf("len(triggered) = %d, want 2", len(triggered))
	}
}

func TestConcurrentAddAndTrigger(t *testing.T) {
	wm := NewWatchManager()
	var wg sync.WaitGroup
	n := 100
	m := 100

	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < n; i++ {
			w := &testWatcher{id: int32(i)}
			paths := generatePaths(m)
			for _, p := range paths {
				wm.AddWatch(p, w)
			}
		}
	}()
	go func() {
		defer wg.Done()
		paths := generatePaths(m)
		for i := 0; i < n; i++ {
			for _, p := range paths {
				wm.TriggerWatch(p)
			}
		}
	}()
	wg.Wait()
	// No panics = test passes.
}

// ---------------------------------------------------------------------------
// Memory leak detection
// ---------------------------------------------------------------------------

func TestNoMemoryLeakAfterFullCleanup(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping memory-leak test in short mode")
	}

	wm := NewWatchManager()
	nPaths := 1000
	nWatchers := 100

	paths := generatePaths(nPaths)
	watchers := make([]Watcher, nWatchers)
	for j := range watchers {
		watchers[j] = &testWatcher{id: int32(j)}
	}

	for _, p := range paths {
		for _, w := range watchers {
			wm.AddWatch(p, w)
		}
	}

	// Remove each watcher using the same pointer instances.
	for _, w := range watchers {
		wm.RemoveWatcher(w)
	}

	// The maps should be empty.
	if got := wm.WatchCount(); got != 0 {
		t.Fatalf("WatchCount after full cleanup = %d, want 0", got)
	}
	if got := wm.WatcherCount(); got != 0 {
		t.Fatalf("WatcherCount after full cleanup = %d, want 0", got)
	}
}

// ---------------------------------------------------------------------------
// Large-scale watch benchmark (1,000,000 watches)
// ---------------------------------------------------------------------------

func BenchmarkLargeScaleWatch(b *testing.B) {
	wm := NewWatchManager()
	nPaths := 10_000
	nWatchers := 100

	paths := make([]string, nPaths)
	for i := range paths {
		paths[i] = fmt.Sprintf("/app/%d/service/config", i)
	}

	watchers := make([]Watcher, nWatchers)
	for i := range watchers {
		watchers[i] = &testWatcher{id: int32(i)}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Register 1M watches.
		for _, p := range paths {
			for _, w := range watchers {
				wm.AddWatch(p, w)
			}
		}

		// Trigger every path.
		for _, p := range paths {
			wm.TriggerWatch(p)
		}
	}
}

// ---------------------------------------------------------------------------
// Heap allocation benchmark — measure memory saved by PathCache
// ---------------------------------------------------------------------------

func BenchmarkPathCacheDeduplication(b *testing.B) {
	paths := generatePaths(10_000)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		pc := NewPathCache()
		for _, p := range paths {
			pc.Intern(p)
		}
	}
}

// ---------------------------------------------------------------------------
// Memory footprint measurement (for 1,000,000 watches)
// ---------------------------------------------------------------------------

func TestMemoryFootprintOneMillionWatches(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping memory footprint test in short mode")
	}

	var memBefore, memAfter runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&memBefore)

	wm := NewWatchManager()
	nPaths := 10_000
	nWatchers := 100

	paths := make([]string, nPaths)
	for i := range paths {
		paths[i] = fmt.Sprintf("/app/%d/service/config", i)
	}
	watchers := make([]Watcher, nWatchers)
	for i := range watchers {
		watchers[i] = &testWatcher{id: int32(i)}
	}

	// Register 1,000,000 watches.
	for _, p := range paths {
		for _, w := range watchers {
			wm.AddWatch(p, w)
		}
	}

	runtime.GC()
	runtime.ReadMemStats(&memAfter)

	allocDelta := int64(memAfter.Alloc) - int64(memBefore.Alloc)
	allocMB := float64(allocDelta) / (1024 * 1024)
	t.Logf("Heap allocation for 1,000,000 watches: %.2f MB", allocMB)

	// Just informative — not a hard pass/fail.
	_ = allocMB
}

// ---------------------------------------------------------------------------
// CompactSet unit tests
// ---------------------------------------------------------------------------

func TestCompactSet_AddContains(t *testing.T) {
	s := NewCompactSet[int](3)
	if !s.Add(1) {
		t.Fatal("expected Add(1) to return true")
	}
	if s.Add(1) {
		t.Fatal("expected duplicate Add(1) to return false")
	}
	if !s.Contains(1) {
		t.Fatal("expected Contains(1) after add")
	}
	if s.Contains(99) {
		t.Fatal("expected !Contains(99)")
	}
}

func TestCompactSet_Remove(t *testing.T) {
	s := NewCompactSet[int](3)
	s.Add(1)
	s.Add(2)
	if !s.Remove(1) {
		t.Fatal("expected Remove(1) true")
	}
	if s.Remove(1) {
		t.Fatal("expected Remove(1) again false")
	}
	if s.Size() != 1 {
		t.Fatalf("Size = %d, want 1", s.Size())
	}
}

func TestCompactSet_UpgradeToMap(t *testing.T) {
	s := NewCompactSet[int](3)
	for i := 0; i < 5; i++ {
		s.Add(i)
	}
	if s.Size() != 5 {
		t.Fatalf("Size = %d, want 5", s.Size())
	}
	// After upgrade all elements should still be present.
	for i := 0; i < 5; i++ {
		if !s.Contains(i) {
			t.Fatalf("Contains(%d) false after upgrade", i)
		}
	}
}

func TestCompactSet_ForEach(t *testing.T) {
	s := NewCompactSet[int](3)
	for i := 0; i < 10; i++ {
		s.Add(i)
	}
	var sum int
	s.ForEach(func(v int) { sum += v })
	if sum != 45 {
		t.Fatalf("ForEach sum = %d, want 45", sum)
	}
}

func TestCompactSet_Clear(t *testing.T) {
	s := NewCompactSet[int](3)
	for i := 0; i < 10; i++ {
		s.Add(i)
	}
	s.Clear()
	if s.Size() != 0 {
		t.Fatal("expected empty after Clear")
	}
}

func TestCompactSet_ToSlice(t *testing.T) {
	s := NewCompactSet[int](3)
	s.Add(1)
	s.Add(2)
	sl := s.ToSlice()
	if len(sl) != 2 {
		t.Fatalf("len(slice) = %d, want 2", len(sl))
	}
}

// ---------------------------------------------------------------------------
// Benchmark: CompactSet vs raw map for small sizes
// ---------------------------------------------------------------------------

func BenchmarkCompactSet_Small(b *testing.B) {
	for i := 0; i < b.N; i++ {
		s := NewCompactSet[string](3)
		s.Add("/app/config")
		s.Add("/app/status")
		_ = s.Contains("/app/config")
	}
}

func BenchmarkRawMap_Small(b *testing.B) {
	for i := 0; i < b.N; i++ {
		m := make(map[string]struct{}, 2)
		m["/app/config"] = struct{}{}
		m["/app/status"] = struct{}{}
		_, _ = m["/app/config"]
	}
}
