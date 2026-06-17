package main

import (
	"fmt"
	"runtime"
)

func main() {
	fmt.Println("Apache ZooKeeper — Optimized WatchManager Demo")
	fmt.Println("==============================================")

	wm := NewWatchManager()

	// Create some watchers.
	w1 := &simpleWatcher{id: "watcher-1"}
	w2 := &simpleWatcher{id: "watcher-2"}
	w3 := &simpleWatcher{id: "watcher-3"}

	// Register watches.
	wm.AddWatch("/app/config/database", w1)
	wm.AddWatch("/app/config/database", w2)
	wm.AddWatch("/app/config/cache", w1)
	wm.AddWatch("/app/health", w3)

	fmt.Printf("\nRegistered watches:\n")
	fmt.Printf("  WatchCount:  %d unique paths\n", wm.WatchCount())
	fmt.Printf("  WatcherCount: %d unique watchers\n", wm.WatcherCount())
	fmt.Printf("  PathCacheSize: %d interned paths\n", wm.PathCacheSize())

	// Trigger a watch.
	fmt.Printf("\nTriggering /app/config/database...\n")
	triggered := wm.TriggerWatch("/app/config/database")
	fmt.Printf("  %d watchers notified\n", len(triggered))

	fmt.Printf("\nAfter trigger:\n")
	fmt.Printf("  WatchCount:  %d unique paths\n", wm.WatchCount())
	fmt.Printf("  WatcherCount: %d unique watchers\n", wm.WatcherCount())

	// Large-scale simulation.
	fmt.Printf("\n--- Large-Scale Simulation ---\n")
	simulateLargeScale(wm)

	// Memory stats.
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	fmt.Printf("\nMemory: Alloc=%.2f MB  TotalAlloc=%.2f MB  Mallocs=%d\n",
		float64(m.Alloc)/1e6, float64(m.TotalAlloc)/1e6, m.Mallocs)
	fmt.Println("\n✅ WatchManager initialized and operational.")
}

// simpleWatcher is a demo watcher.
type simpleWatcher struct {
	id string
}

func (sw *simpleWatcher) Process(event WatcherEvent) {
	fmt.Printf("    [%s] received event: path=%s type=%d\n", sw.id, event.Path, event.Type)
}

func simulateLargeScale(wm *WatchManager) {
	watchers := make([]Watcher, 100)
	for i := range watchers {
		watchers[i] = &simpleWatcher{id: fmt.Sprintf("scale-watcher-%d", i)}
	}

	// Register 10,000 paths × 100 watchers = 1,000,000 watches.
	fmt.Printf("Registering 1,000,000 watches (10,000 paths × 100 watchers)...\n")
	for i := 0; i < 10_000; i++ {
		path := fmt.Sprintf("/app/%d/service/config", i)
		for _, w := range watchers {
			wm.AddWatch(path, w)
		}
	}

	fmt.Printf("  WatchCount:     %d\n", wm.WatchCount())
	fmt.Printf("  WatcherCount:   %d\n", wm.WatcherCount())
	fmt.Printf("  PathCacheSize:  %d (deduplicated)\n", wm.PathCacheSize())

	// Trigger all paths.
	fmt.Printf("Triggering all 10,000 paths...\n")
	for i := 0; i < 10_000; i++ {
		path := fmt.Sprintf("/app/%d/service/config", i)
		wm.TriggerWatch(path)
	}

	fmt.Printf("  WatchCount after cleanup: %d\n", wm.WatchCount())
	fmt.Printf("  WatcherCount after cleanup: %d\n", wm.WatcherCount())
}
