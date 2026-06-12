package main

import (
	"fmt"
	"sync"

	"github.com/jeromyritchie52/Apache-ZooKeeper/internal/collections"
)

// WatchManager is responsible for managing watches in ZooKeeper.
// It maintains a bidirectional mapping between paths and watchers.
type WatchManager struct {
	mu         sync.RWMutex
	watchTable  map[string]collections.CompactSet[string]
	watch2Paths map[string]collections.CompactSet[string]
	pathInterner *stringInterner
}

// NewWatchManager returns a new instance of WatchManager.
func NewWatchManager() *WatchManager {
	return &WatchManager{
		watchTable:  make(map[string]collections.CompactSet[string]),
		watch2Paths: make(map[string]collections.CompactSet[string]),
		pathInterner: newStringInterner(),
	}
}

// AddWatch adds a new watch for the given path and watcher.
func (wm *WatchManager) AddWatch(path string, watcher string) {
	wm.mu.Lock()
	defer wm.mu.Unlock()

	// Intern the path string to avoid duplicates
	internedPath := wm.pathInterner.intern(path)

	// Get or create the set of watchers for the path
	watchers, ok := wm.watchTable[internedPath]
	if !ok {
		watchers = collections.NewCompactSet[string]()
		wm.watchTable[internedPath] = watchers
	}

	// Get or create the set of paths for the watcher
	paths, ok := wm.watch2Paths[watcher]
	if !ok {
		paths = collections.NewCompactSet[string]()
		wm.watch2Paths[watcher] = paths
	}

	// Add the watcher to the path's set and the path to the watcher's set
	watchers.Add(watcher)
	paths.Add(internedPath)
}

// RemoveWatch removes a watch for the given path and watcher.
func (wm *WatchManager) RemoveWatch(path string, watcher string) {
	wm.mu.Lock()
	defer wm.mu.Unlock()

	// Intern the path string
	internedPath := wm.pathInterner.intern(path)

	// Remove the watcher from the path's set
	watchers, ok := wm.watchTable[internedPath]
	if ok {
		watchers.Remove(watcher)
		if watchers.IsEmpty() {
			delete(wm.watchTable, internedPath)
		}
	}

	// Remove the path from the watcher's set
	paths, ok := wm.watch2Paths[watcher]
	if ok {
		paths.Remove(internedPath)
		if paths.IsEmpty() {
			delete(wm.watch2Paths, watcher)
		}
	}
}

type stringInterner struct {
	mu    sync.RWMutex
	cache map[string]string
}

func newStringInterner() *stringInterner {
	return &stringInterner{
		cache: make(map[string]string),
	}
}

func (si *stringInterner) intern(s string) string {
	si.mu.RLock()
	defer si.mu.RUnlock()

	// Check if the string is already in the cache
	interned, ok := si.cache[s]
	if ok {
		return interned
	}

	si.mu.RUnlock()
	si.mu.Lock()
	defer si.mu.Unlock()

	// Double-check if the string was interned while we were waiting for the lock
	interned, ok = si.cache[s]
	if ok {
		return interned
	}

	// Intern the string and add it to the cache
	interned = s
	si.cache[s] = interned
	return interned
}
