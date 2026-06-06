package main

import (
	"sync"
	"unsafe"
)

// WatchManager optimized version to reduce memory footprint during large-scale watch registration
type WatchManager struct {
	// Using sync.Map for better concurrent performance and lower memory overhead
	watchTable *sync.Map // path -> watchers mapping
	watchers   *sync.Map // watcher -> paths mapping
	mutex      sync.RWMutex
}

// Watcher represents a client watcher
type Watcher struct {
	id uintptr // Using uintptr for efficient storage
}

// NewWatchManager creates a new optimized WatchManager
func NewWatchManager() *WatchManager {
	return &WatchManager{
		watchTable: &sync.Map{},
		watchers:   &sync.Map{},
	}
}

// AddWatch adds a watch for a given path and watcher
func (wm *WatchManager) AddWatch(path string, watcher *Watcher) {
	wm.mutex.Lock()
	defer wm.mutex.Unlock()
	
	// Add to path -> watchers mapping
	if existing, ok := wm.watchTable.Load(path); ok {
		watchers := existing.(*sync.Map)
		watchers.Store(watcher, true)
	} else {
		watchers := &sync.Map{}
		watchers.Store(watcher, true)
		wm.watchTable.Store(path, watchers)
	}
	
	// Add to watcher -> paths mapping
	if existing, ok := wm.watchers.Load(watcher); ok {
		paths := existing.(*sync.Map)
		paths.Store(path, true)
	} else {
		paths := &sync.Map{}
		paths.Store(path, true)
		wm.watchers.Store(watcher, paths)
	}
}

// RemoveWatch removes a specific watch
func (wm *WatchManager) RemoveWatch(path string, watcher *Watcher) {
	wm.mutex.Lock()
	defer wm.mutex.Unlock()
	
	// Remove from path -> watchers mapping
	if watchers, ok := wm.watchTable.Load(path); ok {
		watchers.(*sync.Map).Delete(watcher)
	}
	
	// Remove from watcher -> paths mapping
	if paths, ok := wm.watchers.Load(watcher); ok {
		paths.(*sync.Map).Delete(path)
	}
}

// RemoveWatchesByWatcher removes all watches for a specific watcher
func (wm *WatchManager) RemoveWatchesByWatcher(watcher *Watcher) {
	wm.mutex.Lock()
	defer wm.mutex.Unlock()
	
	// Get all paths for this watcher and remove them
	if paths, ok := wm.watchers.Load(watcher); ok {
		paths.(*sync.Map).Range(func(key, value interface{}) bool {
			path := key.(string)
			// Remove watcher from path's watcher list
			if watchers, ok := wm.watchTable.Load(path); ok {
				watchers.(*sync.Map).Delete(watcher)
			}
			return true
		})
	}
	
	// Remove watcher from global watcher map
	wm.watchers.Delete(watcher)
}

// RemoveWatchesByPath removes all watches for a specific path
func (wm *WatchManager) RemoveWatchesByPath(path string) {
	wm.mutex.Lock()
	defer wm.mutex.Unlock()
	
	// Get all watchers for this path and remove them
	if watchers, ok := wm.watchTable.Load(path); ok {
		watchers.(*sync.Map).Range(func(key, value interface{}) bool {
			watcher := key.(*Watcher)
			// Remove path from watcher's path list
			if paths, ok := wm.watchers.Load(watcher); ok {
				paths.(*sync.Map).Delete(path)
			}
			return true
		})
	}
	
	// Remove path from global watch table
	wm.watchTable.Delete(path)
}

// GetWatchers returns all watchers for a path
func (wm *WatchManager) GetWatchers(path string) []*Watcher {
	var result []*Watcher
	wm.mutex.RLock()
	defer wm.mutex.RUnlock()
	
	if watchers, ok := wm.watchTable.Load(path); ok {
		watchers.(*sync.Map).Range(func(key, value interface{}) bool {
			result = append(result, key.(*Watcher))
			return true
		})
	}
	
	return result
}

// GetPaths returns all paths watched by a watcher
func (wm *WatchManager) GetPaths(watcher *Watcher) []string {
	var result []string
	wm.mutex.RLock()
	defer wm.mutex.RUnlock()
	
	if paths, ok := wm.watchers.Load(watcher); ok {
		paths.(*sync.Map).Range(func(key, value interface{}) bool {
			result = append(result, key.(string))
			return true
		})
	}
	
	return result
}

// GetWatcherCount returns the total number of active watchers
func (wm *WatchManager) GetWatcherCount() int {
	count := 0
	wm.watchers.Range(func(key, value interface{}) bool {
		count++
		return true
	})
	return count
}

// GetWatchTableSize returns the total number of watched paths
func (wm *WatchManager) GetWatchTableSize() int {
	count := 0
	wm.watchTable.Range(func(key, value interface{}) bool {
		count++
		return true
	})
	return count
}

// Memory optimization: Use object pooling for frequently created watcher objects
var watcherPool = sync.Pool{
	New: func() interface{} {
		return &Watcher{}
	},
}

// NewWatcher creates a new watcher, using object pooling to reduce GC pressure
func NewWatcher() *Watcher {
	return watcherPool.Get().(*Watcher)
}

// ReleaseWatcher returns a watcher to the pool
func ReleaseWatcher(w *Watcher) {
	// Reset the watcher state before returning to pool
	*w = Watcher{}
	watcherPool.Put(w)
}

func main() {
	// Example usage
	wm := NewWatchManager()
	
	// Create watchers using the pool
	watcher1 := NewWatcher()
	watcher2 := NewWatcher()
	
	// Add some watches
	wm.AddWatch("/path1", watcher1)
	wm.AddWatch("/path2", watcher1)
	wm.AddWatch("/path1", watcher2)
	
	// Use the watches...
	
	// Clean up when done
	ReleaseWatcher(watcher1)
	ReleaseWatcher(watcher2)
}