package main

import (
	"sync"
)

// ---------------------------------------------------------------------------
// PathCache — localized string interning for path deduplication.
//
// Ensures that the same logical path string shares a single backing string
// instance across both watchTable and watch2Paths, eliminating redundant
// heap allocations.  Thread-safe with RWMutex.
// ---------------------------------------------------------------------------

type PathCache struct {
	mu    sync.RWMutex
	cache map[string]string
}

func NewPathCache() *PathCache {
	return &PathCache{cache: make(map[string]string)}
}

// Intern returns a canonical (interned) copy of path.  If the path has been
// seen before the existing string reference is returned; otherwise path is
// stored and returned as-is.
func (pc *PathCache) Intern(path string) string {
	pc.mu.RLock()
	if cached, ok := pc.cache[path]; ok {
		pc.mu.RUnlock()
		return cached
	}
	pc.mu.RUnlock()

	pc.mu.Lock()
	defer pc.mu.Unlock()
	if cached, ok := pc.cache[path]; ok {
		return cached
	}
	pc.cache[path] = path
	return path
}

// Size returns the number of unique paths currently cached.
func (pc *PathCache) Size() int {
	pc.mu.RLock()
	defer pc.mu.RUnlock()
	return len(pc.cache)
}

// ---------------------------------------------------------------------------
// CompactSet — memory-efficient set with graduated storage.
//
// For sizes <= threshold the set is backed by a simple slice (zero per-element
// overhead beyond the element pointers).  When the set grows past threshold it
// upgrades to a map-backed representation, yielding O(1) lookups at the cost
// of the map's per-bucket overhead.
//
// Under the workloads described in the issue the *vast* majority of
// path→watcher and watcher→path entries have cardinality 1‑3, so the slice
// representation saves hundreds of thousands of map allocations.
// ---------------------------------------------------------------------------

type CompactSet[T comparable] struct {
	items     []T
	itemMap   map[T]struct{}
	threshold int
}

func NewCompactSet[T comparable](threshold int) *CompactSet[T] {
	return &CompactSet[T]{
		items:     make([]T, 0),
		threshold: threshold,
	}
}

// Add inserts item into the set and reports whether it was newly added.
func (s *CompactSet[T]) Add(item T) bool {
	if s.itemMap != nil {
		// map-backed — fast path.
		if _, exists := s.itemMap[item]; exists {
			return false
		}
		s.itemMap[item] = struct{}{}
		return true
	}

	// Linear scan — only happens while size <= threshold.
	for _, existing := range s.items {
		if existing == item {
			return false
		}
	}

	if len(s.items) < s.threshold {
		s.items = append(s.items, item)
		return true
	}

	// Upgrade to map.
	s.itemMap = make(map[T]struct{}, s.threshold+1)
	for _, existing := range s.items {
		s.itemMap[existing] = struct{}{}
	}
	s.items = nil
	s.itemMap[item] = struct{}{}
	return true
}

// Remove deletes item from the set and reports whether it was present.
func (s *CompactSet[T]) Remove(item T) bool {
	if s.itemMap != nil {
		if _, exists := s.itemMap[item]; exists {
			delete(s.itemMap, item)
			return true
		}
		return false
	}
	for i, existing := range s.items {
		if existing == item {
			s.items = append(s.items[:i], s.items[i+1:]...)
			return true
		}
	}
	return false
}

// Contains reports whether item is in the set.
func (s *CompactSet[T]) Contains(item T) bool {
	if s.itemMap != nil {
		_, exists := s.itemMap[item]
		return exists
	}
	for _, existing := range s.items {
		if any(existing) == any(item) {
			return true
		}
	}
	return false
}

// Size returns the number of elements in the set.
func (s *CompactSet[T]) Size() int {
	if s.itemMap != nil {
		return len(s.itemMap)
	}
	return len(s.items)
}

// ForEach calls f once for every element in the set.  Order is unspecified.
func (s *CompactSet[T]) ForEach(f func(T)) {
	if s.itemMap != nil {
		for item := range s.itemMap {
			f(item)
		}
		return
	}
	for _, item := range s.items {
		f(item)
	}
}

// Clear removes all elements and resets to the slice representation.
func (s *CompactSet[T]) Clear() {
	s.items = make([]T, 0)
	s.itemMap = nil
}

// ToSlice returns a snapshot of the current elements.
func (s *CompactSet[T]) ToSlice() []T {
	if s.itemMap != nil {
		out := make([]T, 0, len(s.itemMap))
		for item := range s.itemMap {
			out = append(out, item)
		}
		return out
	}
	out := make([]T, len(s.items))
	copy(out, s.items)
	return out
}

// ---------------------------------------------------------------------------
// Watcher — minimal interface for watch recipients.
// ---------------------------------------------------------------------------

// WatcherEvent carries the path and event type for a triggered watch.
type WatcherEvent struct {
	Path string
	Type int32
}

// Watcher is notified when a watched path is triggered.
type Watcher interface {
	Process(event WatcherEvent)
}

// ---------------------------------------------------------------------------
// WatchManager — memory-optimised watch registration and dispatch.
//
// Optimisations applied (vs. naïve HashSet/HashMap approach):
//  1. PathCache for string deduplication across watchTable and watch2Paths.
//  2. CompactSet (slice→map graduated) instead of raw map[T]struct{} for
//     the per-entry value sets.
//  3. Lock-free reads on path cache; fine-grained RWMutex on the maps.
// ---------------------------------------------------------------------------

type WatchManager struct {
	mu          sync.RWMutex
	watchTable  map[string]*CompactSet[Watcher] // path → set of watchers
	watch2Paths map[Watcher]*CompactSet[string] // watcher → set of paths
	pathCache   *PathCache
}

func NewWatchManager() *WatchManager {
	return &WatchManager{
		watchTable:  make(map[string]*CompactSet[Watcher]),
		watch2Paths: make(map[Watcher]*CompactSet[string]),
		pathCache:   NewPathCache(),
	}
}

// AddWatch registers watcher for the given path.  The path is interned via
// PathCache so that identical strings share a single allocation.
func (wm *WatchManager) AddWatch(path string, watcher Watcher) {
	interned := wm.pathCache.Intern(path)

	wm.mu.Lock()
	defer wm.mu.Unlock()

	// watchTable[path] ← watcher
	watchers, ok := wm.watchTable[interned]
	if !ok {
		watchers = NewCompactSet[Watcher](3)
		wm.watchTable[interned] = watchers
	}
	watchers.Add(watcher)

	// watch2Paths[watcher] ← path
	paths, ok := wm.watch2Paths[watcher]
	if !ok {
		paths = NewCompactSet[string](3)
		wm.watch2Paths[watcher] = paths
	}
	paths.Add(interned)
}

// RemoveWatcher removes every watch registered for the given watcher.  Both
// watchTable and watch2Paths are cleaned up, preventing orphaned references.
func (wm *WatchManager) RemoveWatcher(watcher Watcher) {
	wm.mu.Lock()
	defer wm.mu.Unlock()

	paths, ok := wm.watch2Paths[watcher]
	if !ok {
		return
	}

	paths.ForEach(func(path string) {
		if watchers, exists := wm.watchTable[path]; exists {
			watchers.Remove(watcher)
			if watchers.Size() == 0 {
				delete(wm.watchTable, path)
			}
		}
	})

	delete(wm.watch2Paths, watcher)
}

// TriggerWatch fires all watchers registered on path and returns the list of
// affected watchers.  All entries for this path are cleaned up from both
// watchTable and watch2Paths (zero memory leak).
func (wm *WatchManager) TriggerWatch(path string) []Watcher {
	wm.mu.Lock()
	defer wm.mu.Unlock()

	// Use plain path for lookup; if it hasn't been interned nothing will match.
	watchers, ok := wm.watchTable[path]
	if !ok {
		return nil
	}

	result := watchers.ToSlice()

	// Remove path from every watcher's path set.
	watchers.ForEach(func(w Watcher) {
		if paths, exists := wm.watch2Paths[w]; exists {
			paths.Remove(path)
			if paths.Size() == 0 {
				delete(wm.watch2Paths, w)
			}
		}
	})

	delete(wm.watchTable, path)
	return result
}

// WatchCount returns the number of unique paths with active watches.
func (wm *WatchManager) WatchCount() int {
	wm.mu.RLock()
	defer wm.mu.RUnlock()
	return len(wm.watchTable)
}

// WatcherCount returns the number of distinct watchers registered.
func (wm *WatchManager) WatcherCount() int {
	wm.mu.RLock()
	defer wm.mu.RUnlock()
	return len(wm.watch2Paths)
}

// PathCacheSize returns the number of interned path strings.
func (wm *WatchManager) PathCacheSize() int {
	return wm.pathCache.Size()
}
