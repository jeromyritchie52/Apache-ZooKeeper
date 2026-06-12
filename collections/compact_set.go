package collections

import (
	"sort"
)

// CompactSet is a compact, array-backed set implementation.
type CompactSet[T comparable] struct {
	elements []T
}

// NewCompactSet returns a new instance of CompactSet.
func NewCompactSet[T comparable]() CompactSet[T] {
	return CompactSet[T]{}
}

// Add adds an element to the set.
func (cs *CompactSet[T]) Add(element T) {
	// Check if the element is already in the set
	idx := cs.indexOf(element)
	if idx != -1 {
		return
	}

	// Add the element to the set
	cs.elements = append(cs.elements, element)
	sort.Ints(cs.elements) // Assume T is sortable
}

// Remove removes an element from the set.
func (cs *CompactSet[T]) Remove(element T) {
	idx := cs.indexOf(element)
	if idx == -1 {
		return
	}

	// Remove the element from the set
	cs.elements = append(cs.elements[:idx], cs.elements[idx+1:]...)
}

// IsEmpty checks if the set is empty.
func (cs *CompactSet[T]) IsEmpty() bool {
	return len(cs.elements) == 0
}

func (cs *CompactSet[T]) indexOf(element T) int {
	for i, e := range cs.elements {
		if e == element {
			return i
		}
	}
	return -1
}
