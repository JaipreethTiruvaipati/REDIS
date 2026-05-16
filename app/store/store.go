package store

import (
	"sync"
	"time"
)

// entry holds a string value and an optional expiry time.
type entry struct {
	value     string
	expiresAt time.Time
	hasExpiry bool
}

// Store is a thread-safe in-memory key-value store supporting strings and lists.
type Store struct {
	mu    sync.RWMutex
	data  map[string]entry    // string keys
	lists map[string][]string // list keys
}

// New creates a new Store instance.
func New() *Store {
	return &Store{
		data:  make(map[string]entry),
		lists: make(map[string][]string),
	}
}

// Set stores a key-value string pair with no expiry.
func (s *Store) Set(key, value string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[key] = entry{value: value}
}

// SetWithExpiry stores a key-value string pair that expires after the given duration.
func (s *Store) SetWithExpiry(key, value string, ttl time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[key] = entry{
		value:     value,
		expiresAt: time.Now().Add(ttl),
		hasExpiry: true,
	}
}

// Get retrieves the string value for a key. Returns ("", false) if missing or expired.
func (s *Store) Get(key string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	e, ok := s.data[key]
	if !ok {
		return "", false
	}
	if e.hasExpiry && time.Now().After(e.expiresAt) {
		return "", false
	}
	return e.value, true
}

// RPush appends values to the end of a list and returns the new list length.
// If the list doesn't exist, it is created first.
func (s *Store) RPush(key string, values ...string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lists[key] = append(s.lists[key], values...)
	return len(s.lists[key])
}

// LRange returns elements from a list between start and stop (inclusive).
// Supports negative indexes: -1 is the last element, -2 second-to-last, etc.
// Returns an empty slice if the list doesn't exist or indices are out of range.
func (s *Store) LRange(key string, start, stop int) []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	list, ok := s.lists[key]
	if !ok {
		return []string{}
	}

	length := len(list)

	// Convert negative indexes to positive
	if start < 0 {
		start = length + start
	}
	if stop < 0 {
		stop = length + stop
	}

	// Clamp start to 0 if still negative (was out of range)
	if start < 0 {
		start = 0
	}

	// If start is beyond the list, return empty
	if start >= length {
		return []string{}
	}

	// Clamp stop to last valid index
	if stop >= length {
		stop = length - 1
	}

	// If start > stop, return empty
	if start > stop {
		return []string{}
	}

	return list[start : stop+1]
}

// LPush prepends values to the start of a list and returns the new list length.
// Values are inserted in reverse order, so the last argument ends up at the front.
// If the list doesn't exist, it is created first.
func (s *Store) LPush(key string, values ...string) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Reverse values so the last argument ends up at the front
	// e.g., LPUSH list a b c → [c, b, a]
	for i, j := 0, len(values)-1; i < j; i, j = i+1, j-1 {
		values[i], values[j] = values[j], values[i]
	}

	s.lists[key] = append(values, s.lists[key]...)
	return len(s.lists[key])
}

// LLen returns the length of a list. Returns 0 if the list doesn't exist.
func (s *Store) LLen(key string) int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.lists[key])
}

// LPop removes and returns the first element of a list.
// Returns ("", false) if the list doesn't exist or is empty.
func (s *Store) LPop(key string) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	list, ok := s.lists[key]
	if !ok || len(list) == 0 {
		return "", false
	}

	val := list[0]          // grab the first element
	s.lists[key] = list[1:] // shrink the list by removing index 0
	return val, true
}
