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
	mu      sync.RWMutex
	data    map[string]entry
	lists   map[string][]string
	waiters map[string][]chan string // blocked BLPOP clients, per key (FIFO order)
}

// New creates a new Store instance.
func New() *Store {
	return &Store{
		data:    make(map[string]entry),
		lists:   make(map[string][]string),
		waiters: make(map[string][]chan string),
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

// notifyWaiters checks if any BLPOP clients are waiting on this key and serves them.
// Must be called with s.mu held (write lock).
func (s *Store) notifyWaiters(key string) {
	for len(s.lists[key]) > 0 && len(s.waiters[key]) > 0 {
		val := s.lists[key][0]
		s.lists[key] = s.lists[key][1:]
		ch := s.waiters[key][0]
		s.waiters[key] = s.waiters[key][1:]
		ch <- val // buffered channel (cap 1), won't block
	}
}

// RPush appends values to the end of a list and returns the new list length.
// If the list doesn't exist, it is created first.
// Notifies any blocked BLPOP clients.
func (s *Store) RPush(key string, values ...string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lists[key] = append(s.lists[key], values...)
	total := len(s.lists[key]) // capture BEFORE waiters consume elements
	s.notifyWaiters(key)
	return total
}

// LPush prepends values to the start of a list and returns the new list length.
// Values are inserted in reverse order, so the last argument ends up at the front.
// If the list doesn't exist, it is created first.
// Notifies any blocked BLPOP clients.
func (s *Store) LPush(key string, values ...string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i, j := 0, len(values)-1; i < j; i, j = i+1, j-1 {
		values[i], values[j] = values[j], values[i]
	}
	s.lists[key] = append(values, s.lists[key]...)
	total := len(s.lists[key]) // capture BEFORE waiters consume elements
	s.notifyWaiters(key)
	return total

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

	if start < 0 {
		start = length + start
	}
	if stop < 0 {
		stop = length + stop
	}
	if start < 0 {
		start = 0
	}
	if start >= length {
		return []string{}
	}
	if stop >= length {
		stop = length - 1
	}
	if start > stop {
		return []string{}
	}

	return list[start : stop+1]
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
	val := list[0]
	s.lists[key] = list[1:]
	return val, true
}

// LPopN removes and returns the first n elements of a list.
// If n exceeds the list length, all elements are removed and returned.
// Returns an empty slice if the list doesn't exist or is empty.
func (s *Store) LPopN(key string, count int) []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	list, ok := s.lists[key]
	if !ok || len(list) == 0 {
		return []string{}
	}
	if count > len(list) {
		count = len(list)
	}
	popped := list[:count]
	s.lists[key] = list[count:]
	return popped
}

// BLPop blocks until an element is available in the list or the timeout expires.
// timeout=0 means block indefinitely.
// Returns ("", false) on timeout.
func (s *Store) BLPop(key string, timeout time.Duration) (string, bool) {
	s.mu.Lock()

	// Try immediate pop — no need to block if element already exists
	list := s.lists[key]
	if len(list) > 0 {
		val := list[0]
		s.lists[key] = list[1:]
		s.mu.Unlock()
		return val, true
	}

	// Register as a waiter (buffered so RPush won't block when sending)
	ch := make(chan string, 1)
	s.waiters[key] = append(s.waiters[key], ch)
	s.mu.Unlock()

	// Block indefinitely
	if timeout == 0 {
		val := <-ch
		return val, true
	}

	// Block with timeout
	select {
	case val := <-ch:
		return val, true
	case <-time.After(timeout):
		// Remove our channel from waiters (RPush may have already removed it)
		s.mu.Lock()
		for i, w := range s.waiters[key] {
			if w == ch {
				s.waiters[key] = append(s.waiters[key][:i], s.waiters[key][i+1:]...)
				break
			}
		}
		s.mu.Unlock()
		// Drain any value sent after timeout fired (race condition safety)
		select {
		case <-ch:
		default:
		}
		return "", false
	}
}

// Type returns the Redis type of the value stored at key.
// Returns "string", "list", or "none" if the key doesn't exist.
func (s *Store) Type(key string) string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if e, ok := s.data[key]; ok {
		// Check expiry for string keys
		if !e.hasExpiry || time.Now().Before(e.expiresAt) {
			return "string"
		}
	}
	if _, ok := s.lists[key]; ok {
		return "list"
	}
	return "none"
}
