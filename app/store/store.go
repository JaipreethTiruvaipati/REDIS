package store

import (
	"sync"
	"time"
)

// entry holds a value and an optional expiry time.
type entry struct {
	value     string
	expiresAt time.Time
	hasExpiry bool
}

// Store is a thread-safe in-memory key-value store.
type Store struct {
	mu   sync.RWMutex
	data map[string]entry
}

// New creates a new Store instance.
func New() *Store {
	return &Store{
		data: make(map[string]entry),
	}
}

// Set stores a key-value pair with no expiry.
func (s *Store) Set(key, value string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[key] = entry{value: value}
}

// SetWithExpiry stores a key-value pair that expires after the given duration.
func (s *Store) SetWithExpiry(key, value string, ttl time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[key] = entry{
		value:     value,
		expiresAt: time.Now().Add(ttl),
		hasExpiry: true,
	}
}

// Get retrieves the value for a key. Returns ("", false) if missing or expired.
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
