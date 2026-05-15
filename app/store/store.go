package store

import "sync"

// Store is a thread-safe in-memory key-value store.
type Store struct {
	mu   sync.RWMutex
	data map[string]string
}

// New creates a new Store instance.
func New() *Store {
	return &Store{
		data: make(map[string]string),
	}
}

// Set stores a key-value pair.
func (s *Store) Set(key, value string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[key] = value
}

// Get retrieves the value for a key. Returns value and whether it exists.
func (s *Store) Get(key string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	val, ok := s.data[key]
	return val, ok
}
