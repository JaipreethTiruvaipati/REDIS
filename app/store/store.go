package store

import (
	"fmt"
	"sync"
	"time"

	"github.com/codecrafters-io/redis-starter-go/app/stream"
	"github.com/codecrafters-io/redis-starter-go/app/zset"
)

// entry holds a string value and an optional expiry time.
type entry struct {
	value     string
	expiresAt time.Time
	hasExpiry bool
}

// Store is a thread-safe in-memory key-value store supporting strings and lists.
type Store struct {
	mu            sync.RWMutex
	data          map[string]entry
	lists         map[string][]string
	waiters       map[string][]chan string // blocked BLPOP clients, per key (FIFO order)
	streams       map[string]*stream.Stream
	streamWaiters map[string][]streamWaiter // blocked XREAD clients, per key (FIFO order)
	zsets         map[string]*zset.ZSet
}

// streamWaiter represents a blocked XREAD client waiting for new stream entries.
// Each blocking client gets its own channel and tracks which ID it's waiting after.
type streamWaiter struct {
	afterID stream.EntryID    // only entries with ID > afterID will wake this waiter
	ch      chan stream.Entry // buffered channel: receives the new entry when available
}

// New creates a new Store instance.
func New() *Store {
	return &Store{
		data:          make(map[string]entry),
		lists:         make(map[string][]string),
		waiters:       make(map[string][]chan string),
		streams:       make(map[string]*stream.Stream),
		streamWaiters: make(map[string][]streamWaiter),
		zsets:         make(map[string]*zset.ZSet),
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
	if _, ok := s.streams[key]; ok {
		return "stream"
	}
	return "none"
}

// XAdd appends an entry to a stream. Creates the stream if it doesn't exist.
// Supports explicit IDs and auto-sequence IDs ("ms-*").
// Returns the entry ID as a string, or an error if the ID is invalid.
func (s *Store) XAdd(key, idStr string, fields []string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.streams[key]; !exists {
		s.streams[key] = stream.New()
	}

	st := s.streams[key]
	var id stream.EntryID

	if stream.IsAutoFull(idStr) {
		// Fully auto-generated: "*"
		id = stream.GenerateFull(st.LastID)
	} else if stream.IsAutoSeq(idStr) {
		// Auto-generate sequence number: "ms-*"
		ms, err := stream.ParseAutoSeqMs(idStr)
		if err != nil {
			return "", err
		}
		id = stream.GenerateSeq(ms, st.LastID)

		// Validate: generated ID must still be > lastID (e.g. ms < lastID.ms fails)
		if !st.LastID.IsZero() && !st.LastID.LessThan(id) {
			return "", fmt.Errorf("The ID specified in XADD is equal or smaller than the target stream top item")
		}
	} else {
		// Explicit ID: parse and validate
		var err error
		id, err = stream.Parse(idStr)
		if err != nil {
			return "", err
		}

		// 0-0 is always invalid with its specific message
		if id.IsZero() {
			return "", fmt.Errorf("The ID specified in XADD must be greater than 0-0")
		}

		// Must be strictly greater than last entry
		if !st.LastID.IsZero() && !st.LastID.LessThan(id) {
			return "", fmt.Errorf("The ID specified in XADD is equal or smaller than the target stream top item")
		}
	}

	st.Add(id, fields)
	// Wake up any XREAD BLOCK clients waiting for new entries on this stream
	s.notifyStreamWaiters(key, st.Entries[len(st.Entries)-1])
	return id.String(), nil

}

// XRange returns all stream entries with IDs between start and end (inclusive).
// Returns an empty slice if the stream doesn't exist or no entries match.
func (s *Store) XRange(key string, start, end stream.EntryID) []stream.Entry {
	s.mu.RLock()
	defer s.mu.RUnlock()

	st, ok := s.streams[key]
	if !ok {
		return []stream.Entry{}
	}

	var result []stream.Entry
	for _, e := range st.Entries {
		// Include entry if: start <= e.ID <= end
		if !e.ID.LessThan(start) && !end.LessThan(e.ID) {
			result = append(result, e)
		}
	}
	return result
}

// XRead returns entries from one or more streams that are strictly after the given IDs.
// Results only include streams that have at least one matching entry.
func (s *Store) XRead(keys []string, afterIDs []stream.EntryID) []stream.ReadResult {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var results []stream.ReadResult
	for i, key := range keys {
		afterID := afterIDs[i]
		st, ok := s.streams[key]
		if !ok {
			continue
		}

		var entries []stream.Entry
		for _, e := range st.Entries {
			if afterID.LessThan(e.ID) { // exclusive: only entries AFTER afterID
				entries = append(entries, e)
			}
		}

		if len(entries) > 0 {
			results = append(results, stream.ReadResult{Key: key, Entries: entries})
		}
	}
	return results
}

// notifyStreamWaiters wakes up any blocked XREAD clients waiting on this key
// if the new entry satisfies their afterID condition.
// Must be called with s.mu held (write lock).
func (s *Store) notifyStreamWaiters(key string, entry stream.Entry) {
	var remaining []streamWaiter
	for _, w := range s.streamWaiters[key] {
		if w.afterID.LessThan(entry.ID) {
			// This entry is newer than what the waiter is looking for → wake them up
			w.ch <- entry // buffered channel (cap 1), won't block
		} else {
			// This waiter is still waiting (its afterID is >= new entry)
			remaining = append(remaining, w)
		}
	}
	s.streamWaiters[key] = remaining
}

// BXRead blocks until a new entry arrives after afterID in the given stream,
// or until the timeout expires. timeout=0 blocks indefinitely.
// Returns the matching entries and true, or nil and false on timeout.
func (s *Store) BXRead(key string, afterID stream.EntryID, timeout time.Duration) ([]stream.Entry, bool) {
	s.mu.Lock()

	// STEP 1: Try immediate read — no blocking needed if entries already exist
	if st, ok := s.streams[key]; ok {
		var entries []stream.Entry
		for _, e := range st.Entries {
			if afterID.LessThan(e.ID) {
				entries = append(entries, e)
			}
		}
		if len(entries) > 0 {
			s.mu.Unlock()
			return entries, true
		}
	}

	// STEP 2: No entries yet — register as a waiter with a personal channel
	ch := make(chan stream.Entry, 1) // buffered so XAdd won't block when sending
	s.streamWaiters[key] = append(s.streamWaiters[key], streamWaiter{
		afterID: afterID,
		ch:      ch,
	})
	s.mu.Unlock()

	// STEP 3: Sleep until an entry arrives or timeout fires
	if timeout == 0 {
		// Block indefinitely (XREAD BLOCK 0)
		entry := <-ch
		return []stream.Entry{entry}, true
	}

	select {
	case entry := <-ch:
		// Woken up by XAdd
		return []stream.Entry{entry}, true

	case <-time.After(timeout):
		// Timeout expired — clean up our waiter
		s.mu.Lock()
		for i, w := range s.streamWaiters[key] {
			if w.ch == ch {
				s.streamWaiters[key] = append(
					s.streamWaiters[key][:i],
					s.streamWaiters[key][i+1:]...,
				)
				break
			}
		}
		s.mu.Unlock()
		// Drain the channel in case XAdd sent to it right as timeout fired
		select {
		case <-ch:
		default:
		}
		return nil, false
	}
}

// GetStreamLastID returns the last entry ID for a stream.
// Returns a zero EntryID {0, 0} if the stream doesn't exist or is empty.
func (s *Store) GetStreamLastID(key string) stream.EntryID {
	s.mu.RLock()
	defer s.mu.RUnlock()
	st, ok := s.streams[key]
	if !ok {
		return stream.EntryID{} // zero value = 0-0
	}
	return st.LastID
}

// ZAdd adds a member with a score to the sorted set stored at key.
func (s *Store) ZAdd(key string, score float64, member string) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	zs, ok := s.zsets[key]
	if !ok {
		zs = zset.New()
		s.zsets[key] = zs
	}

	return zs.Add(score, member)
}

// ZRank returns the rank of a member in the sorted set stored at key.
// Returns -1, false if the key or member does not exist.
func (s *Store) ZRank(key string, member string) (int, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	zs, ok := s.zsets[key]
	if !ok {
		return -1, false
	}

	rank := zs.Rank(member)
	if rank == -1 {
		return -1, false
	}
	return rank, true
}

// ZRange returns members within the specified rank range for the sorted set at key.
func (s *Store) ZRange(key string, start, stop int) []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	zs, ok := s.zsets[key]
	if !ok {
		return []string{} // Return empty slice if key doesn't exist
	}

	return zs.Range(start, stop)
}
