package store

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/jaipreethtiruvaipati/redis-clone/app/stream"
	"github.com/jaipreethtiruvaipati/redis-clone/app/transactions"
	"github.com/jaipreethtiruvaipati/redis-clone/app/zset"
)

// ErrWrongType is returned when a command is used with a key holding another
// Redis data type.
var ErrWrongType = errors.New("WRONGTYPE Operation against a key holding the wrong kind of value")

const (
	TypeNone   = "none"
	TypeString = "string"
	TypeList   = "list"
	TypeStream = "stream"
	TypeZSet   = "zset"
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
	commandMu     sync.Mutex // serializes command execution and whole EXEC batches
	data          map[string]entry
	lists         map[string][]string
	waiters       map[string][]listWaiter // blocked BLPOP clients, per key (FIFO order)
	streams       map[string]*stream.Stream
	streamWaiters map[string][]streamWaiter // blocked XREAD clients, per key (FIFO order)
	zsets         map[string]*zset.ZSet
}

// WithCommandLock serializes a command (or an entire transaction batch) with
// other handler-dispatched commands. Store methods remain independently safe
// for callers that do not use the command layer.
func (s *Store) WithCommandLock(fn func()) {
	s.commandMu.Lock()
	defer s.commandMu.Unlock()
	fn()
}

// streamWaiter represents a blocked XREAD client waiting for new stream entries.
// Each blocking client gets its own channel and tracks which ID it's waiting after.
type streamWaiter struct {
	afterID stream.EntryID    // only entries with ID > afterID will wake this waiter
	ch      chan stream.Entry // buffered channel: receives the new entry when available
}

type listWaiterResult struct {
	value    string
	canceled bool
}

type listWaiter struct {
	ch chan listWaiterResult
}

type multiReadResult struct {
	key     string
	entries []stream.Entry
	err     error
}

type multiListResult struct {
	key   string
	value string
	ok    bool
	err   error
}

// New creates a new Store instance.
func New() *Store {
	return &Store{
		data:          make(map[string]entry),
		lists:         make(map[string][]string),
		waiters:       make(map[string][]listWaiter),
		streams:       make(map[string]*stream.Stream),
		streamWaiters: make(map[string][]streamWaiter),
		zsets:         make(map[string]*zset.ZSet),
	}
}

// keyTypeLocked returns the current type. It must be called with s.mu held.
// Expired strings are removed eagerly so every command observes them as absent.
func (s *Store) keyTypeLocked(key string) string {
	if e, ok := s.data[key]; ok {
		if e.hasExpiry && !time.Now().Before(e.expiresAt) {
			delete(s.data, key)
		} else {
			return TypeString
		}
	}
	if _, ok := s.lists[key]; ok {
		return TypeList
	}
	if _, ok := s.streams[key]; ok {
		return TypeStream
	}
	if _, ok := s.zsets[key]; ok {
		return TypeZSet
	}
	return TypeNone
}

// keyTypeLockedRead is the read-lock equivalent of keyTypeLocked. It cannot
// remove expired entries, but still reports them as nonexistent.
func (s *Store) keyTypeLockedRead(key string) string {
	if e, ok := s.data[key]; ok {
		if !e.hasExpiry || time.Now().Before(e.expiresAt) {
			return TypeString
		}
	}
	if _, ok := s.lists[key]; ok {
		return TypeList
	}
	if _, ok := s.streams[key]; ok {
		return TypeStream
	}
	if _, ok := s.zsets[key]; ok {
		return TypeZSet
	}
	return TypeNone
}

func (s *Store) ensureTypeLocked(key, want string) error {
	actual := s.keyTypeLocked(key)
	if actual != TypeNone && actual != want {
		return ErrWrongType
	}
	return nil
}

// Set stores a key-value string pair with no expiry.
func (s *Store) Set(key, value string) {
	_ = s.SetChecked(key, value)
}

// SetChecked stores a string, returning ErrWrongType if key has another type.
func (s *Store) SetChecked(key, value string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	// SET replaces an existing value regardless of its previous type.
	s.cancelListWaitersLocked(key)
	delete(s.lists, key)
	delete(s.streams, key)
	delete(s.zsets, key)
	s.data[key] = entry{value: value}
	return nil
}

// SetWithExpiry stores a key-value string pair that expires after the given duration.
func (s *Store) SetWithExpiry(key, value string, ttl time.Duration) {
	_ = s.SetWithExpiryChecked(key, value, ttl)
}

// SetWithExpiryChecked stores a string with a TTL, returning ErrWrongType if
// key has another type.
func (s *Store) SetWithExpiryChecked(key, value string, ttl time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	// SET with EX/PX also replaces an existing value regardless of type.
	s.cancelListWaitersLocked(key)
	delete(s.lists, key)
	delete(s.streams, key)
	delete(s.zsets, key)
	s.data[key] = entry{
		value:     value,
		expiresAt: time.Now().Add(ttl),
		hasExpiry: true,
	}
	return nil
}

// Get retrieves the string value for a key. Returns ("", false) if missing or expired.
func (s *Store) Get(key string) (string, bool) {
	value, ok, _ := s.GetChecked(key)
	return value, ok
}

// GetChecked retrieves a string and reports ErrWrongType for non-string keys.
func (s *Store) GetChecked(key string) (string, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.data[key]
	if !ok {
		if actual := s.keyTypeLocked(key); actual != TypeNone {
			return "", false, ErrWrongType
		}
		return "", false, nil
	}
	if e.hasExpiry && !time.Now().Before(e.expiresAt) {
		delete(s.data, key)
		return "", false, nil
	}
	return e.value, true, nil
}

// notifyWaiters checks if any BLPOP clients are waiting on this key and serves them.
// Must be called with s.mu held (write lock).
func (s *Store) notifyWaiters(key string) {
	for len(s.lists[key]) > 0 && len(s.waiters[key]) > 0 {
		val := s.lists[key][0]
		s.lists[key] = s.lists[key][1:]
		waiter := s.waiters[key][0]
		s.waiters[key] = s.waiters[key][1:]
		waiter.ch <- listWaiterResult{value: val} // buffered channel (cap 1), won't block
	}
}

func (s *Store) cancelListWaitersLocked(key string) {
	for _, ch := range s.waiters[key] {
		ch.ch <- listWaiterResult{canceled: true}
	}
	delete(s.waiters, key)
}

// RPush appends values to the end of a list and returns the new list length.
// If the list doesn't exist, it is created first.
// Notifies any blocked BLPOP clients.
func (s *Store) RPush(key string, values ...string) int {
	n, _ := s.RPushChecked(key, values...)
	return n
}

func (s *Store) RPushChecked(key string, values ...string) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.ensureTypeLocked(key, TypeList); err != nil {
		return 0, err
	}
	s.lists[key] = append(s.lists[key], values...)
	total := len(s.lists[key]) // capture BEFORE waiters consume elements
	s.notifyWaiters(key)
	if len(s.lists[key]) == 0 {
		delete(s.lists, key)
	}
	return total, nil
}

// LPush prepends values to the start of a list and returns the new list length.
// Values are inserted in reverse order, so the last argument ends up at the front.
// If the list doesn't exist, it is created first.
// Notifies any blocked BLPOP clients.
func (s *Store) LPush(key string, values ...string) int {
	n, _ := s.LPushChecked(key, values...)
	return n
}

func (s *Store) LPushChecked(key string, values ...string) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.ensureTypeLocked(key, TypeList); err != nil {
		return 0, err
	}
	values = append([]string(nil), values...)
	for i, j := 0, len(values)-1; i < j; i, j = i+1, j-1 {
		values[i], values[j] = values[j], values[i]
	}
	s.lists[key] = append(values, s.lists[key]...)
	total := len(s.lists[key]) // capture BEFORE waiters consume elements
	s.notifyWaiters(key)
	if len(s.lists[key]) == 0 {
		delete(s.lists, key)
	}
	return total, nil

}

// LRange returns elements from a list between start and stop (inclusive).
// Supports negative indexes: -1 is the last element, -2 second-to-last, etc.
// Returns an empty slice if the list doesn't exist or indices are out of range.
func (s *Store) LRange(key string, start, stop int) []string {
	items, _ := s.LRangeChecked(key, start, stop)
	return items
}

func (s *Store) LRangeChecked(key string, start, stop int) ([]string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if actual := s.keyTypeLockedRead(key); actual != TypeNone && actual != TypeList {
		return []string{}, ErrWrongType
	}
	list, ok := s.lists[key]
	if !ok {
		return []string{}, nil
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
		return []string{}, nil
	}
	if stop >= length {
		stop = length - 1
	}
	if start > stop {
		return []string{}, nil
	}

	return append([]string(nil), list[start:stop+1]...), nil
}

// LLen returns the length of a list. Returns 0 if the list doesn't exist.
func (s *Store) LLen(key string) int {
	n, _ := s.LLenChecked(key)
	return n
}

func (s *Store) LLenChecked(key string) (int, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if actual := s.keyTypeLockedRead(key); actual != TypeNone && actual != TypeList {
		return 0, ErrWrongType
	}
	return len(s.lists[key]), nil
}

// LPop removes and returns the first element of a list.
// Returns ("", false) if the list doesn't exist or is empty.
func (s *Store) LPop(key string) (string, bool) {
	val, ok, _ := s.LPopChecked(key)
	return val, ok
}

func (s *Store) LPopChecked(key string) (string, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.ensureTypeLocked(key, TypeList); err != nil {
		return "", false, err
	}
	list, ok := s.lists[key]
	if !ok || len(list) == 0 {
		return "", false, nil
	}
	val := list[0]
	s.lists[key] = list[1:]
	if len(s.lists[key]) == 0 {
		delete(s.lists, key)
	}
	return val, true, nil
}

// LPopN removes and returns the first n elements of a list.
// If n exceeds the list length, all elements are removed and returned.
// Returns an empty slice if the list doesn't exist or is empty.
func (s *Store) LPopN(key string, count int) []string {
	items, _ := s.LPopNChecked(key, count)
	return items
}

func (s *Store) LPopNChecked(key string, count int) ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.ensureTypeLocked(key, TypeList); err != nil {
		return []string{}, err
	}
	list, ok := s.lists[key]
	if !ok || len(list) == 0 {
		return []string{}, nil
	}
	if count < 0 {
		return []string{}, fmt.Errorf("count must not be negative")
	}
	if count > len(list) {
		count = len(list)
	}
	popped := list[:count]
	s.lists[key] = list[count:]
	if len(s.lists[key]) == 0 {
		delete(s.lists, key)
	}
	return append([]string(nil), popped...), nil
}

// BLPop blocks until an element is available in the list or the timeout expires.
// timeout=0 means block indefinitely.
// Returns ("", false) on timeout.
func (s *Store) BLPop(key string, timeout time.Duration) (string, bool) {
	val, ok, _ := s.BLPopContext(context.Background(), key, timeout)
	return val, ok
}

// BLPopContext is BLPop with cancellation support for server shutdown or a
// disconnected client.
func (s *Store) BLPopContext(ctx context.Context, key string, timeout time.Duration) (string, bool, error) {
	s.mu.Lock()
	if err := s.ensureTypeLocked(key, TypeList); err != nil {
		s.mu.Unlock()
		return "", false, err
	}

	// Try immediate pop — no need to block if element already exists
	list := s.lists[key]
	if len(list) > 0 {
		val := list[0]
		s.lists[key] = list[1:]
		if len(s.lists[key]) == 0 {
			delete(s.lists, key)
		}
		s.mu.Unlock()
		return val, true, nil
	}

	// Register as a waiter (buffered so RPush won't block when sending)
	ch := make(chan listWaiterResult, 1)
	s.waiters[key] = append(s.waiters[key], listWaiter{ch: ch})
	s.mu.Unlock()

	// Block indefinitely
	if timeout == 0 {
		select {
		case result := <-ch:
			if result.canceled {
				return "", false, nil
			}
			return result.value, true, nil
		case <-ctx.Done():
			s.removeListWaiter(key, ch)
			s.restoreListValueIfNotCanceled(key, ch)
			return "", false, ctx.Err()
		}
	}

	// Block with timeout
	select {
	case result := <-ch:
		if result.canceled {
			return "", false, nil
		}
		return result.value, true, nil
	case <-ctx.Done():
		s.removeListWaiter(key, ch)
		s.restoreListValueIfNotCanceled(key, ch)
		return "", false, ctx.Err()
	case <-time.After(timeout):
		// Remove our channel from waiters (RPush may have already removed it)
		s.mu.Lock()
		for i, w := range s.waiters[key] {
			if w.ch == ch {
				s.waiters[key] = append(s.waiters[key][:i], s.waiters[key][i+1:]...)
				break
			}
		}
		s.mu.Unlock()
		select {
		case result := <-ch:
			if !result.canceled {
				return result.value, true, nil
			}
		default:
		}
		return "", false, nil
	}
}

// BLPopMultiContext waits for the first available item across keys in key
// order. It preserves items delivered concurrently with timeout/cancellation
// by returning them to their list when the caller cannot consume them.
func (s *Store) BLPopMultiContext(ctx context.Context, keys []string, timeout time.Duration) (string, string, bool, error) {
	if len(keys) == 0 {
		return "", "", false, fmt.Errorf("at least one key is required")
	}
	for _, key := range keys {
		value, ok, err := s.LPopChecked(key)
		if err != nil {
			return "", "", false, err
		}
		if ok {
			return key, value, true, nil
		}
	}

	waitCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	results := make(chan multiListResult, len(keys))
	for _, key := range keys {
		go func(key string) {
			value, ok, err := s.BLPopContext(waitCtx, key, 0)
			results <- multiListResult{key: key, value: value, ok: ok, err: err}
		}(key)
	}

	var timeoutCh <-chan time.Time
	if timeout > 0 {
		timer := time.NewTimer(timeout)
		defer timer.Stop()
		timeoutCh = timer.C
	}
	remaining := len(keys)
	for remaining > 0 {
		select {
		case result := <-results:
			remaining--
			if result.err != nil && !errors.Is(result.err, context.Canceled) {
				cancel()
				s.drainMultiListResults(results, remaining)
				return "", "", false, result.err
			}
			if result.ok {
				return result.key, result.value, true, nil
			}
		case <-ctx.Done():
			cancel()
			s.drainMultiListResults(results, remaining)
			return "", "", false, ctx.Err()
		case <-timeoutCh:
			cancel()
			s.drainMultiListResults(results, remaining)
			return "", "", false, nil
		}
	}
	return "", "", false, nil
}

func (s *Store) drainMultiListResults(results <-chan multiListResult, remaining int) {
	for i := 0; i < remaining; i++ {
		result := <-results
		if result.ok {
			s.mu.Lock()
			s.lists[result.key] = append([]string{result.value}, s.lists[result.key]...)
			s.mu.Unlock()
		}
	}
}

func (s *Store) removeListWaiter(key string, ch chan listWaiterResult) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i, w := range s.waiters[key] {
		if w.ch == ch {
			s.waiters[key] = append(s.waiters[key][:i], s.waiters[key][i+1:]...)
			break
		}
	}
}

func (s *Store) restoreListValueIfNotCanceled(key string, ch chan listWaiterResult) {
	select {
	case result := <-ch:
		if result.canceled {
			return
		}
		s.mu.Lock()
		s.lists[key] = append([]string{result.value}, s.lists[key]...)
		s.mu.Unlock()
	default:
	}
}

// Type returns the Redis type of the value stored at key.
// Returns "string", "list", or "none" if the key doesn't exist.
func (s *Store) Type(key string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.keyTypeLocked(key)
}

// XAdd appends an entry to a stream. Creates the stream if it doesn't exist.
// Supports explicit IDs and auto-sequence IDs ("ms-*").
// Returns the entry ID as a string, or an error if the ID is invalid.
func (s *Store) XAdd(key, idStr string, fields []string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.ensureTypeLocked(key, TypeStream); err != nil {
		return "", err
	}

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

	st.Add(id, append([]string(nil), fields...))
	// Wake up any XREAD BLOCK clients waiting for new entries on this stream
	s.notifyStreamWaiters(key, st.Entries[len(st.Entries)-1])
	return id.String(), nil

}

// XRange returns all stream entries with IDs between start and end (inclusive).
// Returns an empty slice if the stream doesn't exist or no entries match.
func (s *Store) XRange(key string, start, end stream.EntryID) []stream.Entry {
	result, _ := s.XRangeChecked(key, start, end)
	return result
}

func (s *Store) XRangeChecked(key string, start, end stream.EntryID) ([]stream.Entry, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if actual := s.keyTypeLockedRead(key); actual != TypeNone && actual != TypeStream {
		return []stream.Entry{}, ErrWrongType
	}

	st, ok := s.streams[key]
	if !ok {
		return []stream.Entry{}, nil
	}

	var result []stream.Entry
	for _, e := range st.Entries {
		// Include entry if: start <= e.ID <= end
		if !e.ID.LessThan(start) && !end.LessThan(e.ID) {
			result = append(result, stream.Entry{ID: e.ID, Fields: append([]string(nil), e.Fields...)})
		}
	}
	return result, nil
}

// XRead returns entries from one or more streams that are strictly after the given IDs.
// Results only include streams that have at least one matching entry.
func (s *Store) XRead(keys []string, afterIDs []stream.EntryID) []stream.ReadResult {
	results, _ := s.XReadChecked(keys, afterIDs)
	return results
}

func (s *Store) XReadChecked(keys []string, afterIDs []stream.EntryID) ([]stream.ReadResult, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if len(keys) != len(afterIDs) {
		return nil, fmt.Errorf("keys and IDs must have the same length")
	}
	for _, key := range keys {
		if actual := s.keyTypeLockedRead(key); actual != TypeNone && actual != TypeStream {
			return nil, ErrWrongType
		}
	}

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
				entries = append(entries, stream.Entry{ID: e.ID, Fields: append([]string(nil), e.Fields...)})
			}
		}

		if len(entries) > 0 {
			results = append(results, stream.ReadResult{Key: key, Entries: entries})
		}
	}
	return results, nil
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
	entries, ok, _ := s.BXReadContext(context.Background(), key, afterID, timeout)
	return entries, ok
}

// BXReadMultiContext waits for any of the requested streams, then returns all
// entries currently available after their respective IDs. It preserves the
// connection cancellation and timeout semantics of BXReadContext.
func (s *Store) BXReadMultiContext(ctx context.Context, keys []string, afterIDs []stream.EntryID, timeout time.Duration) ([]stream.ReadResult, bool, error) {
	if len(keys) == 0 || len(keys) != len(afterIDs) {
		return nil, false, fmt.Errorf("keys and IDs must have the same length")
	}
	if results, err := s.XReadChecked(keys, afterIDs); err != nil {
		return nil, false, err
	} else if len(results) > 0 {
		return results, true, nil
	}

	waitCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	resultCh := make(chan multiReadResult, len(keys))
	for i, key := range keys {
		go func(key string, afterID stream.EntryID) {
			entries, ok, err := s.BXReadContext(waitCtx, key, afterID, 0)
			if ok {
				resultCh <- multiReadResult{key: key, entries: entries}
			} else {
				resultCh <- multiReadResult{key: key, err: err}
			}
		}(key, afterIDs[i])
	}

	var timeoutCh <-chan time.Time
	if timeout > 0 {
		timer := time.NewTimer(timeout)
		defer timer.Stop()
		timeoutCh = timer.C
	}
	select {
	case result := <-resultCh:
		if result.err != nil && !errors.Is(result.err, context.Canceled) {
			return nil, false, result.err
		}
		if result.entries == nil {
			return nil, false, nil
		}
		// Re-read all streams so simultaneous notifications are included and the
		// response has the same shape as non-blocking XREAD.
		results, err := s.XReadChecked(keys, afterIDs)
		if err != nil {
			return nil, false, err
		}
		return results, len(results) > 0, nil
	case <-ctx.Done():
		return nil, false, ctx.Err()
	case <-timeoutCh:
		return nil, false, nil
	}
}

// BXReadContext is BXRead with cancellation support.
func (s *Store) BXReadContext(ctx context.Context, key string, afterID stream.EntryID, timeout time.Duration) ([]stream.Entry, bool, error) {
	s.mu.Lock()
	if err := s.ensureTypeLocked(key, TypeStream); err != nil {
		s.mu.Unlock()
		return nil, false, err
	}

	// STEP 1: Try immediate read — no blocking needed if entries already exist
	if st, ok := s.streams[key]; ok {
		var entries []stream.Entry
		for _, e := range st.Entries {
			if afterID.LessThan(e.ID) {
				entries = append(entries, stream.Entry{ID: e.ID, Fields: append([]string(nil), e.Fields...)})
			}
		}
		if len(entries) > 0 {
			s.mu.Unlock()
			return entries, true, nil
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
		select {
		case entry := <-ch:
			return []stream.Entry{entry}, true, nil
		case <-ctx.Done():
			s.removeStreamWaiter(key, ch)
			return nil, false, ctx.Err()
		}
	}

	select {
	case entry := <-ch:
		// Woken up by XAdd
		return []stream.Entry{entry}, true, nil

	case <-ctx.Done():
		s.removeStreamWaiter(key, ch)
		return nil, false, ctx.Err()

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
		return nil, false, nil
	}
}

func (s *Store) removeStreamWaiter(key string, ch chan stream.Entry) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i, w := range s.streamWaiters[key] {
		if w.ch == ch {
			s.streamWaiters[key] = append(s.streamWaiters[key][:i], s.streamWaiters[key][i+1:]...)
			break
		}
	}
}

// GetStreamLastID returns the last entry ID for a stream.
// Returns a zero EntryID {0, 0} if the stream doesn't exist or is empty.
func (s *Store) GetStreamLastID(key string) stream.EntryID {
	id, _ := s.GetStreamLastIDChecked(key)
	return id
}

func (s *Store) GetStreamLastIDChecked(key string) (stream.EntryID, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if actual := s.keyTypeLockedRead(key); actual != TypeNone && actual != TypeStream {
		return stream.EntryID{}, ErrWrongType
	}
	st, ok := s.streams[key]
	if !ok {
		return stream.EntryID{}, nil // zero value = 0-0
	}
	return st.LastID, nil
}

// ZAdd adds a member with a score to the sorted set stored at key.
func (s *Store) ZAdd(key string, score float64, member string) int {
	n, _ := s.ZAddChecked(key, score, member)
	return n
}

func (s *Store) ZAddChecked(key string, score float64, member string) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.ensureTypeLocked(key, TypeZSet); err != nil {
		return 0, err
	}

	zs, ok := s.zsets[key]
	if !ok {
		zs = zset.New()
		s.zsets[key] = zs
	}

	return zs.Add(score, member), nil
}

// ZRank returns the rank of a member in the sorted set stored at key.
// Returns -1, false if the key or member does not exist.
func (s *Store) ZRank(key string, member string) (int, bool) {
	rank, ok, _ := s.ZRankChecked(key, member)
	return rank, ok
}

func (s *Store) ZRankChecked(key string, member string) (int, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if actual := s.keyTypeLockedRead(key); actual != TypeNone && actual != TypeZSet {
		return -1, false, ErrWrongType
	}

	zs, ok := s.zsets[key]
	if !ok {
		return -1, false, nil
	}

	rank := zs.Rank(member)
	if rank == -1 {
		return -1, false, nil
	}
	return rank, true, nil
}

// ZRange returns members within the specified rank range for the sorted set at key.
func (s *Store) ZRange(key string, start, stop int) []string {
	members, _ := s.ZRangeChecked(key, start, stop)
	return members
}

func (s *Store) ZRangeChecked(key string, start, stop int) ([]string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if actual := s.keyTypeLockedRead(key); actual != TypeNone && actual != TypeZSet {
		return []string{}, ErrWrongType
	}

	zs, ok := s.zsets[key]
	if !ok {
		return []string{}, nil // Return empty slice if key doesn't exist
	}

	return zs.Range(start, stop), nil
}

// ZCard returns the cardinality of the sorted set at key.
// Returns 0 if the key does not exist.
func (s *Store) ZCard(key string) int {
	n, _ := s.ZCardChecked(key)
	return n
}

func (s *Store) ZCardChecked(key string) (int, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if actual := s.keyTypeLockedRead(key); actual != TypeNone && actual != TypeZSet {
		return 0, ErrWrongType
	}

	zs, ok := s.zsets[key]
	if !ok {
		return 0, nil
	}

	return zs.Card(), nil
}

// ZScore returns the score of a member in the sorted set at key.
// Returns 0, false if the key or member does not exist.
func (s *Store) ZScore(key string, member string) (float64, bool) {
	score, ok, _ := s.ZScoreChecked(key, member)
	return score, ok
}

func (s *Store) ZScoreChecked(key string, member string) (float64, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if actual := s.keyTypeLockedRead(key); actual != TypeNone && actual != TypeZSet {
		return 0, false, ErrWrongType
	}

	zs, ok := s.zsets[key]
	if !ok {
		return 0, false, nil
	}

	score, ok := zs.Score(member)
	return score, ok, nil
}

// ZRem removes a member from the sorted set at key.
// Returns the number of members removed (1 or 0).
func (s *Store) ZRem(key string, member string) int {
	n, _ := s.ZRemChecked(key, member)
	return n
}

func (s *Store) ZRemChecked(key string, member string) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.ensureTypeLocked(key, TypeZSet); err != nil {
		return 0, err
	}

	zs, ok := s.zsets[key]
	if !ok {
		return 0, nil
	}

	removed := zs.Remove(member)

	// Redis automatically deletes the key if the sorted set becomes empty
	if zs.Card() == 0 {
		delete(s.zsets, key)
	}

	return removed, nil
}

// Incr increments the integer value stored at key by 1.
// If the key does not exist or has expired, it is created with value 1.
// If the key exists, its value must be a base-10 integer string.
// Returns the new value after incrementing.
func (s *Store) Incr(key string) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if actual := s.keyTypeLocked(key); actual != TypeNone && actual != TypeString {
		return 0, ErrWrongType
	}

	e, ok := s.data[key]

	// Missing or expired key → Redis sets value to 1
	if !ok || (e.hasExpiry && time.Now().After(e.expiresAt)) {
		s.data[key] = entry{value: "1"}
		return 1, nil
	}

	newValue, newInt, err := transactions.IncrementByOne(e.value)
	if err != nil {
		return 0, err
	}

	e.value = newValue
	s.data[key] = e // update in place — preserves EX/PX expiry

	return newInt, nil
}
