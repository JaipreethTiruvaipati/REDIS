package stream

import "fmt"

// EntryID represents a Redis stream entry ID (milliseconds-sequence).
type EntryID struct {
	Milliseconds int64
	Seq          uint64
}

// ReadResult holds the XREAD result for a single stream.
type ReadResult struct {
	Key     string
	Entries []Entry
}

// String formats an EntryID as "ms-seq".
func (id EntryID) String() string {
	return fmt.Sprintf("%d-%d", id.Milliseconds, id.Seq)
}

// IsZero returns true if this is the zero value ID (0-0).
func (id EntryID) IsZero() bool {
	return id.Milliseconds == 0 && id.Seq == 0
}

// LessThan returns true if id < other.
func (id EntryID) LessThan(other EntryID) bool {
	if id.Milliseconds != other.Milliseconds {
		return id.Milliseconds < other.Milliseconds
	}
	return id.Seq < other.Seq
}

// Entry represents a single stream entry.
// Fields is a flat slice of alternating key-value pairs: [k1, v1, k2, v2, ...]
// Using a slice (not map) preserves insertion order.
type Entry struct {
	ID     EntryID
	Fields []string
}

// Stream holds an ordered sequence of entries.
type Stream struct {
	Entries []Entry
	LastID  EntryID
}

// New creates a new empty Stream.
func New() *Stream {
	return &Stream{
		Entries: []Entry{},
	}
}

// Add appends a new entry to the stream.
func (s *Stream) Add(id EntryID, fields []string) {
	s.Entries = append(s.Entries, Entry{ID: id, Fields: fields})
	s.LastID = id
}
