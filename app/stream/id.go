package stream

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// Parse parses an explicit entry ID string like "1526919030474-0" into an EntryID.
func Parse(idStr string) (EntryID, error) {
	parts := strings.SplitN(idStr, "-", 2)
	if len(parts) != 2 {
		return EntryID{}, fmt.Errorf("invalid entry ID format: %s", idStr)
	}

	ms, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return EntryID{}, fmt.Errorf("invalid milliseconds in ID: %s", parts[0])
	}

	seq, err := strconv.ParseUint(parts[1], 10, 64)
	if err != nil {
		return EntryID{}, fmt.Errorf("invalid sequence in ID: %s", parts[1])
	}

	return EntryID{Milliseconds: ms, Seq: seq}, nil
}

// IsAutoSeq returns true if the ID uses auto-sequence format "ms-*".
func IsAutoSeq(idStr string) bool {
	return len(idStr) > 2 && strings.HasSuffix(idStr, "-*")
}

// ParseAutoSeqMs parses the milliseconds part from an "ms-*" format ID.
func ParseAutoSeqMs(idStr string) (int64, error) {
	msStr := strings.TrimSuffix(idStr, "-*")
	ms, err := strconv.ParseInt(msStr, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid milliseconds in ID: %s", msStr)
	}
	return ms, nil
}

// GenerateSeq creates an EntryID with an auto-generated sequence number
// based on the last entry's ID in the stream.
// Rules:
//   - Same ms as lastID → seq = lastID.Seq + 1
//   - ms = 0, new ms → seq = 1 (special case)
//   - Any other new ms → seq = 0
func GenerateSeq(ms int64, lastID EntryID) EntryID {
	var seq uint64
	if lastID.Milliseconds == ms {
		seq = lastID.Seq + 1
	} else if ms == 0 {
		seq = 1 // ms=0 default starts at 1, not 0
	} else {
		seq = 0
	}
	return EntryID{Milliseconds: ms, Seq: seq}
}

// IsAutoFull returns true if the ID is "*" — fully auto-generated.
func IsAutoFull(idStr string) bool {
	return idStr == "*"
}

// GenerateFull generates a fully auto-generated EntryID.
// Uses the current Unix time in milliseconds as the time part.
// Sequence is 0 unless the current ms matches the last entry's ms.
func GenerateFull(lastID EntryID) EntryID {
	ms := time.Now().UnixMilli()
	return GenerateSeq(ms, lastID) // reuse existing seq logic
}
