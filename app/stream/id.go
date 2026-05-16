package stream

import (
	"fmt"
	"strconv"
	"strings"
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
