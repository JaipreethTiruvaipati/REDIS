package transactions

import "strconv"

// IncrementByOne parses a base-10 integer string, adds 1, and returns the new value.
// Used by INCR and similar commands that operate on string-encoded integers.
func IncrementByOne(value string) (newValue string, newInt int, err error) {
	n, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return "", 0, err
	}

	newVal := n + 1
	return strconv.FormatInt(newVal, 10), int(newVal), nil
}
