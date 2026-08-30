package resp

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/jaipreethtiruvaipati/redis-clone/app/stream"
)

// Command represents a parsed Redis command with its arguments.
type Command struct {
	Name string
	Args []string
}

// Limits controls the amount of input accepted by the RESP command parser.
// The defaults are intentionally conservative for this in-memory server.
type Limits struct {
	MaxArrayElements    int
	MaxBulkStringLength int
	MaxLineLength       int
}

var DefaultLimits = Limits{
	MaxArrayElements:    1024,
	MaxBulkStringLength: 16 * 1024 * 1024,
	MaxLineLength:       64 * 1024,
}

// Parse reads and parses a RESP-encoded command from the reader.
func Parse(r *bufio.Reader) (*Command, error) {
	return ParseWithLimits(r, DefaultLimits)
}

// ParseWithLimits reads one RESP2 array of bulk strings while enforcing limits.
// It rejects malformed CRLF delimiters, negative/impossibly large lengths and
// truncated frames before making any large allocation.
func ParseWithLimits(r *bufio.Reader, limits Limits) (*Command, error) {
	if limits.MaxArrayElements <= 0 || limits.MaxBulkStringLength < 0 || limits.MaxLineLength < 2 {
		return nil, fmt.Errorf("invalid RESP parser limits")
	}

	line, err := readLine(r, limits.MaxLineLength)
	if err != nil {
		return nil, err
	}

	if len(line) == 0 || line[0] != '*' {
		return nil, fmt.Errorf("expected array, got %q", string(line))
	}

	count64, err := strconv.ParseInt(string(line[1:]), 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid array count: %w", err)
	}
	if count64 < 0 || count64 > int64(limits.MaxArrayElements) {
		return nil, fmt.Errorf("array count %d is out of range", count64)
	}
	count := int(count64)

	args := make([]string, 0, count)
	for i := 0; i < count; i++ {
		// Read the bulk string header e.g. "$4"
		line, err = readLine(r, limits.MaxLineLength)
		if err != nil {
			return nil, err
		}

		if len(line) == 0 || line[0] != '$' {
			return nil, fmt.Errorf("expected bulk string, got %q", string(line))
		}

		length64, err := strconv.ParseInt(string(line[1:]), 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid bulk string length: %w", err)
		}
		maxInt := int64(^uint(0) >> 1)
		if length64 < 0 || length64 > int64(limits.MaxBulkStringLength) || length64 > maxInt-2 {
			return nil, fmt.Errorf("bulk string length %d is out of range", length64)
		}
		length := int(length64)

		// Read exactly `length` bytes + trailing \r\n
		data := make([]byte, length+2)
		_, err = io.ReadFull(r, data)
		if err != nil {
			return nil, err
		}
		if data[length] != '\r' || data[length+1] != '\n' {
			return nil, fmt.Errorf("bulk string is missing CRLF terminator")
		}
		args = append(args, string(data[:length]))
	}

	if len(args) == 0 {
		return nil, fmt.Errorf("empty command")
	}

	return &Command{
		Name: strings.ToUpper(args[0]),
		Args: args[1:],
	}, nil
}

func readLine(r *bufio.Reader, max int) ([]byte, error) {
	line := make([]byte, 0, min(max, 128))
	for {
		part, err := r.ReadSlice('\n')
		line = append(line, part...)
		if len(line) > max {
			return nil, fmt.Errorf("RESP line exceeds maximum length of %d bytes", max)
		}
		if err == nil {
			break
		}
		if err != bufio.ErrBufferFull {
			return nil, err
		}
	}
	if len(line) < 2 || line[len(line)-2] != '\r' || line[len(line)-1] != '\n' {
		return nil, fmt.Errorf("malformed RESP line terminator")
	}
	return line[:len(line)-2], nil
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// SimpleString encodes s as a RESP simple string e.g. +PONG\r\n
func SimpleString(s string) string {
	return fmt.Sprintf("+%s\r\n", s)
}

// BulkString encodes s as a RESP bulk string e.g. $3\r\nhey\r\n
func BulkString(s string) string {
	return fmt.Sprintf("$%d\r\n%s\r\n", len(s), s)
}

// Error encodes s as a RESP error.
func Error(s string) string {
	if strings.HasPrefix(s, "ERR ") {
		return fmt.Sprintf("-%s\r\n", s)
	}
	return fmt.Sprintf("-ERR %s\r\n", s)
}

// NullBulkString returns a RESP null bulk string, used when a key doesn't exist.
func NullBulkString() string {
	return "$-1\r\n"
}

// Integer encodes n as a RESP integer e.g. :1\r\n
func Integer(n int) string {
	return fmt.Sprintf(":%d\r\n", n)
}

// Array encodes a slice of strings as a RESP array of bulk strings.
// Returns *0\r\n for an empty slice.
func Array(items []string) string {
	result := fmt.Sprintf("*%d\r\n", len(items))
	for _, item := range items {
		result += BulkString(item)
	}
	return result
}

// NullArray returns a RESP null array, used when BLPOP times out.
func NullArray() string {
	return "*-1\r\n"
}

// StreamEntries encodes a slice of stream entries as a RESP array of arrays.
// Each entry is encoded as: [id_bulk_string, [field1, value1, field2, value2, ...]]
func StreamEntries(entries []stream.Entry) string {
	result := fmt.Sprintf("*%d\r\n", len(entries))
	for _, e := range entries {
		result += "*2\r\n"
		result += BulkString(e.ID.String())
		result += fmt.Sprintf("*%d\r\n", len(e.Fields))
		for _, f := range e.Fields {
			result += BulkString(f)
		}
	}
	return result
}

// StreamReadResults encodes XREAD results as a RESP array.
// Format: [[key, [entries...]], [key, [entries...]], ...]
func StreamReadResults(results []stream.ReadResult) string {
	result := fmt.Sprintf("*%d\r\n", len(results))
	for _, r := range results {
		result += "*2\r\n"
		result += BulkString(r.Key)
		result += StreamEntries(r.Entries) // reuse existing encoder
	}
	return result
}

// ArrayOfReplies encodes a RESP array whose elements are already-encoded RESP values.
// Example: ArrayOfReplies([]string{"+OK\r\n", ":7\r\n"}) → *2\r\n+OK\r\n:7\r\n
func ArrayOfReplies(replies []string) string {
	result := fmt.Sprintf("*%d\r\n", len(replies))
	for _, r := range replies {
		result += r
	}
	return result
}
