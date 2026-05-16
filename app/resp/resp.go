package resp

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
	"strings"
)

// Command represents a parsed Redis command with its arguments.
type Command struct {
	Name string
	Args []string
}

// Parse reads and parses a RESP-encoded command from the reader.
func Parse(r *bufio.Reader) (*Command, error) {
	line, err := r.ReadString('\n')
	if err != nil {
		return nil, err
	}
	line = strings.TrimRight(line, "\r\n")

	if len(line) == 0 || line[0] != '*' {
		return nil, fmt.Errorf("expected array, got %q", line)
	}

	count, err := strconv.Atoi(line[1:])
	if err != nil {
		return nil, fmt.Errorf("invalid array count: %w", err)
	}

	args := make([]string, 0, count)
	for i := 0; i < count; i++ {
		// Read the bulk string header e.g. "$4"
		line, err = r.ReadString('\n')
		if err != nil {
			return nil, err
		}
		line = strings.TrimRight(line, "\r\n")

		if len(line) == 0 || line[0] != '$' {
			return nil, fmt.Errorf("expected bulk string, got %q", line)
		}

		length, err := strconv.Atoi(line[1:])
		if err != nil {
			return nil, fmt.Errorf("invalid bulk string length: %w", err)
		}

		// Read exactly `length` bytes + trailing \r\n
		data := make([]byte, length+2)
		_, err = io.ReadFull(r, data)
		if err != nil {
			return nil, err
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
