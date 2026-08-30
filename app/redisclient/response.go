package redisclient

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
)

// ResponseType identifies a RESP2 response kind.
type ResponseType string

const (
	SimpleStringType ResponseType = "simple_string"
	ErrorType        ResponseType = "error"
	IntegerType      ResponseType = "integer"
	BulkStringType   ResponseType = "bulk_string"
	NullType         ResponseType = "null"
	ArrayType        ResponseType = "array"
)

// Response preserves the structure and type of a Redis reply, including mixed
// and nested arrays returned by EXEC and stream commands.
type Response struct {
	Type  ResponseType
	Str   string
	Int   int64
	Array []*Response
}

func (r *Response) IsError() bool { return r != nil && r.Type == ErrorType }

// ResponseLimits bounds server replies before allocation.
type ResponseLimits struct {
	MaxBulkStringLength int
	MaxArrayElements    int
	MaxDepth            int
}

var DefaultResponseLimits = ResponseLimits{
	MaxBulkStringLength: 16 * 1024 * 1024,
	MaxArrayElements:    4096,
	MaxDepth:            64,
}

func decodeResponse(r *bufio.Reader, limits ResponseLimits) (*Response, error) {
	return decodeResponseDepth(r, limits, 0)
}

func decodeResponseDepth(r *bufio.Reader, limits ResponseLimits, depth int) (*Response, error) {
	if depth > limits.MaxDepth {
		return nil, fmt.Errorf("RESP response nesting exceeds limit")
	}
	line, err := readRESPLine(r, 64*1024)
	if err != nil {
		return nil, err
	}
	if len(line) == 0 {
		return nil, fmt.Errorf("empty RESP response")
	}
	switch line[0] {
	case '+':
		return &Response{Type: SimpleStringType, Str: string(line[1:])}, nil
	case '-':
		return &Response{Type: ErrorType, Str: string(line[1:])}, nil
	case ':':
		n, err := strconv.ParseInt(string(line[1:]), 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid RESP integer: %w", err)
		}
		return &Response{Type: IntegerType, Int: n}, nil
	case '$':
		n, err := strconv.ParseInt(string(line[1:]), 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid bulk string length: %w", err)
		}
		if n == -1 {
			return &Response{Type: NullType}, nil
		}
		if n < 0 || n > int64(limits.MaxBulkStringLength) || n > int64(^uint(0)>>1)-2 {
			return nil, fmt.Errorf("bulk string length %d is out of range", n)
		}
		data := make([]byte, int(n)+2)
		if _, err := io.ReadFull(r, data); err != nil {
			return nil, err
		}
		if data[n] != '\r' || data[n+1] != '\n' {
			return nil, fmt.Errorf("bulk string is missing CRLF terminator")
		}
		return &Response{Type: BulkStringType, Str: string(data[:n])}, nil
	case '*':
		n, err := strconv.ParseInt(string(line[1:]), 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid array length: %w", err)
		}
		if n == -1 {
			return &Response{Type: NullType}, nil
		}
		if n < 0 || n > int64(limits.MaxArrayElements) {
			return nil, fmt.Errorf("array length %d is out of range", n)
		}
		items := make([]*Response, 0, int(n))
		for i := int64(0); i < n; i++ {
			item, err := decodeResponseDepth(r, limits, depth+1)
			if err != nil {
				return nil, err
			}
			items = append(items, item)
		}
		return &Response{Type: ArrayType, Array: items}, nil
	default:
		return nil, fmt.Errorf("unknown RESP response type %q", line[0])
	}
}

func readRESPLine(r *bufio.Reader, max int) ([]byte, error) {
	line := make([]byte, 0, 128)
	for {
		part, err := r.ReadSlice('\n')
		line = append(line, part...)
		if len(line) > max {
			return nil, fmt.Errorf("RESP line exceeds maximum length")
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
