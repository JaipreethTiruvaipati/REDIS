package redisclient

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"strings"
	"testing"
	"time"
)

func TestParseCommandLine(t *testing.T) {
	tests := []struct {
		input string
		want  []string
		err   bool
	}{
		{`SET foo "hello world"`, []string{"SET", "foo", "hello world"}, false},
		{`ECHO 'a b'`, []string{"ECHO", "a b"}, false},
		{`SET empty ""`, []string{"SET", "empty", ""}, false},
		{`SET foo hello\ world`, []string{"SET", "foo", "hello world"}, false},
		{"", nil, true},
		{`SET "unterminated`, nil, true},
	}
	for _, tc := range tests {
		got, err := ParseCommandLine(tc.input, 1024)
		if (err != nil) != tc.err {
			t.Fatalf("ParseCommandLine(%q) error=%v want=%v", tc.input, err, tc.err)
		}
		if !tc.err && strings.Join(got, "|") != strings.Join(tc.want, "|") {
			t.Fatalf("ParseCommandLine(%q)=%#v want %#v", tc.input, got, tc.want)
		}
	}
}

func TestDecodeResponses(t *testing.T) {
	tests := []struct {
		wire     string
		typeWant ResponseType
		str      string
		int      int64
		arr      int
	}{
		{"+OK\r\n", SimpleStringType, "OK", 0, 0},
		{"-ERR wrong type\r\n", ErrorType, "ERR wrong type", 0, 0},
		{":42\r\n", IntegerType, "", 42, 0},
		{"$3\r\nfoo\r\n", BulkStringType, "foo", 0, 0},
		{"$-1\r\n", NullType, "", 0, 0},
		{"*3\r\n+OK\r\n:1\r\n$-1\r\n", ArrayType, "", 0, 3},
		{"*2\r\n*1\r\n:7\r\n-ERR no\r\n", ArrayType, "", 0, 2},
	}
	for _, tc := range tests {
		got, err := decodeResponse(bufio.NewReader(strings.NewReader(tc.wire)), DefaultResponseLimits)
		if err != nil {
			t.Fatalf("decode %q: %v", tc.wire, err)
		}
		if got.Type != tc.typeWant || got.Str != tc.str || got.Int != tc.int || len(got.Array) != tc.arr {
			t.Fatalf("decode %q = %#v", tc.wire, got)
		}
	}
}

func TestDecodeRejectsMalformedResponses(t *testing.T) {
	for _, wire := range []string{"$4\r\nfoo\r\n", "$-2\r\n", "*1\r\n$4\r\nfoo\n", "*999999\r\n"} {
		if _, err := decodeResponse(bufio.NewReader(strings.NewReader(wire)), ResponseLimits{MaxBulkStringLength: 32, MaxArrayElements: 4, MaxDepth: 4}); err == nil {
			t.Errorf("accepted malformed response %q", wire)
		}
	}
}

func TestClientRoundTripAndAuth(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Skipf("network unavailable: %v", err)
	}
	defer listener.Close()
	serverDone := make(chan struct{})
	go func() {
		defer close(serverDone)
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		r := bufio.NewReader(conn)
		// AUTH, then PING; verify the client reuses one connection.
		for i, reply := range []string{"+OK\r\n", "+PONG\r\n", "+PONG\r\n"} {
			if i == 0 {
				if _, err := readTestCommand(r); err != nil {
					return
				}
			} else {
				if _, err := readTestCommand(r); err != nil {
					return
				}
			}
			_, _ = conn.Write([]byte(reply))
		}
	}()
	c := New(Config{Addr: listener.Addr().String(), Username: "default", Password: "secret", IOTimeout: time.Second})
	defer c.Close()
	response, err := c.DoContext(context.Background(), "PING")
	if err != nil || response.Type != SimpleStringType || response.Str != "PONG" {
		t.Fatalf("client PING = %#v, %v", response, err)
	}
	response, err = c.Do("PING")
	if err != nil || response.Str != "PONG" {
		t.Fatalf("client reused PING = %#v, %v", response, err)
	}
	_ = listener.Close()
	<-serverDone
}

func readTestCommand(r *bufio.Reader) ([]string, error) {
	line, err := r.ReadString('\n')
	if err != nil {
		return nil, err
	}
	count := 0
	if _, err := fmt.Sscanf(line, "*%d\r\n", &count); err != nil {
		return nil, err
	}
	args := make([]string, 0, count)
	for i := 0; i < count; i++ {
		line, err = r.ReadString('\n')
		if err != nil {
			return nil, err
		}
		var n int
		if _, err := fmt.Sscanf(line, "$%d\r\n", &n); err != nil {
			return nil, err
		}
		body := make([]byte, n+2)
		if _, err := io.ReadFull(r, body); err != nil {
			return nil, err
		}
		args = append(args, string(body[:n]))
	}
	return args, nil
}
