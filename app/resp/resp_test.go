package resp

import (
	"bufio"
	"strings"
	"testing"
)

func TestParseValidAndMalformedFrames(t *testing.T) {
	tests := []struct {
		name string
		wire string
		want *Command
		err  bool
	}{
		{"valid", "*3\r\n$4\r\nPING\r\n$3\r\nfoo\r\n$0\r\n\r\n", &Command{Name: "PING", Args: []string{"foo", ""}}, false},
		{"negative array", "*-1\r\n", nil, true},
		{"negative bulk", "*1\r\n$-1\r\n", nil, true},
		{"invalid number", "*x\r\n", nil, true},
		{"bad line ending", "*1\n$4\r\nPING\r\n", nil, true},
		{"bad bulk ending", "*1\r\n$4\r\nPINGxx", nil, true},
		{"truncated", "*1\r\n$4\r\nPI", nil, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := Parse(bufio.NewReader(strings.NewReader(tc.wire)))
			if (err != nil) != tc.err {
				t.Fatalf("Parse() error = %v, want error %v", err, tc.err)
			}
			if err == nil && (got.Name != tc.want.Name || len(got.Args) != len(tc.want.Args) || got.Args[0] != tc.want.Args[0]) {
				t.Fatalf("Parse() = %#v, want %#v", got, tc.want)
			}
		})
	}
}

func TestParseLimits(t *testing.T) {
	limits := Limits{MaxArrayElements: 2, MaxBulkStringLength: 4, MaxLineLength: 32}
	for _, wire := range []string{
		"*3\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n",
		"*1\r\n$5\r\nhello\r\n",
		"*1\r\n$1\r\na\n",
	} {
		if _, err := ParseWithLimits(bufio.NewReader(strings.NewReader(wire)), limits); err == nil {
			t.Errorf("ParseWithLimits accepted invalid/oversized frame %q", wire)
		}
	}
}

func TestEncoding(t *testing.T) {
	tests := map[string]string{
		"simple": SimpleString("PONG"),
		"bulk":   BulkString("hey"),
		"error":  Error("wrong type"),
		"null":   NullBulkString(),
		"int":    Integer(7),
		"array":  Array([]string{"a", "b"}),
	}
	want := map[string]string{"simple": "+PONG\r\n", "bulk": "$3\r\nhey\r\n", "error": "-ERR wrong type\r\n", "null": "$-1\r\n", "int": ":7\r\n", "array": "*2\r\n$1\r\na\r\n$1\r\nb\r\n"}
	for name, got := range tests {
		if got != want[name] {
			t.Errorf("%s = %q, want %q", name, got, want[name])
		}
	}
}
