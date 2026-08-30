package handler

import (
	"bytes"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/jaipreethtiruvaipati/redis-clone/app/auth"
	"github.com/jaipreethtiruvaipati/redis-clone/app/resp"
	"github.com/jaipreethtiruvaipati/redis-clone/app/store"
	"github.com/jaipreethtiruvaipati/redis-clone/app/transactions"
)

func run(t *testing.T, s *store.Store, user **auth.User, tx *transactions.State, name string, args ...string) string {
	t.Helper()
	var out bytes.Buffer
	dispatch(&resp.Command{Name: name, Args: args}, &out, s, user, tx)
	return out.String()
}

func runHandle(t *testing.T, s *store.Store, user **auth.User, tx *transactions.State, name string, args ...string) string {
	t.Helper()
	serverConn, clientConn := net.Pipe()
	done := make(chan struct{})
	go func() {
		Handle(&resp.Command{Name: name, Args: args}, serverConn, s, user, tx)
		close(done)
	}()
	buf := make([]byte, 4096)
	n, err := clientConn.Read(buf)
	_ = clientConn.Close()
	<-done
	if err != nil {
		t.Fatal(err)
	}
	return string(buf[:n])
}

func TestCommandReplies(t *testing.T) {
	s := store.New()
	user := auth.DefaultUser()
	var tx transactions.State
	tests := []struct {
		name string
		args []string
		want string
	}{
		{"PING", nil, "+PONG\r\n"},
		{"SET", []string{"key", "value"}, "+OK\r\n"},
		{"GET", []string{"key"}, "$5\r\nvalue\r\n"},
		{"LPUSH", []string{"list", "b", "a"}, ":2\r\n"},
		{"RPUSH", []string{"list", "c"}, ":3\r\n"},
		{"LRANGE", []string{"list", "0", "-1"}, "*3\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n"},
		{"LLEN", []string{"list"}, ":3\r\n"},
		{"LPOP", []string{"list"}, "$1\r\na\r\n"},
		{"ZADD", []string{"z", "2", "b"}, ":1\r\n"},
		{"ZADD", []string{"z", "1", "a"}, ":1\r\n"},
		{"ZRANK", []string{"z", "b"}, ":1\r\n"},
		{"ZRANGE", []string{"z", "0", "-1"}, "*2\r\n$1\r\na\r\n$1\r\nb\r\n"},
		{"ZCARD", []string{"z"}, ":2\r\n"},
		{"ZSCORE", []string{"z", "a"}, "$1\r\n1\r\n"},
		{"ZREM", []string{"z", "a"}, ":1\r\n"},
		{"INCR", []string{"counter"}, ":1\r\n"},
		{"INCR", []string{"counter"}, ":2\r\n"},
	}
	for _, tc := range tests {
		t.Run(tc.name+strings.Join(tc.args, "/"), func(t *testing.T) {
			if got := run(t, s, &user, &tx, tc.name, tc.args...); got != tc.want {
				t.Fatalf("%s reply = %q, want %q", tc.name, got, tc.want)
			}
		})
	}
	if got := run(t, s, &user, &tx, "PING", "hello"); got != "$5\r\nhello\r\n" {
		t.Fatalf("PING message = %q", got)
	}

	if got := run(t, s, &user, &tx, "SET", "key", "new", "PX", "1"); got != "+OK\r\n" {
		t.Fatal(got)
	}
	if got := run(t, s, &user, &tx, "SET", "exkey", "value", "EX", "1"); got != "+OK\r\n" {
		t.Fatal(got)
	}
	time.Sleep(5 * time.Millisecond)
	if got := run(t, s, &user, &tx, "GET", "key"); got != "$-1\r\n" {
		t.Fatalf("expired GET = %q", got)
	}
	if got := run(t, s, &user, &tx, "GET", "list"); !strings.HasPrefix(got, "-ERR WRONGTYPE") {
		t.Fatalf("wrong type GET = %q", got)
	}
}

func TestStreamsTransactionsAndAuth(t *testing.T) {
	s := store.New()
	user := auth.DefaultUser()
	var tx transactions.State
	if got := run(t, s, &user, &tx, "XADD", "events", "1-0", "kind", "start"); got != "$3\r\n1-0\r\n" {
		t.Fatal(got)
	}
	if got := run(t, s, &user, &tx, "XADD", "events", "2-0", "kind", "end"); got != "$3\r\n2-0\r\n" {
		t.Fatal(got)
	}
	if got := run(t, s, &user, &tx, "XRANGE", "events", "-", "+"); !strings.HasPrefix(got, "*2\r\n*2\r\n") {
		t.Fatalf("XRANGE = %q", got)
	}
	if got := run(t, s, &user, &tx, "XREAD", "STREAMS", "events", "1-0"); !strings.Contains(got, "2-0") {
		t.Fatalf("XREAD = %q", got)
	}
	if got := runHandle(t, s, &user, &tx, "MULTI"); got != "+OK\r\n" {
		t.Fatal(got)
	}
	if got := runHandle(t, s, &user, &tx, "MULTI"); !strings.HasPrefix(got, "-ERR MULTI calls can not be nested") {
		t.Fatalf("nested MULTI = %q", got)
	}
	if got := runHandle(t, s, &user, &tx, "SET", "txkey", "v"); got != "+QUEUED\r\n" {
		t.Fatal(got)
	}
	if got := runHandle(t, s, &user, &tx, "INCR", "txcounter"); got != "+QUEUED\r\n" {
		t.Fatal(got)
	}
	if got := runHandle(t, s, &user, &tx, "EXEC"); got != "*2\r\n+OK\r\n:1\r\n" {
		t.Fatalf("EXEC = %q", got)
	}
	if got := run(t, s, &user, &tx, "SET", "bad", "not-an-int"); got != "+OK\r\n" {
		t.Fatal(got)
	}
	if got := runHandle(t, s, &user, &tx, "MULTI"); got != "+OK\r\n" {
		t.Fatal(got)
	}
	if got := runHandle(t, s, &user, &tx, "INCR", "bad"); got != "+QUEUED\r\n" {
		t.Fatal(got)
	}
	if got := runHandle(t, s, &user, &tx, "SET", "after-error", "ok"); got != "+QUEUED\r\n" {
		t.Fatal(got)
	}
	if got := runHandle(t, s, &user, &tx, "EXEC"); !strings.HasPrefix(got, "*2\r\n-ERR value is not an integer or out of range\r\n+OK\r\n") {
		t.Fatalf("EXEC runtime error = %q", got)
	}
	if got := runHandle(t, s, &user, &tx, "DISCARD"); !strings.HasPrefix(got, "-ERR DISCARD without MULTI") {
		t.Fatalf("DISCARD outside transaction = %q", got)
	}
	if got := runHandle(t, s, &user, &tx, "MULTI"); got != "+OK\r\n" {
		t.Fatal(got)
	}
	if got := runHandle(t, s, &user, &tx, "SET", "discarded", "value"); got != "+QUEUED\r\n" {
		t.Fatal(got)
	}
	if got := runHandle(t, s, &user, &tx, "DISCARD"); got != "+OK\r\n" {
		t.Fatal(got)
	}
	if got := run(t, s, &user, &tx, "GET", "discarded"); got != "$-1\r\n" {
		t.Fatalf("discarded value = %q", got)
	}
	var unauth *auth.User
	if got := runHandle(t, s, &unauth, &tx, "AUTH", "default", "anything"); got != "+OK\r\n" || unauth == nil {
		t.Fatalf("AUTH = %q user=%v", got, unauth)
	}
	if got := runHandle(t, s, &unauth, &tx, "AUTH", "missing", "anything"); !strings.HasPrefix(got, "-WRONGPASS") {
		t.Fatalf("bad AUTH = %q", got)
	}
}
