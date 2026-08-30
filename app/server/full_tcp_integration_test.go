package server

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net"
	"strings"
	"testing"
	"time"
)

// startTCPIntegrationServer starts the real listener on a dynamically selected
// localhost port. Every command in this file crosses TCP and RESP; no Store
// method is called by the test body.
func startTCPIntegrationServer(t *testing.T) (*Server, string) {
	t.Helper()
	s := New("127.0.0.1:0")
	started := make(chan error, 1)
	go func() { started <- s.Start() }()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		addr := s.Addr()
		if host, port, err := net.SplitHostPort(addr); err == nil && host != "" && port != "0" {
			t.Cleanup(func() {
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				defer cancel()
				if err := s.Shutdown(ctx); err != nil {
					t.Errorf("shutdown integration server: %v", err)
				}
				select {
				case err := <-started:
					if err != nil {
						t.Errorf("server exit: %v", err)
					}
				case <-time.After(time.Second):
					t.Errorf("server did not exit after shutdown")
				}
			})
			return s, addr
		}
		select {
		case err := <-started:
			t.Skipf("TCP listener unavailable: %v", err)
		default:
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("server did not become ready")
	return nil, ""
}

func dialTCPIntegration(t *testing.T, addr string) (net.Conn, *bufio.Reader) {
	t.Helper()
	conn, err := net.DialTimeout("tcp", addr, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn, bufio.NewReader(conn)
}

func TestFullFeatureMatrixOverTCP(t *testing.T) {
	_, addr := startTCPIntegrationServer(t)
	conn, reader := dialTCPIntegration(t, addr)

	if got := sendAndRead(t, conn, reader, "PING"); got != "+PONG\r\n" {
		t.Fatalf("PING = %q", got)
	}
	if got := sendAndRead(t, conn, reader, "ECHO", "hello"); got != "$5\r\nhello\r\n" {
		t.Fatalf("ECHO = %q", got)
	}

	if got := sendAndRead(t, conn, reader, "SET", "session", "alice", "EX", "1"); got != "+OK\r\n" {
		t.Fatalf("SET EX = %q", got)
	}
	if got := sendAndRead(t, conn, reader, "GET", "session"); got != "$5\r\nalice\r\n" {
		t.Fatalf("GET before EX expiry = %q", got)
	}
	if got := sendAndRead(t, conn, reader, "SET", "short", "px", "PX", "30"); got != "+OK\r\n" {
		t.Fatalf("SET PX = %q", got)
	}
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if got := sendAndRead(t, conn, reader, "GET", "short"); got == "$-1\r\n" {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if got := sendAndRead(t, conn, reader, "GET", "short"); got != "$-1\r\n" {
		t.Fatalf("GET after PX expiry = %q", got)
	}
	time.Sleep(1100 * time.Millisecond)
	if got := sendAndRead(t, conn, reader, "GET", "session"); got != "$-1\r\n" {
		t.Fatalf("GET after EX expiry = %q", got)
	}
	if got := sendAndRead(t, conn, reader, "SET", "counter", "41"); got != "+OK\r\n" {
		t.Fatalf("SET counter = %q", got)
	}
	if got := sendAndRead(t, conn, reader, "INCR", "counter"); got != ":42\r\n" {
		t.Fatalf("INCR = %q", got)
	}
	if got := sendAndRead(t, conn, reader, "GET", "counter"); got != "$2\r\n42\r\n" {
		t.Fatalf("GET counter = %q", got)
	}
	if got := sendAndRead(t, conn, reader, "INCR", "missing-counter"); got != ":1\r\n" {
		t.Fatalf("INCR missing = %q", got)
	}
	if got := sendAndRead(t, conn, reader, "SET", "bad-counter", "not-an-int"); got != "+OK\r\n" {
		t.Fatalf("SET bad counter = %q", got)
	}
	if got := sendAndRead(t, conn, reader, "INCR", "bad-counter"); !strings.HasPrefix(got, "-ERR value is not an integer") {
		t.Fatalf("INCR invalid = %q", got)
	}

	if got := sendAndRead(t, conn, reader, "RPUSH", "jobs", "build", "test", "deploy"); got != ":3\r\n" {
		t.Fatalf("RPUSH = %q", got)
	}
	if got := sendAndRead(t, conn, reader, "LPUSH", "jobs", "lint", "check"); got != ":5\r\n" {
		t.Fatalf("LPUSH = %q", got)
	}
	if got := sendAndRead(t, conn, reader, "LRANGE", "jobs", "-3", "-1"); got != "*3\r\n$5\r\nbuild\r\n$4\r\ntest\r\n$6\r\ndeploy\r\n" {
		t.Fatalf("LRANGE negative = %q", got)
	}
	if got := sendAndRead(t, conn, reader, "LLEN", "jobs"); got != ":5\r\n" {
		t.Fatalf("LLEN = %q", got)
	}
	if got := sendAndRead(t, conn, reader, "LPOP", "jobs", "2"); got != "*2\r\n$5\r\ncheck\r\n$4\r\nlint\r\n" {
		t.Fatalf("LPOP count = %q", got)
	}
	if got := sendAndRead(t, conn, reader, "TYPE", "jobs"); got != "+list\r\n" {
		t.Fatalf("TYPE list = %q", got)
	}

	for _, pair := range [][2]string{{"100", "alice"}, {"200", "bob"}, {"150", "charlie"}} {
		if got := sendAndRead(t, conn, reader, "ZADD", "leaderboard", pair[0], pair[1]); got != ":1\r\n" {
			t.Fatalf("ZADD %s = %q", pair[1], got)
		}
	}
	if got := sendAndRead(t, conn, reader, "ZRANGE", "leaderboard", "0", "-1"); got != "*3\r\n$5\r\nalice\r\n$7\r\ncharlie\r\n$3\r\nbob\r\n" {
		t.Fatalf("ZRANGE = %q", got)
	}
	if got := sendAndRead(t, conn, reader, "ZRANK", "leaderboard", "charlie"); got != ":1\r\n" {
		t.Fatalf("ZRANK = %q", got)
	}
	if got := sendAndRead(t, conn, reader, "ZSCORE", "leaderboard", "bob"); got != "$3\r\n200\r\n" {
		t.Fatalf("ZSCORE = %q", got)
	}
	if got := sendAndRead(t, conn, reader, "ZCARD", "leaderboard"); got != ":3\r\n" {
		t.Fatalf("ZCARD = %q", got)
	}
	if got := sendAndRead(t, conn, reader, "ZREM", "leaderboard", "bob"); got != ":1\r\n" {
		t.Fatalf("ZREM = %q", got)
	}

	if got := sendAndRead(t, conn, reader, "XADD", "events", "1000-0", "user", "bob", "action", "logout"); got != "$6\r\n1000-0\r\n" {
		t.Fatalf("XADD explicit ID = %q", got)
	}
	if got := sendAndRead(t, conn, reader, "XADD", "events", "*", "user", "alice", "action", "login"); !strings.HasPrefix(got, "$") {
		t.Fatalf("XADD auto ID = %q", got)
	}
	if got := sendAndRead(t, conn, reader, "XRANGE", "events", "-", "+"); !strings.Contains(got, "1000-0") || !strings.Contains(got, "alice") {
		t.Fatalf("XRANGE = %q", got)
	}
	if got := sendAndRead(t, conn, reader, "XREAD", "STREAMS", "events", "0-0"); !strings.Contains(got, "1000-0") {
		t.Fatalf("XREAD = %q", got)
	}
	if got := sendAndRead(t, conn, reader, "TYPE", "events"); got != "+stream\r\n" {
		t.Fatalf("TYPE stream = %q", got)
	}

	// Wrong-type checks cross every supported data structure.
	for _, tc := range []struct {
		name string
		args []string
	}{
		{"string-as-list", []string{"RPUSH", "counter", "x"}},
		{"list-as-string", []string{"GET", "jobs"}},
		{"zset-as-string", []string{"GET", "leaderboard"}},
		{"stream-as-list", []string{"LRANGE", "events", "0", "-1"}},
	} {
		got := sendAndRead(t, conn, reader, tc.args...)
		if !strings.HasPrefix(got, "-ERR WRONGTYPE") {
			t.Errorf("%s = %q", tc.name, got)
		}
	}
	if got := sendAndRead(t, conn, reader, "TYPE", "missing-type"); got != "+none\r\n" {
		t.Fatalf("TYPE missing = %q", got)
	}

	if got := sendAndRead(t, conn, reader, "MULTI"); got != "+OK\r\n" {
		t.Fatalf("MULTI = %q", got)
	}
	if got := sendAndRead(t, conn, reader, "SET", "tx-counter", "10"); got != "+QUEUED\r\n" {
		t.Fatalf("transaction SET = %q", got)
	}
	if got := sendAndRead(t, conn, reader, "INCR", "tx-counter"); got != "+QUEUED\r\n" {
		t.Fatalf("transaction INCR = %q", got)
	}
	if got := sendAndRead(t, conn, reader, "GET", "tx-counter"); got != "+QUEUED\r\n" {
		t.Fatalf("transaction GET = %q", got)
	}
	if got := sendAndRead(t, conn, reader, "EXEC"); got != "*3\r\n+OK\r\n:11\r\n$2\r\n11\r\n" {
		t.Fatalf("EXEC = %q", got)
	}
	if got := sendAndRead(t, conn, reader, "EXEC"); !strings.HasPrefix(got, "-ERR EXEC without MULTI") {
		t.Fatalf("EXEC outside transaction = %q", got)
	}
	if got := sendAndRead(t, conn, reader, "MULTI"); got != "+OK\r\n" {
		t.Fatalf("MULTI for DISCARD = %q", got)
	}
	if got := sendAndRead(t, conn, reader, "SET", "discarded", "value"); got != "+QUEUED\r\n" {
		t.Fatalf("queued DISCARD value = %q", got)
	}
	if got := sendAndRead(t, conn, reader, "DISCARD"); got != "+OK\r\n" {
		t.Fatalf("DISCARD = %q", got)
	}
	if got := sendAndRead(t, conn, reader, "GET", "discarded"); got != "$-1\r\n" {
		t.Fatalf("discarded key = %q", got)
	}
}

func TestMalformedRESPFramesDoNotKillServer(t *testing.T) {
	_, addr := startTCPIntegrationServer(t)
	frames := []string{
		"*x\r\n",
		"*1\r\n$4\r\nPING\n",
		"*1\r\n$4\r\nPI",
		"*1\r\n$16777217\r\n",
	}
	for i, frame := range frames {
		conn, err := net.DialTimeout("tcp", addr, time.Second)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := conn.Write([]byte(frame)); err != nil {
			t.Fatalf("frame %d write: %v", i, err)
		}
		_ = conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
		_, readErr := bufio.NewReader(conn).ReadByte()
		if readErr == nil {
			t.Errorf("malformed frame %d unexpectedly received a reply", i)
		} else if !errors.Is(readErr, io.EOF) {
			var netErr net.Error
			if !errors.As(readErr, &netErr) {
				t.Errorf("malformed frame %d unexpected read error: %v", i, readErr)
			}
		}
		_ = conn.Close()
		valid, validReader := dialTCPIntegration(t, addr)
		if got := sendAndRead(t, valid, validReader, "PING"); got != "+PONG\r\n" {
			t.Fatalf("server after malformed frame %d = %q", i, got)
		}
	}
}

func TestTCPBlockingMultipleWaitersAndStreams(t *testing.T) {
	_, addr := startTCPIntegrationServer(t)
	watcher1, reader1 := dialTCPIntegration(t, addr)
	watcher2, reader2 := dialTCPIntegration(t, addr)
	if _, err := watcher1.Write([]byte(encodeCommand("BLPOP", "fifo", "1"))); err != nil {
		t.Fatal(err)
	}
	if _, err := watcher2.Write([]byte(encodeCommand("BLPOP", "fifo", "1"))); err != nil {
		t.Fatal(err)
	}
	time.Sleep(30 * time.Millisecond)
	producer, producerReader := dialTCPIntegration(t, addr)
	if got := sendAndRead(t, producer, producerReader, "RPUSH", "fifo", "one"); got != ":1\r\n" {
		t.Fatalf("first RPUSH FIFO = %q", got)
	}
	got1, err := readReply(reader1)
	if err != nil || !strings.Contains(got1, "one") {
		t.Fatalf("first waiter = %q, %v", got1, err)
	}
	if got := sendAndRead(t, producer, producerReader, "RPUSH", "fifo", "two"); got != ":1\r\n" {
		t.Fatalf("second RPUSH FIFO = %q", got)
	}
	got2, err := readReply(reader2)
	if err != nil || !strings.Contains(got2, "two") {
		t.Fatalf("second waiter = %q, %v", got2, err)
	}

	streamWatcher, streamReader := dialTCPIntegration(t, addr)
	if _, err := streamWatcher.Write([]byte(encodeCommand("XREAD", "BLOCK", "800", "STREAMS", "one", "two", "0-0", "0-0"))); err != nil {
		t.Fatal(err)
	}
	time.Sleep(30 * time.Millisecond)
	if got := sendAndRead(t, producer, producerReader, "XADD", "two", "*", "field", "value"); !strings.HasPrefix(got, "$") {
		t.Fatalf("XADD multi stream = %q", got)
	}
	streamReply, err := readReply(streamReader)
	if err != nil || !strings.Contains(streamReply, "two") || !strings.Contains(streamReply, "value") {
		t.Fatalf("XREAD multi stream = %q, %v", streamReply, err)
	}
}
