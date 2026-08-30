package server

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jaipreethtiruvaipati/redis-clone/app/auth"
)

func encodeCommand(args ...string) string {
	var b strings.Builder
	b.WriteString("*" + strconv.Itoa(len(args)) + "\r\n")
	for _, arg := range args {
		b.WriteString("$" + strconv.Itoa(len(arg)) + "\r\n" + arg + "\r\n")
	}
	return b.String()
}

func readReply(r *bufio.Reader) (string, error) {
	line, err := r.ReadString('\n')
	if err != nil {
		return "", err
	}
	if len(line) < 1 {
		return "", errors.New("empty RESP reply")
	}
	switch line[0] {
	case '+', '-', ':':
		return line, nil
	case '$':
		n, err := strconv.Atoi(strings.TrimSpace(line[1:]))
		if err != nil {
			return "", err
		}
		if n < 0 {
			return line, nil
		}
		body := make([]byte, n+2)
		if _, err := io.ReadFull(r, body); err != nil {
			return "", err
		}
		return line + string(body), nil
	case '*':
		n, err := strconv.Atoi(strings.TrimSpace(line[1:]))
		if err != nil {
			return "", err
		}
		if n < 0 {
			return line, nil
		}
		var b strings.Builder
		b.WriteString(line)
		for i := 0; i < n; i++ {
			part, err := readReply(r)
			if err != nil {
				return "", err
			}
			b.WriteString(part)
		}
		return b.String(), nil
	default:
		return "", errors.New("unknown RESP reply type")
	}
}

func sendAndRead(t *testing.T, conn net.Conn, r *bufio.Reader, args ...string) string {
	t.Helper()
	reply, err := sendAndReadErr(conn, r, args...)
	if err != nil {
		t.Fatal(err)
	}
	return reply
}

func sendAndReadErr(conn net.Conn, r *bufio.Reader, args ...string) (string, error) {
	if _, err := conn.Write([]byte(encodeCommand(args...))); err != nil {
		return "", err
	}
	reply, err := readReply(r)
	if err != nil {
		return "", err
	}
	return reply, nil
}

func TestTCPIntegrationAndShutdown(t *testing.T) {
	s := New("127.0.0.1:0")
	startErr := make(chan error, 1)
	go func() { startErr <- s.Start() }()

	var addr string
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		addr = s.Addr()
		if _, port, err := net.SplitHostPort(addr); err == nil && port != "0" {
			break
		}
		select {
		case err := <-startErr:
			t.Skipf("TCP listener unavailable in this environment: %v", err)
		default:
		}
		time.Sleep(time.Millisecond)
	}
	if _, port, _ := net.SplitHostPort(addr); port == "0" {
		t.Fatal("server did not become ready")
	}

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	r := bufio.NewReader(conn)
	defer conn.Close()
	if got := sendAndRead(t, conn, r, "PING"); got != "+PONG\r\n" {
		t.Fatalf("PING = %q", got)
	}
	if got := sendAndRead(t, conn, r, "SET", "foo", "bar"); got != "+OK\r\n" {
		t.Fatalf("SET = %q", got)
	}
	if got := sendAndRead(t, conn, r, "GET", "foo"); got != "$3\r\nbar\r\n" {
		t.Fatalf("GET = %q", got)
	}
	if got := sendAndRead(t, conn, r, "RPUSH", "foo", "x"); !strings.HasPrefix(got, "-ERR WRONGTYPE") {
		t.Fatalf("wrong type = %q", got)
	}
	if got := sendAndRead(t, conn, r, "SET", "ttl", "v", "PX", "15"); got != "+OK\r\n" {
		t.Fatalf("SET PX = %q", got)
	}
	time.Sleep(25 * time.Millisecond)
	if got := sendAndRead(t, conn, r, "GET", "ttl"); got != "$-1\r\n" {
		t.Fatalf("expired GET = %q", got)
	}
	if got := sendAndRead(t, conn, r, "MULTI"); got != "+OK\r\n" {
		t.Fatalf("MULTI = %q", got)
	}
	if got := sendAndRead(t, conn, r, "SET", "tx", "v"); got != "+QUEUED\r\n" {
		t.Fatalf("queued SET = %q", got)
	}
	if got := sendAndRead(t, conn, r, "INCR", "count"); got != "+QUEUED\r\n" {
		t.Fatalf("queued INCR = %q", got)
	}
	if got := sendAndRead(t, conn, r, "EXEC"); got != "*2\r\n+OK\r\n:1\r\n" {
		t.Fatalf("EXEC = %q", got)
	}

	blocked, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := blocked.Write([]byte(encodeCommand("BLPOP", "jobs", "0"))); err != nil {
		t.Fatal(err)
	}
	time.Sleep(10 * time.Millisecond)
	shutdownCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := s.Shutdown(shutdownCtx); err != nil {
		t.Fatal(err)
	}
	_ = blocked.SetReadDeadline(time.Now().Add(time.Second))
	buf := make([]byte, 1)
	for {
		_, err := blocked.Read(buf)
		if err != nil {
			break
		}
	}
	_ = blocked.Close()
	select {
	case err := <-startErr:
		if err != nil {
			t.Fatalf("Start after shutdown = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Start did not exit after shutdown")
	}
}

func TestConcurrentTCPTransactions(t *testing.T) {
	s := New("127.0.0.1:0")
	startErr := make(chan error, 1)
	go func() { startErr <- s.Start() }()
	var addr string
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		addr = s.Addr()
		if _, port, err := net.SplitHostPort(addr); err == nil && port != "0" {
			break
		}
		select {
		case err := <-startErr:
			t.Skipf("TCP listener unavailable in this environment: %v", err)
		default:
		}
		time.Sleep(time.Millisecond)
	}
	if _, port, _ := net.SplitHostPort(addr); port == "0" {
		t.Fatal("server did not become ready")
	}

	const clients = 8
	var wg sync.WaitGroup
	errs := make(chan error, clients)
	for i := 0; i < clients; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			conn, err := net.Dial("tcp", addr)
			if err != nil {
				errs <- err
				return
			}
			defer conn.Close()
			r := bufio.NewReader(conn)
			for _, expected := range []struct {
				args []string
				resp string
			}{
				{[]string{"MULTI"}, "+OK\r\n"},
				{[]string{"SET", "tx:" + strconv.Itoa(i), "0"}, "+QUEUED\r\n"},
				{[]string{"INCR", "tx:" + strconv.Itoa(i)}, "+QUEUED\r\n"},
			} {
				got, err := sendAndReadErr(conn, r, expected.args...)
				if err != nil {
					errs <- err
					return
				}
				if got != expected.resp {
					errs <- errors.New("unexpected transaction queue reply: " + got)
					return
				}
			}
			got, err := sendAndReadErr(conn, r, "EXEC")
			if err != nil {
				errs <- err
				return
			}
			if got != "*2\r\n+OK\r\n:1\r\n" {
				errs <- errors.New("unexpected EXEC reply: " + got)
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := s.Shutdown(shutdownCtx); err != nil {
		t.Fatal(err)
	}
	if err := <-startErr; err != nil {
		t.Fatal(err)
	}
}

func TestTCPAuthenticationIsConnectionLocal(t *testing.T) {
	defaultUser := auth.DefaultUser()
	defaultUser.SetPassword("phase1-secret")
	defer defaultUser.SetNoPass()

	s := New("127.0.0.1:0")
	startErr := make(chan error, 1)
	go func() { startErr <- s.Start() }()
	var addr string
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		addr = s.Addr()
		if _, port, err := net.SplitHostPort(addr); err == nil && port != "0" {
			break
		}
		select {
		case err := <-startErr:
			t.Skipf("TCP listener unavailable in this environment: %v", err)
		default:
		}
		time.Sleep(time.Millisecond)
	}
	if _, port, _ := net.SplitHostPort(addr); port == "0" {
		t.Fatal("server did not become ready")
	}

	first, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	firstReader := bufio.NewReader(first)
	if got := sendAndRead(t, first, firstReader, "PING"); got != "-NOAUTH Authentication required.\r\n" {
		t.Fatalf("unauthenticated PING = %q", got)
	}
	if got := sendAndRead(t, first, firstReader, "AUTH", "default", "wrong"); !strings.HasPrefix(got, "-WRONGPASS") {
		t.Fatalf("wrong password = %q", got)
	}
	if got := sendAndRead(t, first, firstReader, "AUTH", "default", "phase1-secret"); got != "+OK\r\n" {
		t.Fatalf("AUTH = %q", got)
	}
	if got := sendAndRead(t, first, firstReader, "ACL", "WHOAMI"); got != "$7\r\ndefault\r\n" {
		t.Fatalf("ACL WHOAMI = %q", got)
	}
	if got := sendAndRead(t, first, firstReader, "ACL", "GETUSER", "default"); !strings.Contains(got, "flags") || !strings.Contains(got, "passwords") {
		t.Fatalf("ACL GETUSER = %q", got)
	}
	if got := sendAndRead(t, first, firstReader, "ACL", "SETUSER", "default", "on"); got != "+OK\r\n" {
		t.Fatalf("ACL SETUSER = %q", got)
	}

	second, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	secondReader := bufio.NewReader(second)
	if got := sendAndRead(t, second, secondReader, "PING"); got != "-NOAUTH Authentication required.\r\n" {
		t.Fatalf("second session PING = %q", got)
	}
	if got := sendAndRead(t, second, secondReader, "ACL", "WHOAMI"); got != "-NOAUTH Authentication required.\r\n" {
		t.Fatalf("unauthorized ACL = %q", got)
	}
	_ = first.Close()
	_ = second.Close()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := s.Shutdown(shutdownCtx); err != nil {
		t.Fatal(err)
	}
	if err := <-startErr; err != nil {
		t.Fatal(err)
	}
}
