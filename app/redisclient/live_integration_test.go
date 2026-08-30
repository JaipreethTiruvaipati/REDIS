package redisclient

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/jaipreethtiruvaipati/redis-clone/app/auth"
	"github.com/jaipreethtiruvaipati/redis-clone/app/server"
)

func startRedisClientIntegrationServer(t *testing.T, password string) string {
	t.Helper()
	if password != "" {
		auth.DefaultUser().SetPassword(password)
		t.Cleanup(func() { auth.DefaultUser().SetNoPass() })
	}
	s := server.New("127.0.0.1:0")
	started := make(chan error, 1)
	go func() { started <- s.Start() }()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		addr := s.Addr()
		if _, port, err := net.SplitHostPort(addr); err == nil && port != "0" {
			t.Cleanup(func() {
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				defer cancel()
				if err := s.Shutdown(ctx); err != nil {
					t.Errorf("server shutdown: %v", err)
				}
				if err := <-started; err != nil {
					t.Errorf("server exit: %v", err)
				}
			})
			return addr
		}
		select {
		case err := <-started:
			t.Skipf("TCP listener unavailable: %v", err)
		default:
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("server did not become ready")
	return ""
}

func TestRedisClientUsesLiveMyRedisTCPAndRESP(t *testing.T) {
	addr := startRedisClientIntegrationServer(t, "client-live-secret")
	c := New(Config{Addr: addr, Username: "default", Password: "client-live-secret", DialTimeout: time.Second, IOTimeout: time.Second})
	defer c.Close()

	pong, err := c.DoContext(context.Background(), "PING")
	if err != nil || pong.Type != SimpleStringType || pong.Str != "PONG" {
		t.Fatalf("live PING = %#v, %v", pong, err)
	}
	if echoed, err := c.Do("ECHO", "hello"); err != nil || echoed.Type != BulkStringType || echoed.Str != "hello" {
		t.Fatalf("live ECHO = %#v, %v", echoed, err)
	}
	if _, err := c.Do("SET", "client-key", "value"); err != nil {
		t.Fatal(err)
	}
	if got, err := c.Do("GET", "client-key"); err != nil || got.Type != BulkStringType || got.Str != "value" {
		t.Fatalf("live GET = %#v, %v", got, err)
	}
	if _, err := c.Do("RPUSH", "client-list", "a", "b"); err != nil {
		t.Fatal(err)
	}
	if got, err := c.Do("LRANGE", "client-list", "0", "-1"); err != nil || got.Type != ArrayType || len(got.Array) != 2 {
		t.Fatalf("live LRANGE = %#v, %v", got, err)
	}
	if _, err := c.Do("MULTI"); err != nil {
		t.Fatal(err)
	}
	if queued, err := c.Do("SET", "client-tx", "1"); err != nil || queued.Str != "QUEUED" {
		t.Fatalf("live queued SET = %#v, %v", queued, err)
	}
	if result, err := c.Do("EXEC"); err != nil || result.Type != ArrayType || len(result.Array) != 1 || result.Array[0].Type != SimpleStringType {
		t.Fatalf("live EXEC = %#v, %v", result, err)
	}
}

func TestRedisClientLiveContextCancellationAndReconnect(t *testing.T) {
	addr := startRedisClientIntegrationServer(t, "")
	c := New(Config{Addr: addr, DialTimeout: time.Second, IOTimeout: 2 * time.Second})
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 35*time.Millisecond)
	defer cancel()
	if _, err := c.DoContext(ctx, "BLPOP", "client-never", "0"); err == nil {
		t.Fatal("blocking command unexpectedly succeeded")
	}
	if _, err := c.Do("PING"); err != nil {
		t.Fatalf("client did not reconnect after cancellation: %v", err)
	}
}
