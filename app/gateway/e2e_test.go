package gateway

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/jaipreethtiruvaipati/redis-clone/app/server"
)

func waitForAddr(t *testing.T, addr func() string, started <-chan error) string {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		value := addr()
		if _, port, err := net.SplitHostPort(value); err == nil && port != "0" {
			return value
		}
		select {
		case err := <-started:
			t.Skipf("network unavailable: %v", err)
		default:
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("listener did not become ready")
	return ""
}

func TestRealRedisToGatewayHTTP(t *testing.T) {
	redis := server.New("127.0.0.1:0")
	redisStarted := make(chan error, 1)
	go func() { redisStarted <- redis.Start() }()
	redisAddr := waitForAddr(t, redis.Addr, redisStarted)

	g := New(Config{APIAddr: "127.0.0.1:0", RedisAddr: redisAddr, RequestTimeout: 2 * time.Second})
	gatewayStarted := make(chan error, 1)
	go func() { gatewayStarted <- g.Start() }()
	apiAddr := waitForAddr(t, g.Addr, gatewayStarted)

	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = g.Shutdown(ctx)
		_ = redis.Shutdown(ctx)
		select {
		case <-gatewayStarted:
		default:
		}
		select {
		case <-redisStarted:
		default:
		}
	}()

	httpClient := &http.Client{Timeout: 3 * time.Second}
	call := func(command string, headers map[string]string) (int, string) {
		body := strings.NewReader(`{"command":` + strconvQuote(command) + `}`)
		req, err := http.NewRequest(http.MethodPost, "http://"+apiAddr+"/api/command", body)
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Content-Type", "application/json")
		for key, value := range headers {
			req.Header.Set(key, value)
		}
		response, err := httpClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()
		data, err := io.ReadAll(response.Body)
		if err != nil {
			t.Fatal(err)
		}
		return response.StatusCode, string(data)
	}

	commands := []string{
		"PING", "SET foo bar", "GET foo", "INCR counter",
		"RPUSH list a b", "LRANGE list 0 -1", "ZADD ranks 1 alice", "ZRANGE ranks 0 -1",
		"XADD events 1-0 field value", "XRANGE events - +",
	}
	for _, command := range commands {
		status, body := call(command, nil)
		if status != http.StatusOK || !strings.Contains(body, `"ok":true`) {
			t.Fatalf("%s => %d %s", command, status, body)
		}
	}
	status, body := call("GET list", nil)
	if status != http.StatusBadRequest || !strings.Contains(body, `"redis_error"`) {
		t.Fatalf("wrong type => %d %s", status, body)
	}
	session := map[string]string{"X-Redis-Session": "integration-session"}
	for _, command := range []string{"MULTI", "SET tx value", "INCR txcounter"} {
		status, body = call(command, session)
		if status != http.StatusOK || !strings.Contains(body, `"ok":true`) {
			t.Fatalf("%s => %d %s", command, status, body)
		}
	}
	status, body = call("EXEC", session)
	if status != http.StatusOK || !strings.Contains(body, `"array"`) || !strings.Contains(body, `"OK"`) {
		t.Fatalf("EXEC => %d %s", status, body)
	}
	status, body = call("AUTH default secret", nil)
	if status != http.StatusForbidden || !strings.Contains(body, `"authorization_error"`) {
		t.Fatalf("gateway AUTH exposure => %d %s", status, body)
	}

	var serverInfo map[string]interface{}
	status, body = func() (int, string) {
		response, err := httpClient.Get("http://" + apiAddr + "/api/server")
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()
		data, _ := io.ReadAll(response.Body)
		return response.StatusCode, string(data)
	}()
	if status != http.StatusOK || json.Unmarshal([]byte(body), &serverInfo) != nil || serverInfo["status"] != "ok" {
		t.Fatalf("server info => %d %s", status, body)
	}
}

func strconvQuote(value string) string {
	data, _ := json.Marshal(value)
	return string(data)
}
