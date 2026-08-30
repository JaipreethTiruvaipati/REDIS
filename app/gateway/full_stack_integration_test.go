package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/jaipreethtiruvaipati/redis-clone/app/auth"
	"github.com/jaipreethtiruvaipati/redis-clone/app/server"
)

type liveGatewayStack struct {
	redis   *server.Server
	gateway *Gateway
	base    string
}

func startLiveGatewayStack(t *testing.T, token string, redisPassword ...string) *liveGatewayStack {
	t.Helper()
	password := ""
	if len(redisPassword) > 0 {
		password = redisPassword[0]
	}
	if password != "" {
		auth.DefaultUser().SetPassword(password)
		t.Cleanup(func() { auth.DefaultUser().SetNoPass() })
	}
	redis := server.New("127.0.0.1:0")
	redisStarted := make(chan error, 1)
	go func() { redisStarted <- redis.Start() }()
	redisAddr := waitForAddr(t, redis.Addr, redisStarted)

	g := New(Config{APIAddr: "127.0.0.1:0", RedisAddr: redisAddr, RedisPassword: password, APIToken: token, CommandMaxBytes: 512, RequestTimeout: 2 * time.Second})
	gatewayStarted := make(chan error, 1)
	go func() { gatewayStarted <- g.Start() }()
	apiAddr := waitForAddr(t, g.Addr, gatewayStarted)
	stack := &liveGatewayStack{redis: redis, gateway: g, base: "http://" + apiAddr}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if err := g.Shutdown(ctx); err != nil {
			t.Errorf("gateway shutdown: %v", err)
		}
		if err := redis.Shutdown(ctx); err != nil {
			t.Errorf("redis shutdown: %v", err)
		}
		select {
		case err := <-gatewayStarted:
			if err != nil {
				t.Errorf("gateway exit: %v", err)
			}
		case <-time.After(time.Second):
			t.Errorf("gateway did not exit")
		}
		select {
		case err := <-redisStarted:
			if err != nil {
				t.Errorf("redis exit: %v", err)
			}
		case <-time.After(time.Second):
			t.Errorf("redis did not exit")
		}
	})
	return stack
}

func (s *liveGatewayStack) request(t *testing.T, method, path, body string, headers map[string]string) (int, http.Header, []byte) {
	t.Helper()
	req, err := http.NewRequest(method, s.base+path, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	for key, value := range headers {
		req.Header.Set(key, value)
	}
	response, err := (&http.Client{Timeout: 4 * time.Second}).Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	data, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	return response.StatusCode, response.Header, data
}

func (s *liveGatewayStack) command(t *testing.T, command, session, token string) (int, map[string]interface{}) {
	t.Helper()
	headers := map[string]string{"Content-Type": "application/json"}
	if session != "" {
		headers["X-Redis-Session"] = session
	}
	if token != "" {
		headers["Authorization"] = "Bearer " + token
	}
	payload, _ := json.Marshal(map[string]string{"command": command})
	status, _, body := s.request(t, http.MethodPost, "/api/command", string(payload), headers)
	var decoded map[string]interface{}
	if err := json.Unmarshal(body, &decoded); err != nil {
		t.Fatalf("%s returned invalid JSON (%d): %s", command, status, body)
	}
	return status, decoded
}

func assertCommandOK(t *testing.T, stack *liveGatewayStack, command, session, token string) map[string]interface{} {
	t.Helper()
	status, body := stack.command(t, command, session, token)
	if status != http.StatusOK || body["ok"] != true {
		t.Fatalf("%s => %d %#v", command, status, body)
	}
	return body
}

func TestCompleteHTTPGatewayFeatureMatrixAgainstLiveRedis(t *testing.T) {
	stack := startLiveGatewayStack(t, "phase4-token")
	token := "phase4-token"
	baseHeaders := map[string]string{"Authorization": "Bearer " + token}

	status, headers, body := stack.request(t, http.MethodOptions, "/api/server", "", map[string]string{"Origin": "http://localhost:5173"})
	if status != http.StatusNoContent || headers.Get("Access-Control-Allow-Origin") != "http://localhost:5173" {
		t.Fatalf("CORS preflight => %d headers=%v body=%s", status, headers, body)
	}
	status, _, body = stack.request(t, http.MethodGet, "/api/server", "", baseHeaders)
	var serverInfo map[string]interface{}
	if status != http.StatusOK || json.Unmarshal(body, &serverInfo) != nil || serverInfo["status"] != "ok" {
		t.Fatalf("initial server info => %d %s", status, body)
	}
	if serverInfo["redis_addr"] == "" || serverInfo["api_addr"] == "" || serverInfo["supported_command_count"].(float64) < 20 {
		t.Fatalf("incomplete server info: %#v", serverInfo)
	}

	assertCommandOK(t, stack, "PING", "", token)
	echo := assertCommandOK(t, stack, "ECHO hello", "", token)
	if echo["response"].(map[string]interface{})["value"] != "hello" {
		t.Fatalf("ECHO body: %#v", echo)
	}
	assertCommandOK(t, stack, "SET string-key value", "", token)
	assertCommandOK(t, stack, "GET string-key", "", token)
	assertCommandOK(t, stack, "SET counter 41", "", token)
	incr := assertCommandOK(t, stack, "INCR counter", "", token)
	if incr["response"].(map[string]interface{})["value"].(float64) != 42 {
		t.Fatalf("INCR body: %#v", incr)
	}
	assertCommandOK(t, stack, "SET expiry-key value PX 30", "", token)
	time.Sleep(60 * time.Millisecond)
	assertCommandOK(t, stack, "GET expiry-key", "", token)

	assertCommandOK(t, stack, "RPUSH list-key a b c", "", token)
	assertCommandOK(t, stack, "LPUSH list-key first", "", token)
	assertCommandOK(t, stack, "LRANGE list-key 0 -1", "", token)
	assertCommandOK(t, stack, "LLEN list-key", "", token)
	assertCommandOK(t, stack, "LPOP list-key 2", "", token)
	assertCommandOK(t, stack, "ZADD zset-key 100 alice", "", token)
	assertCommandOK(t, stack, "ZADD zset-key 200 bob", "", token)
	assertCommandOK(t, stack, "ZRANGE zset-key 0 -1", "", token)
	assertCommandOK(t, stack, "ZRANK zset-key alice", "", token)
	assertCommandOK(t, stack, "ZSCORE zset-key bob", "", token)
	assertCommandOK(t, stack, "ZCARD zset-key", "", token)
	assertCommandOK(t, stack, "ZREM zset-key bob", "", token)
	assertCommandOK(t, stack, "XADD stream-key 1-0 field value", "", token)
	assertCommandOK(t, stack, "XADD stream-key 2-0 field next", "", token)
	assertCommandOK(t, stack, "XRANGE stream-key - +", "", token)
	assertCommandOK(t, stack, "XREAD STREAMS stream-key 0-0", "", token)
	// Blocking commands use real HTTP requests and separate producer requests.
	blpopResult := make(chan struct {
		status int
		body   map[string]interface{}
	}, 1)
	go func() {
		status, result := stack.command(t, "BLPOP http-blocking 1", "", token)
		blpopResult <- struct {
			status int
			body   map[string]interface{}
		}{status, result}
	}()
	time.Sleep(80 * time.Millisecond)
	assertCommandOK(t, stack, "RPUSH http-blocking value", "", token)
	select {
	case result := <-blpopResult:
		if result.status != http.StatusOK || !strings.Contains(string(mustJSON(t, result.body)), "value") {
			t.Fatalf("HTTP BLPOP wakeup => %d %#v", result.status, result.body)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("HTTP BLPOP did not wake")
	}
	xreadResult := make(chan struct {
		status int
		body   map[string]interface{}
	}, 1)
	go func() {
		status, result := stack.command(t, "XREAD BLOCK 1000 STREAMS http-events 0-0", "", token)
		xreadResult <- struct {
			status int
			body   map[string]interface{}
		}{status, result}
	}()
	time.Sleep(80 * time.Millisecond)
	assertCommandOK(t, stack, "XADD http-events 1-0 field value", "", token)
	select {
	case result := <-xreadResult:
		if result.status != http.StatusOK || !strings.Contains(string(mustJSON(t, result.body)), "http-events") {
			t.Fatalf("HTTP XREAD BLOCK wakeup => %d %#v", result.status, result.body)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("HTTP XREAD BLOCK did not wake")
	}

	for _, command := range []string{"GET list-key", "GET zset-key", "LRANGE stream-key 0 -1"} {
		status, result := stack.command(t, command, "", token)
		if status != http.StatusBadRequest || result["error"].(map[string]interface{})["type"] != "redis_error" {
			t.Fatalf("wrong type %s => %d %#v", command, status, result)
		}
	}

	const session1 = "gateway-session-1"
	assertCommandOK(t, stack, "MULTI", session1, token)
	assertCommandOK(t, stack, "SET transaction-key 10", session1, token)
	assertCommandOK(t, stack, "INCR transaction-key", session1, token)
	assertCommandOK(t, stack, "GET transaction-key", session1, token)
	exec := assertCommandOK(t, stack, "EXEC", session1, token)
	if exec["response"].(map[string]interface{})["type"] != "array" {
		t.Fatalf("EXEC response: %#v", exec)
	}
	assertCommandOK(t, stack, "MULTI", "gateway-session-2", token)
	assertCommandOK(t, stack, "DISCARD", "gateway-session-2", token)
	if status, result := stack.command(t, "MULTI", "", token); status != http.StatusForbidden || result["error"].(map[string]interface{})["type"] != "authorization_error" {
		t.Fatalf("MULTI without session => %d %#v", status, result)
	}

	for _, keyType := range []struct{ key, want string }{{"string-key", "string"}, {"list-key", "list"}, {"zset-key", "zset"}, {"stream-key", "stream"}} {
		status, _, body = stack.request(t, http.MethodGet, "/api/keys/"+keyType.key, "", baseHeaders)
		var detail map[string]interface{}
		if status != http.StatusOK || json.Unmarshal(body, &detail) != nil || detail["type"] != keyType.want {
			t.Fatalf("key detail %s => %d %s", keyType.key, status, body)
		}
	}
	status, _, body = stack.request(t, http.MethodGet, "/api/keys/missing-key", "", baseHeaders)
	if status != http.StatusNotFound || !bytes.Contains(body, []byte("key does not exist")) {
		t.Fatalf("missing key => %d %s", status, body)
	}
	status, _, body = stack.request(t, http.MethodGet, "/api/keys", "", baseHeaders)
	if status != http.StatusNotImplemented || !bytes.Contains(body, []byte("KEYS or SCAN")) {
		t.Fatalf("key enumeration => %d %s", status, body)
	}

	if status, result := stack.command(t, "AUTH default secret", "", token); status != http.StatusForbidden || result["error"].(map[string]interface{})["type"] != "authorization_error" {
		t.Fatalf("AUTH policy => %d %#v", status, result)
	}
	if status, _, _ := stack.request(t, http.MethodPost, "/api/command", `{"command":"PING"}`, map[string]string{"Content-Type": "application/json"}); status != http.StatusUnauthorized {
		t.Fatalf("missing API auth => %d", status)
	}
	if status, _, _ := stack.request(t, http.MethodPost, "/api/command", `{"command":"PING"}`, map[string]string{"Content-Type": "application/json", "Authorization": "Bearer wrong"}); status != http.StatusUnauthorized {
		t.Fatalf("wrong API auth => %d", status)
	}
	if status, _, _ := stack.request(t, http.MethodPost, "/api/command", `{"command":"PING"}`, map[string]string{"Content-Type": "text/plain", "Authorization": "Bearer " + token}); status != http.StatusUnsupportedMediaType {
		t.Fatalf("wrong content type => %d", status)
	}
	if status, _, _ := stack.request(t, http.MethodPost, "/api/command", `{"command":"PING"} {"command":"ECHO"}`, map[string]string{"Content-Type": "application/json", "Authorization": "Bearer " + token}); status != http.StatusBadRequest {
		t.Fatalf("multiple JSON values => %d", status)
	}
	if status, _, _ := stack.request(t, http.MethodPost, "/api/command", `{"command":"`+strings.Repeat("x", 700)+`"}`, map[string]string{"Content-Type": "application/json", "Authorization": "Bearer " + token}); status != http.StatusBadRequest {
		t.Fatalf("oversized command => %d", status)
	}

	status, _, body = stack.request(t, http.MethodGet, "/api/server", "", baseHeaders)
	if status != http.StatusOK {
		t.Fatalf("final server info => %d %s", status, body)
	}
	if err := json.Unmarshal(body, &serverInfo); err != nil {
		t.Fatal(err)
	}
	if serverInfo["status"] != "ok" || serverInfo["command_requests"].(float64) < 20 || serverInfo["requests"].(float64) < serverInfo["command_requests"].(float64) {
		t.Fatalf("counters/status do not reflect live traffic: %#v", serverInfo)
	}
}

func TestGatewayAuthenticatesToLiveRedisInternally(t *testing.T) {
	stack := startLiveGatewayStack(t, "", "gateway-internal-secret")
	status, body := stack.command(t, "PING", "", "")
	if status != http.StatusOK || body["ok"] != true {
		t.Fatalf("gateway internal auth => %d %#v", status, body)
	}
	if strings.Contains(string(mustJSON(t, body)), "gateway-internal-secret") {
		t.Fatal("Redis password leaked through gateway response")
	}
}

func mustJSON(t *testing.T, value interface{}) []byte {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func TestGatewayReportsRedisUnavailableSeparatelyFromGateway(t *testing.T) {
	g := New(Config{APIAddr: "127.0.0.1:0", RedisAddr: "127.0.0.1:1", RedisDialTimeout: 100 * time.Millisecond, RequestTimeout: 250 * time.Millisecond})
	started := make(chan error, 1)
	go func() { started <- g.Start() }()
	apiAddr := waitForAddr(t, g.Addr, started)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = g.Shutdown(ctx)
		<-started
	})
	response, err := (&http.Client{Timeout: time.Second}).Get("http://" + apiAddr + "/api/server")
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	var info map[string]interface{}
	if err := json.NewDecoder(response.Body).Decode(&info); err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusOK || info["status"] != "unavailable" || info["redis_addr"] != "127.0.0.1:1" {
		t.Fatalf("unavailable Redis report => %d %#v", response.StatusCode, info)
	}
	request, _ := http.NewRequest(http.MethodPost, "http://"+apiAddr+"/api/command", strings.NewReader(`{"command":"PING"}`))
	request.Header.Set("Content-Type", "application/json")
	commandResponse, err := (&http.Client{Timeout: time.Second}).Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer commandResponse.Body.Close()
	var commandBody map[string]interface{}
	if err := json.NewDecoder(commandResponse.Body).Decode(&commandBody); err != nil {
		t.Fatal(err)
	}
	if commandResponse.StatusCode != http.StatusBadGateway || commandBody["error"].(map[string]interface{})["type"] != "redis_connection_error" {
		t.Fatalf("command unavailable report => %d %#v", commandResponse.StatusCode, commandBody)
	}
}

func TestGatewayPropagatesLiveBlockingTimeout(t *testing.T) {
	stack := startLiveGatewayStack(t, "")
	g := New(Config{APIAddr: "127.0.0.1:0", RedisAddr: stack.redis.Addr(), RequestTimeout: 250 * time.Millisecond, RedisIOTimeout: 35 * time.Millisecond})
	started := make(chan error, 1)
	go func() { started <- g.Start() }()
	apiAddr := waitForAddr(t, g.Addr, started)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = g.Shutdown(ctx)
		<-started
	})
	request, err := http.NewRequest(http.MethodPost, "http://"+apiAddr+"/api/command", strings.NewReader(`{"command":"BLPOP timeout-key 0"}`))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := (&http.Client{Timeout: time.Second}).Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	var body map[string]interface{}
	if err := json.NewDecoder(response.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusGatewayTimeout || body["error"].(map[string]interface{})["type"] != "redis_timeout" {
		t.Fatalf("live timeout => %d %#v", response.StatusCode, body)
	}
}
