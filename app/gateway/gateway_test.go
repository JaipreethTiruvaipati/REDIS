package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/jaipreethtiruvaipati/redis-clone/app/redisclient"
)

type fakeClient struct {
	responses map[string]*redisclient.Response
	errors    map[string]error
	commands  [][]string
}

func (f *fakeClient) DoContext(_ context.Context, args ...string) (*redisclient.Response, error) {
	f.commands = append(f.commands, append([]string(nil), args...))
	key := strings.Join(args, " ")
	if err := f.errors[key]; err != nil {
		return nil, err
	}
	if response, ok := f.responses[key]; ok {
		return response, nil
	}
	return &redisclient.Response{Type: redisclient.SimpleStringType, Str: "OK"}, nil
}
func (f *fakeClient) Close() error { return nil }

func TestPolicyAndCommandParsing(t *testing.T) {
	if err := CheckPolicy([]string{"GET", "foo"}, false); err != nil {
		t.Fatal(err)
	}
	if err := CheckPolicy([]string{"MULTI"}, false); err == nil {
		t.Fatal("MULTI without session accepted")
	}
	if err := CheckPolicy([]string{"AUTH", "default", "secret"}, true); err == nil {
		t.Fatal("AUTH exposed")
	}
	if category, ok := Categorize("xread"); !ok || category != Blocking {
		t.Fatalf("category=%q ok=%v", category, ok)
	}
	if err := CheckPolicy([]string{"UNKNOWN"}, false); err == nil {
		t.Fatal("unknown command accepted")
	}
}

func TestGatewayValidationAndJSONResponses(t *testing.T) {
	fake := &fakeClient{responses: map[string]*redisclient.Response{
		"PING":        {Type: redisclient.SimpleStringType, Str: "PONG"},
		"GET foo":     {Type: redisclient.BulkStringType, Str: "bar"},
		"GET missing": {Type: redisclient.NullType},
		"EXEC":        {Type: redisclient.ArrayType, Array: []*redisclient.Response{{Type: redisclient.SimpleStringType, Str: "OK"}, {Type: redisclient.IntegerType, Int: 1}}},
		"GET list":    {Type: redisclient.ErrorType, Str: "WRONGTYPE wrong kind"},
	}}
	g := New(Config{APIToken: "api-secret", RequestTimeout: time.Second})
	g.newClient = func() client { return fake }
	h := g.NewHTTPHandler()

	request := func(method, path, body, token string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(method, path, strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		if token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, req)
		return rr
	}
	if rr := request(http.MethodPost, "/api/command", `{"command":"PING"}`, ""); rr.Code != http.StatusUnauthorized {
		t.Fatalf("missing API auth status=%d", rr.Code)
	}
	if rr := request(http.MethodPost, "/api/command", `{"command":"PING"}`, "api-secret"); rr.Code != http.StatusOK || !strings.Contains(rr.Body.String(), `"simple_string"`) {
		t.Fatalf("PING response=%d %s", rr.Code, rr.Body.String())
	}
	if rr := request(http.MethodPost, "/api/command", `{"command":"GET foo"}`, "api-secret"); rr.Code != http.StatusOK || !strings.Contains(rr.Body.String(), `"bar"`) {
		t.Fatalf("GET response=%d %s", rr.Code, rr.Body.String())
	}
	if rr := request(http.MethodPost, "/api/command", `{"command":"GET missing"}`, "api-secret"); rr.Code != http.StatusOK || !strings.Contains(rr.Body.String(), `"type":"null"`) {
		t.Fatalf("null response=%d %s", rr.Code, rr.Body.String())
	}
	if rr := request(http.MethodPost, "/api/command", `{"command":"GET list"}`, "api-secret"); rr.Code != http.StatusBadRequest || !strings.Contains(rr.Body.String(), `"redis_error"`) {
		t.Fatalf("Redis error=%d %s", rr.Code, rr.Body.String())
	}
	if rr := request(http.MethodPost, "/api/command", `{"command":"NOPE"}`, "api-secret"); rr.Code != http.StatusBadRequest || !strings.Contains(rr.Body.String(), `"api_validation_error"`) {
		t.Fatalf("unsupported=%d %s", rr.Code, rr.Body.String())
	}
	if rr := request(http.MethodPost, "/api/command", `{`, "api-secret"); rr.Code != http.StatusBadRequest {
		t.Fatalf("malformed JSON=%d", rr.Code)
	}
	if rr := request(http.MethodPost, "/api/command", `{"command":""}`, "api-secret"); rr.Code != http.StatusBadRequest {
		t.Fatalf("empty command=%d", rr.Code)
	}
	if rr := request(http.MethodGet, "/api/keys", "", "api-secret"); rr.Code != http.StatusNotImplemented {
		t.Fatalf("keys=%d", rr.Code)
	}
	if rr := request(http.MethodGet, "/api/server", "", "api-secret"); rr.Code != http.StatusOK {
		t.Fatalf("server info=%d", rr.Code)
	}

	var decoded commandResponse
	if err := json.Unmarshal(request(http.MethodPost, "/api/command", `{"command":"PING"}`, "api-secret").Body.Bytes(), &decoded); err != nil || decoded.Response.Type != "simple_string" {
		t.Fatalf("decode response=%#v err=%v", decoded, err)
	}
}

func TestGatewayRedisConnectionAndTimeoutErrors(t *testing.T) {
	g := New(Config{RequestTimeout: 20 * time.Millisecond})
	g.newClient = func() client { return &fakeClient{errors: map[string]error{"PING": errors.New("dial timeout")}} }
	req := httptest.NewRequest(http.MethodPost, "/api/command", strings.NewReader(`{"command":"PING"}`))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	g.NewHTTPHandler().ServeHTTP(rr, req)
	if rr.Code != http.StatusGatewayTimeout || !strings.Contains(rr.Body.String(), `"redis_timeout"`) {
		t.Fatalf("timeout response=%d %s", rr.Code, rr.Body.String())
	}
}

func TestGatewayKeyDetailsAndNestedResponses(t *testing.T) {
	fake := &fakeClient{responses: map[string]*redisclient.Response{
		"TYPE key":        {Type: redisclient.SimpleStringType, Str: "list"},
		"LRANGE key 0 -1": {Type: redisclient.ArrayType, Array: []*redisclient.Response{{Type: redisclient.BulkStringType, Str: "a"}}},
		"TYPE missing":    {Type: redisclient.SimpleStringType, Str: "none"},
		"EXEC":            {Type: redisclient.ArrayType, Array: []*redisclient.Response{{Type: redisclient.SimpleStringType, Str: "OK"}, {Type: redisclient.ErrorType, Str: "ERR failed"}}},
	}}
	g := New(Config{})
	g.newClient = func() client { return fake }
	h := g.NewHTTPHandler()
	request := func(path string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodGet, path, nil)
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, req)
		return rr
	}
	if rr := request("/api/keys/key"); rr.Code != http.StatusOK || !strings.Contains(rr.Body.String(), `"list"`) || !strings.Contains(rr.Body.String(), `"a"`) {
		t.Fatalf("key detail = %d %s", rr.Code, rr.Body.String())
	}
	if rr := request("/api/keys/missing"); rr.Code != http.StatusNotFound || !strings.Contains(rr.Body.String(), `"key does not exist"`) {
		t.Fatalf("missing key detail = %d %s", rr.Code, rr.Body.String())
	}
	// Use a session to demonstrate that nested/mixed EXEC responses are retained.
	req := httptest.NewRequest(http.MethodPost, "/api/command", strings.NewReader(`{"command":"EXEC"}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Redis-Session", "nested")
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK || !strings.Contains(rr.Body.String(), `"array"`) || !strings.Contains(rr.Body.String(), `"error"`) {
		t.Fatalf("nested response = %d %s", rr.Code, rr.Body.String())
	}
}
