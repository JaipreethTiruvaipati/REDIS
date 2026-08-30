// Package gateway exposes a deliberately scoped HTTP access layer over MyRedis.
// It communicates exclusively through the redisclient TCP/RESP adapter.
package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jaipreethtiruvaipati/redis-clone/app/redisclient"
)

const (
	defaultAPIAddr   = "127.0.0.1:8080"
	defaultRedisAddr = "127.0.0.1:6379"
)

// Config controls gateway networking, authentication, and request limits.
type Config struct {
	APIAddr          string
	RedisAddr        string
	RedisUsername    string
	RedisPassword    string
	APIToken         string
	CommandMaxBytes  int
	RequestTimeout   time.Duration
	RedisDialTimeout time.Duration
	RedisIOTimeout   time.Duration
}

func (c Config) withDefaults() Config {
	if c.APIAddr == "" {
		c.APIAddr = defaultAPIAddr
	}
	if c.RedisAddr == "" {
		c.RedisAddr = defaultRedisAddr
	}
	if c.CommandMaxBytes <= 0 {
		c.CommandMaxBytes = 1024 * 1024
	}
	if c.RequestTimeout <= 0 {
		c.RequestTimeout = 30 * time.Second
	}
	if c.RedisDialTimeout <= 0 {
		c.RedisDialTimeout = 3 * time.Second
	}
	if c.RedisIOTimeout <= 0 {
		c.RedisIOTimeout = 10 * time.Second
	}
	return c
}

type client interface {
	DoContext(context.Context, ...string) (*redisclient.Response, error)
	Close() error
}

// Gateway is an HTTP server and connection/session manager for MyRedis.
type Gateway struct {
	cfg       Config
	newClient func() client

	mu       sync.Mutex
	sessions map[string]client
	http     *http.Server
	listener net.Listener

	requests        atomic.Uint64
	commandRequests atomic.Uint64
	commandErrors   atomic.Uint64
	redisErrors     atomic.Uint64
	activeRequests  atomic.Int64
	requestID       atomic.Uint64
	startedAt       time.Time
}

func New(cfg Config) *Gateway {
	cfg = cfg.withDefaults()
	g := &Gateway{cfg: cfg, sessions: make(map[string]client), startedAt: time.Now()}
	g.newClient = func() client {
		return redisclient.New(redisclient.Config{
			Addr: cfg.RedisAddr, Username: cfg.RedisUsername, Password: cfg.RedisPassword,
			DialTimeout: cfg.RedisDialTimeout, IOTimeout: cfg.RedisIOTimeout,
		})
	}
	return g
}

func (g *Gateway) Config() Config { return g.cfg }

// NewHTTPHandler returns the handler without starting a listener, useful for
// embedding and httptest-based gateway tests.
func (g *Gateway) NewHTTPHandler() http.Handler { return http.HandlerFunc(g.serveHTTP) }

// Start begins the HTTP listener and blocks until Shutdown or a server error.
func (g *Gateway) Start() error {
	g.http = &http.Server{
		Addr: g.cfg.APIAddr, Handler: g.NewHTTPHandler(),
		ReadHeaderTimeout: 5 * time.Second, ReadTimeout: g.cfg.RequestTimeout,
		WriteTimeout: g.cfg.RequestTimeout, IdleTimeout: 60 * time.Second,
	}
	listener, err := net.Listen("tcp", g.cfg.APIAddr)
	if err != nil {
		return fmt.Errorf("gateway listen failed: %w", err)
	}
	g.mu.Lock()
	g.listener = listener
	g.mu.Unlock()
	defer func() {
		g.mu.Lock()
		if g.listener == listener {
			g.listener = nil
		}
		g.mu.Unlock()
	}()
	log.Printf(`{"event":"gateway_listening","addr":%q,"redis_addr":%q}`, listener.Addr().String(), g.cfg.RedisAddr)
	err = g.http.Serve(listener)
	if errors.Is(err, http.ErrServerClosed) || errors.Is(err, net.ErrClosed) {
		return nil
	}
	return err
}

func (g *Gateway) Addr() string {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.listener != nil {
		return g.listener.Addr().String()
	}
	return g.cfg.APIAddr
}

func (g *Gateway) Shutdown(ctx context.Context) error {
	if g.http != nil {
		if err := g.http.Shutdown(ctx); err != nil {
			return err
		}
	}
	g.mu.Lock()
	for id, c := range g.sessions {
		_ = c.Close()
		delete(g.sessions, id)
	}
	g.mu.Unlock()
	return nil
}

type commandRequest struct {
	Command string `json:"command"`
}

type commandResponse struct {
	OK       bool          `json:"ok"`
	Command  string        `json:"command,omitempty"`
	Response *responseJSON `json:"response,omitempty"`
	Error    *errorJSON    `json:"error,omitempty"`
}

type responseJSON struct {
	Type  string      `json:"type"`
	Value interface{} `json:"value"`
}

type errorJSON struct {
	Type    string `json:"type"`
	Message string `json:"message"`
}

type apiError struct {
	Status  int
	Type    string
	Message string
}

func (e *apiError) Error() string { return e.Message }

func (g *Gateway) serveHTTP(w http.ResponseWriter, r *http.Request) {
	g.applyCORS(w, r)
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	start := time.Now()
	id := g.requestID.Add(1)
	g.requests.Add(1)
	g.activeRequests.Add(1)
	defer func() {
		g.activeRequests.Add(-1)
		log.Printf(`{"event":"http_request","request_id":%d,"method":%q,"path":%q,"latency_ms":%d}`, id, r.Method, r.URL.Path, time.Since(start).Milliseconds())
	}()

	if err := g.checkAPIAuth(r); err != nil {
		g.writeAPIError(w, "", err)
		return
	}

	switch {
	case r.Method == http.MethodPost && r.URL.Path == "/api/command":
		g.handleCommand(w, r)
	case r.Method == http.MethodGet && r.URL.Path == "/api/keys":
		g.handleKeys(w, r)
	case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/api/keys/"):
		g.handleKey(w, r, strings.TrimPrefix(r.URL.Path, "/api/keys/"))
	case r.Method == http.MethodGet && r.URL.Path == "/api/server":
		g.handleServerInfo(w, r)
	default:
		g.writeAPIError(w, "", &apiError{Status: http.StatusNotFound, Type: "api_error", Message: "endpoint not found"})
	}
}

func (g *Gateway) applyCORS(w http.ResponseWriter, r *http.Request) {
	origin := r.Header.Get("Origin")
	// The gateway uses explicit headers rather than cookies for API/session
	// state, so a development wildcard is safe and keeps Vite/other local
	// frontend ports usable without exposing Redis credentials.
	if origin == "" {
		origin = "*"
	}
	w.Header().Set("Access-Control-Allow-Origin", origin)
	w.Header().Set("Vary", "Origin")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization, X-API-Key, X-Redis-Session")
	w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
}

func (g *Gateway) checkAPIAuth(r *http.Request) error {
	if g.cfg.APIToken == "" {
		return nil
	}
	value := strings.TrimSpace(r.Header.Get("Authorization"))
	if strings.HasPrefix(strings.ToLower(value), "bearer ") {
		value = strings.TrimSpace(value[len("Bearer "):])
	}
	if value == "" {
		value = r.Header.Get("X-API-Key")
	}
	if value != g.cfg.APIToken {
		return &apiError{Status: http.StatusUnauthorized, Type: "authentication_error", Message: "API authentication required"}
	}
	return nil
}

func (g *Gateway) handleCommand(w http.ResponseWriter, r *http.Request) {
	if !strings.Contains(strings.ToLower(r.Header.Get("Content-Type")), "application/json") {
		g.writeAPIError(w, "", &apiError{Status: http.StatusUnsupportedMediaType, Type: "api_validation_error", Message: "Content-Type must be application/json"})
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, int64(g.cfg.CommandMaxBytes)+4096)
	var req commandRequest
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&req); err != nil {
		g.writeAPIError(w, "", &apiError{Status: http.StatusBadRequest, Type: "api_validation_error", Message: "malformed JSON request"})
		return
	}
	var extra interface{}
	if err := decoder.Decode(&extra); err != io.EOF {
		g.writeAPIError(w, "", &apiError{Status: http.StatusBadRequest, Type: "api_validation_error", Message: "request must contain one JSON object"})
		return
	}
	args, err := redisclient.ParseCommandLine(req.Command, g.cfg.CommandMaxBytes)
	if err != nil {
		g.writeAPIError(w, req.Command, &apiError{Status: http.StatusBadRequest, Type: "api_validation_error", Message: err.Error()})
		return
	}
	hasSession := strings.TrimSpace(r.Header.Get("X-Redis-Session")) != ""
	if err := CheckPolicy(args, hasSession); err != nil {
		status, typ := http.StatusForbidden, "authorization_error"
		if _, known := Categorize(args[0]); !known {
			status, typ = http.StatusBadRequest, "api_validation_error"
		}
		g.writeAPIError(w, req.Command, &apiError{Status: status, Type: typ, Message: err.Error()})
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), g.cfg.RequestTimeout)
	defer cancel()
	redisClient, ephemeral, err := g.clientFor(r.Header.Get("X-Redis-Session"))
	if err != nil {
		g.writeAPIError(w, req.Command, &apiError{Status: http.StatusBadGateway, Type: "redis_connection_error", Message: "unable to create Redis client"})
		return
	}
	if ephemeral {
		defer redisClient.Close()
	}
	g.commandRequests.Add(1)
	response, err := redisClient.DoContext(ctx, args...)
	if err != nil {
		g.redisErrors.Add(1)
		status, typ, message := classifyClientError(err)
		g.writeAPIError(w, req.Command, &apiError{Status: status, Type: typ, Message: message})
		return
	}
	if response.IsError() {
		g.commandErrors.Add(1)
		g.writeJSON(w, http.StatusBadRequest, commandResponse{OK: false, Command: req.Command, Error: &errorJSON{Type: "redis_error", Message: response.Str}})
		return
	}
	g.writeJSON(w, http.StatusOK, commandResponse{OK: true, Command: req.Command, Response: toJSON(response)})
}

func (g *Gateway) clientFor(sessionID string) (client, bool, error) {
	sessionID = strings.TrimSpace(sessionID)
	if sessionID == "" {
		return g.newClient(), true, nil
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if c, ok := g.sessions[sessionID]; ok {
		return c, false, nil
	}
	c := g.newClient()
	g.sessions[sessionID] = c
	return c, false, nil
}

func (g *Gateway) handleKeys(w http.ResponseWriter, _ *http.Request) {
	g.writeJSON(w, http.StatusNotImplemented, commandResponse{OK: false, Error: &errorJSON{Type: "api_error", Message: "key enumeration is unavailable because MyRedis exposes no KEYS or SCAN command"}})
}

func (g *Gateway) handleKey(w http.ResponseWriter, r *http.Request, rawKey string) {
	key, err := urlPathUnescape(rawKey)
	if err != nil || key == "" || strings.Contains(key, "/") {
		g.writeAPIError(w, "", &apiError{Status: http.StatusBadRequest, Type: "api_validation_error", Message: "invalid key path"})
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), g.cfg.RequestTimeout)
	defer cancel()
	c, ephemeral, err := g.clientFor("")
	if err != nil {
		g.writeAPIError(w, "", &apiError{Status: http.StatusBadGateway, Type: "redis_connection_error", Message: "unable to create Redis client"})
		return
	}
	if ephemeral {
		defer c.Close()
	}
	typeResponse, err := c.DoContext(ctx, "TYPE", key)
	if err != nil {
		g.writeAPIError(w, "", redisAPIError(err))
		return
	}
	if typeResponse.IsError() {
		g.writeRedisError(w, typeResponse)
		return
	}
	typeName := responseString(typeResponse)
	if typeName == "none" {
		g.writeJSON(w, http.StatusNotFound, commandResponse{OK: false, Error: &errorJSON{Type: "api_error", Message: "key does not exist"}})
		return
	}
	var detail *redisclient.Response
	switch typeName {
	case "string":
		detail, err = c.DoContext(ctx, "GET", key)
	case "list":
		detail, err = c.DoContext(ctx, "LRANGE", key, "0", "-1")
	case "zset":
		detail, err = c.DoContext(ctx, "ZRANGE", key, "0", "-1")
	case "stream":
		detail, err = c.DoContext(ctx, "XRANGE", key, "-", "+")
	default:
		g.writeAPIError(w, "", &apiError{Status: http.StatusBadGateway, Type: "redis_protocol_error", Message: "unsupported Redis type response"})
		return
	}
	if err != nil {
		g.writeAPIError(w, "", redisAPIError(err))
		return
	}
	if detail.IsError() {
		g.writeRedisError(w, detail)
		return
	}
	g.writeJSON(w, http.StatusOK, map[string]interface{}{"ok": true, "key": key, "type": typeName, "response": toJSON(detail)})
}

func (g *Gateway) handleServerInfo(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), g.cfg.RequestTimeout)
	defer cancel()
	c, ephemeral, _ := g.clientFor("")
	if ephemeral {
		defer c.Close()
	}
	status := "ok"
	if response, err := c.DoContext(ctx, "PING"); err != nil || response == nil || response.IsError() {
		status = "unavailable"
	}
	g.writeJSON(w, http.StatusOK, map[string]interface{}{
		"ok": true, "status": status, "api_addr": g.cfg.APIAddr, "redis_addr": g.cfg.RedisAddr,
		"uptime_seconds": int(time.Since(g.startedAt).Seconds()), "supported_command_count": len(SupportedCommands()),
		"requests": g.requests.Load(), "command_requests": g.commandRequests.Load(), "command_errors": g.commandErrors.Load(),
		"redis_errors": g.redisErrors.Load(), "active_requests": g.activeRequests.Load(),
	})
}

func toJSON(response *redisclient.Response) *responseJSON {
	if response == nil || response.Type == redisclient.NullType {
		return &responseJSON{Type: "null", Value: nil}
	}
	result := &responseJSON{Type: string(response.Type)}
	switch response.Type {
	case redisclient.IntegerType:
		result.Value = response.Int
	case redisclient.ArrayType:
		values := make([]*responseJSON, 0, len(response.Array))
		for _, item := range response.Array {
			values = append(values, toJSON(item))
		}
		result.Value = values
	default:
		result.Value = response.Str
	}
	return result
}

func responseString(response *redisclient.Response) string {
	if response == nil {
		return ""
	}
	return response.Str
}

func classifyClientError(err error) (int, string, string) {
	if errors.Is(err, context.DeadlineExceeded) || strings.Contains(strings.ToLower(err.Error()), "timeout") {
		return http.StatusGatewayTimeout, "redis_timeout", "Redis request timed out"
	}
	if strings.Contains(strings.ToLower(err.Error()), "protocol") || strings.Contains(strings.ToLower(err.Error()), "resp") {
		return http.StatusBadGateway, "redis_protocol_error", "invalid response from Redis"
	}
	return http.StatusBadGateway, "redis_connection_error", "Redis connection failed"
}

func redisAPIError(err error) error {
	status, typ, message := classifyClientError(err)
	return &apiError{Status: status, Type: typ, Message: message}
}

func (g *Gateway) writeRedisError(w http.ResponseWriter, response *redisclient.Response) {
	g.writeJSON(w, http.StatusBadRequest, commandResponse{OK: false, Error: &errorJSON{Type: "redis_error", Message: response.Str}})
}

func (g *Gateway) writeAPIError(w http.ResponseWriter, command string, err error) {
	apiErr, ok := err.(*apiError)
	if !ok {
		apiErr = &apiError{Status: http.StatusInternalServerError, Type: "api_error", Message: "request failed"}
	}
	g.writeJSON(w, apiErr.Status, commandResponse{OK: false, Command: command, Error: &errorJSON{Type: apiErr.Type, Message: apiErr.Message}})
}

func (g *Gateway) writeJSON(w http.ResponseWriter, status int, value interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(value); err != nil {
		log.Printf(`{"event":"json_write_error","error":%q}`, err.Error())
	}
}

func urlPathUnescape(value string) (string, error) {
	// Avoid importing URL parsing into policy code; this endpoint only needs the
	// standard percent-decoding behavior.
	var out strings.Builder
	for i := 0; i < len(value); i++ {
		if value[i] != '%' {
			out.WriteByte(value[i])
			continue
		}
		if i+2 >= len(value) {
			return "", fmt.Errorf("invalid escape")
		}
		var n byte
		for _, ch := range []byte{value[i+1], value[i+2]} {
			n <<= 4
			switch {
			case ch >= '0' && ch <= '9':
				n += ch - '0'
			case ch >= 'a' && ch <= 'f':
				n += ch - 'a' + 10
			case ch >= 'A' && ch <= 'F':
				n += ch - 'A' + 10
			default:
				return "", fmt.Errorf("invalid escape")
			}
		}
		out.WriteByte(n)
		i += 2
	}
	return out.String(), nil
}
