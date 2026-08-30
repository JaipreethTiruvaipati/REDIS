// Package redisclient provides a small TCP/RESP client for MyRedis.
// It intentionally knows nothing about Store internals or Redis data structures.
package redisclient

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"
	"sync"
	"time"
)

var (
	ErrNotConnected = errors.New("redis client is not connected")
	ErrClosed       = errors.New("redis client is closed")
)

// Config controls TCP connection and RESP reply limits.
type Config struct {
	Addr           string
	Username       string
	Password       string
	DialTimeout    time.Duration
	IOTimeout      time.Duration
	ResponseLimits ResponseLimits
}

func (c Config) withDefaults() Config {
	if c.Addr == "" {
		c.Addr = "127.0.0.1:6379"
	}
	if c.Password != "" && c.Username == "" {
		c.Username = "default"
	}
	if c.DialTimeout <= 0 {
		c.DialTimeout = 3 * time.Second
	}
	if c.IOTimeout <= 0 {
		c.IOTimeout = 10 * time.Second
	}
	if c.ResponseLimits.MaxBulkStringLength == 0 {
		c.ResponseLimits.MaxBulkStringLength = DefaultResponseLimits.MaxBulkStringLength
	}
	if c.ResponseLimits.MaxArrayElements == 0 {
		c.ResponseLimits.MaxArrayElements = DefaultResponseLimits.MaxArrayElements
	}
	if c.ResponseLimits.MaxDepth == 0 {
		c.ResponseLimits.MaxDepth = DefaultResponseLimits.MaxDepth
	}
	return c
}

// Client is safe for sequential/concurrent callers; a command round trip is
// serialized so a reused connection cannot mix request and response frames.
type Client struct {
	mu     sync.Mutex
	cfg    Config
	conn   net.Conn
	reader *bufio.Reader
	closed bool
}

func New(cfg Config) *Client { return &Client{cfg: cfg.withDefaults()} }

func (c *Client) Config() Config { return c.cfg }

func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closed = true
	if c.conn == nil {
		return nil
	}
	err := c.conn.Close()
	c.conn = nil
	c.reader = nil
	return err
}

func (c *Client) Do(args ...string) (*Response, error) {
	return c.DoContext(context.Background(), args...)
}

func (c *Client) DoContext(ctx context.Context, args ...string) (*Response, error) {
	if len(args) == 0 {
		return nil, fmt.Errorf("redis command must not be empty")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil, ErrClosed
	}
	if err := c.ensureConnLocked(ctx); err != nil {
		return nil, err
	}
	if err := c.roundTripLocked(ctx, args); err != nil {
		c.closeConnLocked()
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, err
	}
	response, err := decodeResponse(c.reader, c.cfg.ResponseLimits)
	if err != nil {
		c.closeConnLocked()
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, fmt.Errorf("redis protocol error: %w", err)
	}
	return response, nil
}

func (c *Client) ensureConnLocked(ctx context.Context) error {
	if c.conn != nil {
		return nil
	}
	dialer := net.Dialer{Timeout: c.cfg.DialTimeout}
	conn, err := dialer.DialContext(ctx, "tcp", c.cfg.Addr)
	if err != nil {
		return fmt.Errorf("redis connection failed: %w", err)
	}
	c.conn = conn
	c.reader = bufio.NewReader(conn)
	if c.cfg.Username != "" || c.cfg.Password != "" {
		args := []string{"AUTH"}
		if c.cfg.Username != "" {
			args = append(args, c.cfg.Username)
		}
		args = append(args, c.cfg.Password)
		if err := c.roundTripLocked(ctx, args); err != nil {
			c.closeConnLocked()
			return err
		}
		response, err := decodeResponse(c.reader, c.cfg.ResponseLimits)
		if err != nil {
			c.closeConnLocked()
			return fmt.Errorf("redis AUTH protocol error: %w", err)
		}
		if response.IsError() {
			c.closeConnLocked()
			return fmt.Errorf("redis AUTH failed: %s", response.Str)
		}
	}
	return nil
}

func (c *Client) roundTripLocked(ctx context.Context, args []string) error {
	if c.conn == nil {
		return ErrNotConnected
	}
	deadline := time.Now().Add(c.cfg.IOTimeout)
	if ctxDeadline, ok := ctx.Deadline(); ok && ctxDeadline.Before(deadline) {
		deadline = ctxDeadline
	}
	if err := c.conn.SetDeadline(deadline); err != nil {
		return err
	}
	stop := make(chan struct{})
	go func(conn net.Conn) {
		select {
		case <-ctx.Done():
			_ = conn.Close()
		case <-stop:
		}
	}(c.conn)
	defer close(stop)
	var frame bytes.Buffer
	frame.WriteString("*" + strconv.Itoa(len(args)) + "\r\n")
	for _, arg := range args {
		frame.WriteString("$" + strconv.Itoa(len([]byte(arg))) + "\r\n")
		frame.WriteString(arg)
		frame.WriteString("\r\n")
	}
	_, err := c.conn.Write(frame.Bytes())
	return err
}

func (c *Client) closeConnLocked() {
	if c.conn != nil {
		_ = c.conn.Close()
	}
	c.conn = nil
	c.reader = nil
}

// ParseCommandLine parses the simple command language used by the HTTP API.
// It supports whitespace-separated arguments and single/double quoted values
// with backslash escapes, without executing shell syntax.
func ParseCommandLine(input string, maxBytes int) ([]string, error) {
	if maxBytes > 0 && len([]byte(input)) > maxBytes {
		return nil, fmt.Errorf("command exceeds maximum length")
	}
	var args []string
	var current []byte
	tokenStarted := false
	var quote byte
	escaped := false
	flush := func() {
		if tokenStarted {
			args = append(args, string(current))
			current = nil
			tokenStarted = false
		}
	}
	for i := 0; i < len(input); i++ {
		ch := input[i]
		if escaped {
			current = append(current, ch)
			tokenStarted = true
			escaped = false
			continue
		}
		if ch == '\\' {
			tokenStarted = true
			escaped = true
			continue
		}
		if quote != 0 {
			if ch == quote {
				quote = 0
			} else {
				current = append(current, ch)
			}
			tokenStarted = true
			continue
		}
		switch ch {
		case '\'', '"':
			quote = ch
			tokenStarted = true
		case ' ', '\t', '\r', '\n':
			flush()
		default:
			current = append(current, ch)
			tokenStarted = true
		}
	}
	if escaped || quote != 0 {
		return nil, fmt.Errorf("unterminated escape or quote")
	}
	flush()
	if len(args) == 0 {
		return nil, fmt.Errorf("command must not be empty")
	}
	return args, nil
}
