package server

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"sync"
	"time"

	"github.com/jaipreethtiruvaipati/redis-clone/app/auth"
	"github.com/jaipreethtiruvaipati/redis-clone/app/handler"
	"github.com/jaipreethtiruvaipati/redis-clone/app/resp"
	"github.com/jaipreethtiruvaipati/redis-clone/app/store"
	"github.com/jaipreethtiruvaipati/redis-clone/app/transactions"
)

// Server holds the TCP listener configuration and shared state.
type Server struct {
	addr  string
	store *store.Store

	mu        sync.Mutex
	listener  net.Listener
	cancel    context.CancelFunc
	startDone chan struct{}
	conns     map[net.Conn]struct{}
	wg        sync.WaitGroup
}

type loggingConn struct {
	net.Conn
	remote string
}

// connectionReader serializes parser and disconnect-monitor reads. Bytes read
// by the monitor while a command is blocked are retained for the parser.
type connectionReader struct {
	conn    net.Conn
	readMu  sync.Mutex
	pending []byte
}

func (r *connectionReader) Read(p []byte) (int, error) {
	r.readMu.Lock()
	defer r.readMu.Unlock()
	if len(r.pending) > 0 {
		n := copy(p, r.pending)
		r.pending = r.pending[n:]
		return n, nil
	}
	return r.conn.Read(p)
}

func (r *connectionReader) monitor(ctx context.Context, cancel context.CancelFunc) {
	defer cancel()
	buf := make([]byte, 1024)
	for {
		if ctx.Err() != nil {
			return
		}
		r.readMu.Lock()
		_ = r.conn.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
		n, err := r.conn.Read(buf)
		if n > 0 {
			if len(r.pending)+n > resp.DefaultLimits.MaxBulkStringLength {
				r.readMu.Unlock()
				return
			}
			r.pending = append(r.pending, buf[:n]...)
		}
		r.readMu.Unlock()
		if err != nil {
			var netErr net.Error
			if errors.As(err, &netErr) && netErr.Timeout() {
				continue
			}
			return
		}
	}
}

func (c *loggingConn) Write(p []byte) (int, error) {
	// Bound time spent blocked on a slow client, then restore an idle deadline.
	_ = c.Conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
	n, err := c.Conn.Write(p)
	_ = c.Conn.SetWriteDeadline(time.Time{})
	if err != nil {
		log.Printf("write error to %s: %v", c.remote, err)
	}
	return n, err
}

// New creates a new Server instance.
func New(addr string) *Server {
	return &Server{addr: addr, store: store.New(), conns: make(map[net.Conn]struct{})}
}

// Addr returns the active listener address, or the configured address before
// Start has bound the listener.
func (s *Server) Addr() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.listener != nil {
		return s.listener.Addr().String()
	}
	return s.addr
}

// Start begins listening for incoming TCP connections and blocks until the
// listener is closed by Shutdown or an unrecoverable accept error occurs.
func (s *Server) Start() error {
	l, err := net.Listen("tcp", s.addr)
	if err != nil {
		return fmt.Errorf("failed to bind to %s: %w", s.addr, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	s.mu.Lock()
	s.listener = l
	s.cancel = cancel
	s.startDone = make(chan struct{})
	startDone := s.startDone
	s.mu.Unlock()
	log.Printf("server listening on %s", l.Addr())

	defer func() {
		s.mu.Lock()
		if s.listener == l {
			s.listener = nil
		}
		s.mu.Unlock()
		cancel()
		close(startDone)
	}()

	for {
		conn, err := l.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) || errors.Is(ctx.Err(), context.Canceled) {
				return nil
			}
			var netErr net.Error
			if errors.As(err, &netErr) && netErr.Temporary() {
				log.Printf("temporary accept error: %v", err)
				continue
			}
			return fmt.Errorf("error accepting connection: %w", err)
		}
		s.mu.Lock()
		if ctx.Err() != nil {
			s.mu.Unlock()
			_ = conn.Close()
			continue
		}
		s.conns[conn] = struct{}{}
		s.wg.Add(1)
		s.mu.Unlock()
		go s.handleConnection(ctx, conn)
	}
}

// Shutdown gracefully stops accepting clients, cancels blocked requests and
// closes active connections. It is safe to call more than once.
func (s *Server) Shutdown(ctx context.Context) error {
	s.mu.Lock()
	if s.cancel != nil {
		s.cancel()
	}
	l := s.listener
	startDone := s.startDone
	if l != nil {
		_ = l.Close()
	}
	for conn := range s.conns {
		_ = conn.Close()
	}
	s.mu.Unlock()
	if startDone != nil {
		select {
		case <-startDone:
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// handleConnection reads and handles commands from a single client connection.
func (s *Server) handleConnection(ctx context.Context, conn net.Conn) {
	defer func() {
		_ = conn.Close()
		s.mu.Lock()
		delete(s.conns, conn)
		s.mu.Unlock()
		s.wg.Done()
	}()
	connReader := &connectionReader{conn: conn}
	reader := bufio.NewReader(connReader)
	clientConn := &loggingConn{Conn: conn, remote: conn.RemoteAddr().String()}

	var currentUser *auth.User
	defaultUser, _ := auth.GetUser("default")
	if defaultUser != nil && defaultUser.IsNoPass() {
		currentUser = defaultUser
	}
	var txState transactions.State
	for {
		// Protect idle connections from holding a goroutine forever. The deadline
		// is cleared while a blocking command is being served.
		_ = conn.SetReadDeadline(time.Now().Add(5 * time.Minute))
		cmd, err := resp.Parse(reader)
		if err != nil {
			if !errors.Is(err, io.EOF) && !errors.Is(err, net.ErrClosed) && ctx.Err() == nil {
				log.Printf("error parsing command from %s: %v", conn.RemoteAddr(), err)
			}
			return
		}
		_ = conn.SetReadDeadline(time.Time{})
		if handler.IsBlockingCommand(cmd) {
			blockCtx, cancel := context.WithCancel(ctx)
			monitorDone := make(chan struct{})
			go func() {
				connReader.monitor(blockCtx, cancel)
				close(monitorDone)
			}()
			handler.HandleContext(blockCtx, cmd, clientConn, s.store, &currentUser, &txState)
			cancel()
			<-monitorDone
			_ = conn.SetReadDeadline(time.Time{})
		} else {
			handler.HandleContext(ctx, cmd, clientConn, s.store, &currentUser, &txState)
		}
		if ctx.Err() != nil {
			return
		}
	}
}
