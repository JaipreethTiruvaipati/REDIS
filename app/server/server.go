package server

import (
	"bufio"
	"fmt"
	"io"
	"net"

	"github.com/codecrafters-io/redis-starter-go/app/handler"
	"github.com/codecrafters-io/redis-starter-go/app/resp"
	"github.com/codecrafters-io/redis-starter-go/app/store"
)

// Server holds the TCP listener configuration and shared state.
type Server struct {
	addr  string
	store *store.Store
}

// New creates a new Server instance.
func New(addr string) *Server {
	return &Server{
		addr:  addr,
		store: store.New(),
	}
}

// Start begins listening for incoming TCP connections.
func (s *Server) Start() error {
	l, err := net.Listen("tcp", s.addr)
	if err != nil {
		return fmt.Errorf("failed to bind to %s: %w", s.addr, err)
	}
	fmt.Printf("Server listening on %s\n", s.addr)

	for {
		conn, err := l.Accept()
		if err != nil {
			return fmt.Errorf("error accepting connection: %w", err)
		}
		go s.handleConnection(conn)
	}
}

// handleConnection reads and handles commands from a single client connection.
func (s *Server) handleConnection(conn net.Conn) {
	defer conn.Close()
	reader := bufio.NewReader(conn)
	for {
		cmd, err := resp.Parse(reader)
		if err != nil {
			if err != io.EOF {
				fmt.Println("Error parsing command:", err.Error())
			}
			return
		}
		handler.Handle(cmd, conn, s.store)
	}
}
