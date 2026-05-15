package handler

import (
	"fmt"
	"net"

	"github.com/codecrafters-io/redis-starter-go/app/resp"
	"github.com/codecrafters-io/redis-starter-go/app/store"
)

// Handle dispatches a parsed command and writes the RESP response to conn.
func Handle(cmd *resp.Command, conn net.Conn, s *store.Store) {
	switch cmd.Name {
	case "PING":
		conn.Write([]byte(resp.SimpleString("PONG")))
	case "ECHO":
		if len(cmd.Args) < 1 {
			conn.Write([]byte(resp.Error("wrong number of arguments for 'echo' command")))
			return
		}
		conn.Write([]byte(resp.BulkString(cmd.Args[0])))
	case "SET":
		if len(cmd.Args) < 2 {
			conn.Write([]byte(resp.Error("wrong number of arguments for 'set' command")))
			return
		}
		s.Set(cmd.Args[0], cmd.Args[1])
		conn.Write([]byte(resp.SimpleString("OK")))
	case "GET":
		if len(cmd.Args) < 1 {
			conn.Write([]byte(resp.Error("wrong number of arguments for 'get' command")))
			return
		}
		val, ok := s.Get(cmd.Args[0])
		if !ok {
			conn.Write([]byte(resp.NullBulkString()))
			return
		}
		conn.Write([]byte(resp.BulkString(val)))
	default:
		conn.Write([]byte(resp.Error(fmt.Sprintf("unknown command '%s'", cmd.Name))))
	}
}
