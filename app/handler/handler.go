package handler

import (
	"fmt"
	"net"

	"github.com/codecrafters-io/redis-starter-go/app/resp"
)

// Handle dispatches a parsed command and writes the RESP response to conn.
func Handle(cmd *resp.Command, conn net.Conn) {
	switch cmd.Name {
	case "PING":
		conn.Write([]byte(resp.SimpleString("PONG")))
	case "ECHO":
		if len(cmd.Args) < 1 {
			conn.Write([]byte(resp.Error("wrong number of arguments for 'echo' command")))
			return
		}
		conn.Write([]byte(resp.BulkString(cmd.Args[0])))
	default:
		conn.Write([]byte(resp.Error(fmt.Sprintf("unknown command '%s'", cmd.Name))))
	}
}
