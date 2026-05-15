package handler

import (
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

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
		key, value := cmd.Args[0], cmd.Args[1]

		// Parse optional PX / EX expiry arguments
		if len(cmd.Args) >= 4 {
			option := strings.ToUpper(cmd.Args[2])
			ttlVal, err := strconv.ParseInt(cmd.Args[3], 10, 64)
			if err != nil {
				conn.Write([]byte(resp.Error("value is not an integer or out of range")))
				return
			}
			var ttl time.Duration
			switch option {
			case "PX":
				ttl = time.Duration(ttlVal) * time.Millisecond
			case "EX":
				ttl = time.Duration(ttlVal) * time.Second
			default:
				conn.Write([]byte(resp.Error(fmt.Sprintf("unsupported option '%s'", option))))
				return
			}
			s.SetWithExpiry(key, value, ttl)
		} else {
			s.Set(key, value)
		}
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
