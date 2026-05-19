package handler

import (
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/codecrafters-io/redis-starter-go/app/stream"

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

	case "RPUSH":
		if len(cmd.Args) < 2 {
			conn.Write([]byte(resp.Error("wrong number of arguments for 'rpush' command")))
			return
		}
		key := cmd.Args[0]
		values := cmd.Args[1:]
		newLen := s.RPush(key, values...)
		conn.Write([]byte(resp.Integer(newLen)))
	case "LRANGE":
		if len(cmd.Args) < 3 {
			conn.Write([]byte(resp.Error("wrong number of arguments for 'lrange' command")))
			return
		}
		key := cmd.Args[0]
		start, err := strconv.Atoi(cmd.Args[1])
		if err != nil {
			conn.Write([]byte(resp.Error("value is not an integer or out of range")))
			return
		}
		stop, err := strconv.Atoi(cmd.Args[2])
		if err != nil {
			conn.Write([]byte(resp.Error("value is not an integer or out of range")))
			return
		}
		items := s.LRange(key, start, stop)
		conn.Write([]byte(resp.Array(items)))
	case "LPUSH":
		if len(cmd.Args) < 2 {
			conn.Write([]byte(resp.Error("wrong number of arguments for 'lpush' command")))
			return
		}
		key := cmd.Args[0]
		values := cmd.Args[1:]
		newLen := s.LPush(key, values...)
		conn.Write([]byte(resp.Integer(newLen)))
	case "LLEN":
		if len(cmd.Args) < 1 {
			conn.Write([]byte(resp.Error("wrong number of arguments for 'llen' command")))
			return
		}
		conn.Write([]byte(resp.Integer(s.LLen(cmd.Args[0]))))
	case "LPOP":
		if len(cmd.Args) < 1 {
			conn.Write([]byte(resp.Error("wrong number of arguments for 'lpop' command")))
			return
		}
		key := cmd.Args[0]

		// LPOP key count → returns RESP array
		if len(cmd.Args) >= 2 {
			count, err := strconv.Atoi(cmd.Args[1])
			if err != nil || count < 0 {
				conn.Write([]byte(resp.Error("value is not an integer or out of range")))
				return
			}
			items := s.LPopN(key, count)
			conn.Write([]byte(resp.Array(items)))
			return
		}

		// LPOP key → returns single bulk string (original behavior)
		val, ok := s.LPop(key)
		if !ok {
			conn.Write([]byte(resp.NullBulkString()))
			return
		}
		conn.Write([]byte(resp.BulkString(val)))
	case "BLPOP":
		// Format: BLPOP key [key ...] timeout
		if len(cmd.Args) < 2 {
			conn.Write([]byte(resp.Error("wrong number of arguments for 'blpop' command")))
			return
		}
		key := cmd.Args[0]
		timeoutStr := cmd.Args[len(cmd.Args)-1]
		timeoutSecs, err := strconv.ParseFloat(timeoutStr, 64)
		if err != nil || timeoutSecs < 0 {
			conn.Write([]byte(resp.Error("timeout is not a float or out of range")))
			return
		}
		timeout := time.Duration(timeoutSecs * float64(time.Second))
		val, ok := s.BLPop(key, timeout)
		if !ok {
			conn.Write([]byte(resp.NullArray()))
			return
		}
		conn.Write([]byte(resp.Array([]string{key, val})))
	case "TYPE":
		if len(cmd.Args) < 1 {
			conn.Write([]byte(resp.Error("wrong number of arguments for 'type' command")))
			return
		}
		conn.Write([]byte(resp.SimpleString(s.Type(cmd.Args[0]))))
	case "XADD":
		// Format: XADD key id field1 value1 [field2 value2 ...]
		if len(cmd.Args) < 4 || (len(cmd.Args)-2)%2 != 0 {
			conn.Write([]byte(resp.Error("wrong number of arguments for 'xadd' command")))
			return
		}
		key := cmd.Args[0]
		idStr := cmd.Args[1]
		fields := cmd.Args[2:] // [field1, value1, field2, value2, ...]

		entryID, err := s.XAdd(key, idStr, fields)
		if err != nil {
			conn.Write([]byte(resp.Error(err.Error())))
			return
		}
		conn.Write([]byte(resp.BulkString(entryID)))
	case "XRANGE":
		// Format: XRANGE key start end
		if len(cmd.Args) < 3 {
			conn.Write([]byte(resp.Error("wrong number of arguments for 'xrange' command")))
			return
		}
		key := cmd.Args[0]
		start, err := stream.ParseRangeStart(cmd.Args[1])
		if err != nil {
			conn.Write([]byte(resp.Error(err.Error())))
			return
		}
		end, err := stream.ParseRangeEnd(cmd.Args[2])
		if err != nil {
			conn.Write([]byte(resp.Error(err.Error())))
			return
		}
		entries := s.XRange(key, start, end)
		conn.Write([]byte(resp.StreamEntries(entries)))
	case "XREAD":
		// Format: XREAD STREAMS key1 [key2 ...] id1 [id2 ...]
		if len(cmd.Args) < 3 || strings.ToUpper(cmd.Args[0]) != "STREAMS" {
			conn.Write([]byte(resp.Error("syntax error")))
			return
		}

		remaining := cmd.Args[1:] // everything after "STREAMS"
		if len(remaining)%2 != 0 {
			conn.Write([]byte(resp.Error("unbalanced STREAMS list")))
			return
		}

		half := len(remaining) / 2
		keys := remaining[:half]
		idStrs := remaining[half:]

		afterIDs := make([]stream.EntryID, len(idStrs))
		for i, idStr := range idStrs {
			id, err := stream.Parse(idStr)
			if err != nil {
				conn.Write([]byte(resp.Error(err.Error())))
				return
			}
			afterIDs[i] = id
		}

		results := s.XRead(keys, afterIDs)
		if len(results) == 0 {
			conn.Write([]byte(resp.NullArray()))
			return
		}
		conn.Write([]byte(resp.StreamReadResults(results)))

	default:
		conn.Write([]byte(resp.Error(fmt.Sprintf("unknown command '%s'", cmd.Name))))
	}
}
