package handler

import (
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/codecrafters-io/redis-starter-go/app/stream"

	"github.com/codecrafters-io/redis-starter-go/app/auth"
	"github.com/codecrafters-io/redis-starter-go/app/resp"
	"github.com/codecrafters-io/redis-starter-go/app/store"
	"github.com/codecrafters-io/redis-starter-go/app/transactions"
)

// Handle processes a single parsed command and writes the response to the connection.
func Handle(cmd *resp.Command, conn net.Conn, s *store.Store, currentUser **auth.User, tx *transactions.State) {
	cmdName := strings.ToUpper(cmd.Name)

	if *currentUser == nil && cmdName != "AUTH" {
		conn.Write([]byte("-NOAUTH Authentication required.\r\n"))
		return
	}

	switch cmdName {
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
		args := cmd.Args
		isBlocking := false
		var blockTimeout time.Duration

		// Parse optional BLOCK <milliseconds> at the beginning
		if len(args) > 0 && strings.ToUpper(args[0]) == "BLOCK" {
			if len(args) < 2 {
				conn.Write([]byte(resp.Error("syntax error")))
				return
			}
			blockMs, err := strconv.ParseInt(args[1], 10, 64)
			if err != nil || blockMs < 0 {
				conn.Write([]byte(resp.Error("timeout is not an integer or out of range")))
				return
			}
			blockTimeout = time.Duration(blockMs) * time.Millisecond
			isBlocking = true
			args = args[2:] // skip past "BLOCK <ms>"
		}

		// Now expect: STREAMS key1 [key2 ...] id1 [id2 ...]
		if len(args) < 3 || strings.ToUpper(args[0]) != "STREAMS" {
			conn.Write([]byte(resp.Error("syntax error")))
			return
		}

		remaining := args[1:] // everything after "STREAMS"
		if len(remaining)%2 != 0 {
			conn.Write([]byte(resp.Error("unbalanced STREAMS list")))
			return
		}

		half := len(remaining) / 2
		keys := remaining[:half]
		idStrs := remaining[half:]

		if !isBlocking {
			// Non-blocking: return immediately
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
		} else {
			// Blocking: use BXRead for the first key
			key := keys[0]
			var afterID stream.EntryID
			if idStrs[0] == "$" {
				// $ = current lastID of the stream — only return entries added AFTER this command
				afterID = s.GetStreamLastID(key)
			} else {
				var err error
				afterID, err = stream.Parse(idStrs[0])
				if err != nil {
					conn.Write([]byte(resp.Error(err.Error())))
					return
				}
			}

			entries, ok := s.BXRead(key, afterID, blockTimeout)
			if !ok {
				conn.Write([]byte(resp.NullArray()))
				return
			}
			results := []stream.ReadResult{{Key: key, Entries: entries}}
			conn.Write([]byte(resp.StreamReadResults(results)))
		}
	case "ACL":
		if len(cmd.Args) < 1 {
			conn.Write([]byte(resp.Error("wrong number of arguments for 'acl' command")))
			return
		}
		switch strings.ToUpper(cmd.Args[0]) {
		case "WHOAMI":
			conn.Write([]byte(resp.BulkString((*currentUser).Username)))

		case "GETUSER":
			if len(cmd.Args) < 2 {
				conn.Write([]byte(resp.Error("wrong number of arguments for 'acl|getuser' command")))
				return
			}
			user, ok := auth.GetUser(cmd.Args[1])
			if !ok {
				conn.Write([]byte(resp.NullBulkString()))
				return
			}
			response := "*4\r\n" +
				resp.BulkString("flags") + resp.Array(user.Flags) +
				resp.BulkString("passwords") + resp.Array(user.Passwords)
			conn.Write([]byte(response))

		case "SETUSER":
			if len(cmd.Args) < 2 {
				conn.Write([]byte(resp.Error("wrong number of arguments for 'acl|setuser' command")))
				return
			}
			username := cmd.Args[1]
			user, ok := auth.GetUser(username)
			if !ok {
				conn.Write([]byte(resp.Error(fmt.Sprintf("ERR User '%s' not found", username))))
				return
			}
			for _, rule := range cmd.Args[2:] {
				if strings.HasPrefix(rule, ">") {
					user.SetPassword(rule[1:])
				}
			}
			conn.Write([]byte(resp.SimpleString("OK")))

		default:
			conn.Write([]byte(resp.Error(fmt.Sprintf("unknown subcommand '%s' for 'acl' command", cmd.Args[0]))))
		}

	case "AUTH":
		// Format: AUTH <username> <password>
		if len(cmd.Args) < 2 {
			conn.Write([]byte(resp.Error("wrong number of arguments for 'auth' command")))
			return
		}
		username := cmd.Args[0]
		password := cmd.Args[1]
		user, ok := auth.GetUser(username)
		if !ok || !user.Authenticate(password) {
			conn.Write([]byte("-WRONGPASS invalid username-password pair or user is disabled\r\n"))
			return
		}
		*currentUser = user
		conn.Write([]byte(resp.SimpleString("OK")))
	case "ZADD":
		// Format: ZADD key score member
		if len(cmd.Args) < 3 {
			conn.Write([]byte(resp.Error("wrong number of arguments for 'zadd' command")))
			return
		}

		key := cmd.Args[0]
		scoreStr := cmd.Args[1]
		member := cmd.Args[2]

		// Parse the score as a 64-bit float
		score, err := strconv.ParseFloat(scoreStr, 64)
		if err != nil {
			conn.Write([]byte(resp.Error("ERR value is not a valid float")))
			return
		}

		addedCount := s.ZAdd(key, score, member)
		conn.Write([]byte(resp.Integer(addedCount)))

	case "ZRANK":
		// Format: ZRANK key member
		if len(cmd.Args) < 2 {
			conn.Write([]byte(resp.Error("wrong number of arguments for 'zrank' command")))
			return
		}

		key := cmd.Args[0]
		member := cmd.Args[1]

		rank, exists := s.ZRank(key, member)
		if !exists {
			conn.Write([]byte(resp.NullBulkString()))
			return
		}

		conn.Write([]byte(resp.Integer(rank)))

	case "ZRANGE":
		// Format: ZRANGE key start stop
		if len(cmd.Args) < 3 {
			conn.Write([]byte(resp.Error("wrong number of arguments for 'zrange' command")))
			return
		}

		key := cmd.Args[0]
		start, err1 := strconv.Atoi(cmd.Args[1])
		stop, err2 := strconv.Atoi(cmd.Args[2])

		if err1 != nil || err2 != nil {
			conn.Write([]byte(resp.Error("ERR value is not an integer or out of range")))
			return
		}

		members := s.ZRange(key, start, stop)
		conn.Write([]byte(resp.Array(members)))
	case "ZCARD":
		// Format: ZCARD key
		if len(cmd.Args) < 1 {
			conn.Write([]byte(resp.Error("wrong number of arguments for 'zcard' command")))
			return
		}

		key := cmd.Args[0]
		card := s.ZCard(key)
		conn.Write([]byte(resp.Integer(card)))
	case "ZSCORE":
		// Format: ZSCORE key member
		if len(cmd.Args) < 2 {
			conn.Write([]byte(resp.Error("wrong number of arguments for 'zscore' command")))
			return
		}

		key := cmd.Args[0]
		member := cmd.Args[1]

		score, exists := s.ZScore(key, member)
		if !exists {
			conn.Write([]byte(resp.NullBulkString()))
			return
		}

		// Convert float to string cleanly (removing trailing zeros if any)
		scoreStr := strconv.FormatFloat(score, 'f', -1, 64)
		conn.Write([]byte(resp.BulkString(scoreStr)))
	case "ZREM":
		// Format: ZREM key member
		if len(cmd.Args) < 2 {
			conn.Write([]byte(resp.Error("wrong number of arguments for 'zrem' command")))
			return
		}

		key := cmd.Args[0]
		member := cmd.Args[1]

		removedCount := s.ZRem(key, member)
		conn.Write([]byte(resp.Integer(removedCount)))
	case "INCR":
		// Format: INCR key
		if len(cmd.Args) < 1 {
			conn.Write([]byte(resp.Error("wrong number of arguments for 'incr' command")))
			return
		}

		newVal, err := s.Incr(cmd.Args[0])
		if err != nil {
			// Stage 1 won't hit this; later stages map missing/non-numeric keys here
			conn.Write([]byte(resp.Error("value is not an integer or out of range")))
			return
		}

		conn.Write([]byte(resp.Integer(newVal)))
	case "MULTI":
		// Format: MULTI — starts a transaction; further commands are queued (later stages)
		tx.Begin()
		conn.Write([]byte(resp.SimpleString("OK")))
	case "EXEC":
		// Format: EXEC — runs queued commands; error if MULTI was not called
		if !tx.InTransaction {
			conn.Write([]byte(resp.Error("EXEC without MULTI")))
			return
		}
	
	default:
		conn.Write([]byte(resp.Error(fmt.Sprintf("unknown command '%s'", cmd.Name))))
	}
}
