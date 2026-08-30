package handler

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/jaipreethtiruvaipati/redis-clone/app/stream"

	"github.com/jaipreethtiruvaipati/redis-clone/app/auth"
	"github.com/jaipreethtiruvaipati/redis-clone/app/resp"
	"github.com/jaipreethtiruvaipati/redis-clone/app/store"
	"github.com/jaipreethtiruvaipati/redis-clone/app/transactions"
)

// Handle processes a single parsed command and writes the response to the connection.
func Handle(cmd *resp.Command, conn net.Conn, s *store.Store, currentUser **auth.User, tx *transactions.State) {
	HandleContext(context.Background(), cmd, conn, s, currentUser, tx)
}

// HandleContext processes a command and observes ctx for blocking operations.
func HandleContext(ctx context.Context, cmd *resp.Command, conn net.Conn, s *store.Store, currentUser **auth.User, tx *transactions.State) {
	cmdName := strings.ToUpper(cmd.Name)

	if *currentUser == nil && cmdName != "AUTH" {
		conn.Write([]byte("-NOAUTH Authentication required.\r\n"))
		return
	}
	// While inside a transaction, queue commands instead of executing them.
	// EXEC and DISCARD run immediately without being queued.
	if tx.InTransaction && cmdName != "EXEC" && cmdName != "DISCARD" && cmdName != "MULTI" {
		tx.Enqueue(cmd)
		conn.Write([]byte(resp.SimpleString("QUEUED"))) // +QUEUED\r\n
		return
	}

	if isPotentiallyBlocking(cmd) {
		dispatchContext(ctx, cmd, conn, s, currentUser, tx)
		return
	}
	s.WithCommandLock(func() {
		dispatchContext(ctx, cmd, conn, s, currentUser, tx)
	})
}

func isPotentiallyBlocking(cmd *resp.Command) bool {
	if strings.EqualFold(cmd.Name, "BLPOP") {
		return true
	}
	return strings.EqualFold(cmd.Name, "XREAD") && len(cmd.Args) > 0 && strings.EqualFold(cmd.Args[0], "BLOCK")
}

// IsBlockingCommand reports whether a command may wait for another client.
// It is used by the TCP server to attach disconnect cancellation.
func IsBlockingCommand(cmd *resp.Command) bool { return isPotentiallyBlocking(cmd) }

// dispatch runs a single command and writes the RESP reply to w.
// Used for normal command handling and for executing queued commands on EXEC.
func dispatch(cmd *resp.Command, w io.Writer, s *store.Store, currentUser **auth.User, tx *transactions.State) {
	dispatchContext(context.Background(), cmd, w, s, currentUser, tx)
}

func dispatchContext(ctx context.Context, cmd *resp.Command, w io.Writer, s *store.Store, currentUser **auth.User, tx *transactions.State) {
	cmdName := strings.ToUpper(cmd.Name)

	switch cmdName {
	case "PING":
		if len(cmd.Args) > 1 {
			w.Write([]byte(resp.Error("wrong number of arguments for 'ping' command")))
			return
		}
		if len(cmd.Args) == 1 {
			w.Write([]byte(resp.BulkString(cmd.Args[0])))
			return
		}
		w.Write([]byte(resp.SimpleString("PONG")))

	case "ECHO":
		if len(cmd.Args) != 1 {
			w.Write([]byte(resp.Error("wrong number of arguments for 'echo' command")))
			return
		}
		w.Write([]byte(resp.BulkString(cmd.Args[0])))

	case "SET":
		if len(cmd.Args) < 2 {
			w.Write([]byte(resp.Error("wrong number of arguments for 'set' command")))
			return
		}
		if len(cmd.Args) != 2 && len(cmd.Args) != 4 {
			w.Write([]byte(resp.Error("syntax error")))
			return
		}
		key, value := cmd.Args[0], cmd.Args[1]
		if len(cmd.Args) >= 4 {
			option := strings.ToUpper(cmd.Args[2])
			ttlVal, err := strconv.ParseInt(cmd.Args[3], 10, 64)
			if err != nil || ttlVal <= 0 {
				w.Write([]byte(resp.Error("value is not an integer or out of range")))
				return
			}
			var ttl time.Duration
			switch option {
			case "PX":
				if ttlVal > int64((1<<63-1)/int64(time.Millisecond)) {
					w.Write([]byte(resp.Error("value is not an integer or out of range")))
					return
				}
				ttl = time.Duration(ttlVal) * time.Millisecond
			case "EX":
				if ttlVal > int64((1<<63-1)/int64(time.Second)) {
					w.Write([]byte(resp.Error("value is not an integer or out of range")))
					return
				}
				ttl = time.Duration(ttlVal) * time.Second
			default:
				w.Write([]byte(resp.Error(fmt.Sprintf("unsupported option '%s'", option))))
				return
			}
			if err := s.SetWithExpiryChecked(key, value, ttl); err != nil {
				w.Write([]byte(resp.Error(err.Error())))
				return
			}
		} else {
			if err := s.SetChecked(key, value); err != nil {
				w.Write([]byte(resp.Error(err.Error())))
				return
			}
		}
		w.Write([]byte(resp.SimpleString("OK")))

	case "GET":
		if len(cmd.Args) != 1 {
			w.Write([]byte(resp.Error("wrong number of arguments for 'get' command")))
			return
		}
		val, ok, err := s.GetChecked(cmd.Args[0])
		if err != nil {
			w.Write([]byte(resp.Error(err.Error())))
			return
		}
		if !ok {
			w.Write([]byte(resp.NullBulkString()))
			return
		}
		w.Write([]byte(resp.BulkString(val)))

	case "RPUSH":
		if len(cmd.Args) < 2 {
			w.Write([]byte(resp.Error("wrong number of arguments for 'rpush' command")))
			return
		}
		key := cmd.Args[0]
		values := cmd.Args[1:]
		newLen, err := s.RPushChecked(key, values...)
		if err != nil {
			w.Write([]byte(resp.Error(err.Error())))
			return
		}
		w.Write([]byte(resp.Integer(newLen)))
	case "LRANGE":
		if len(cmd.Args) != 3 {
			w.Write([]byte(resp.Error("wrong number of arguments for 'lrange' command")))
			return
		}
		key := cmd.Args[0]
		start, err := strconv.Atoi(cmd.Args[1])
		if err != nil {
			w.Write([]byte(resp.Error("value is not an integer or out of range")))
			return
		}
		stop, err := strconv.Atoi(cmd.Args[2])
		if err != nil {
			w.Write([]byte(resp.Error("value is not an integer or out of range")))
			return
		}
		items, err := s.LRangeChecked(key, start, stop)
		if err != nil {
			w.Write([]byte(resp.Error(err.Error())))
			return
		}
		w.Write([]byte(resp.Array(items)))
	case "LPUSH":
		if len(cmd.Args) < 2 {
			w.Write([]byte(resp.Error("wrong number of arguments for 'lpush' command")))
			return
		}
		key := cmd.Args[0]
		values := cmd.Args[1:]
		newLen, err := s.LPushChecked(key, values...)
		if err != nil {
			w.Write([]byte(resp.Error(err.Error())))
			return
		}
		w.Write([]byte(resp.Integer(newLen)))
	case "LLEN":
		if len(cmd.Args) != 1 {
			w.Write([]byte(resp.Error("wrong number of arguments for 'llen' command")))
			return
		}
		length, err := s.LLenChecked(cmd.Args[0])
		if err != nil {
			w.Write([]byte(resp.Error(err.Error())))
			return
		}
		w.Write([]byte(resp.Integer(length)))
	case "LPOP":
		if len(cmd.Args) < 1 || len(cmd.Args) > 2 {
			w.Write([]byte(resp.Error("wrong number of arguments for 'lpop' command")))
			return
		}
		key := cmd.Args[0]

		// LPOP key count → returns RESP array
		if len(cmd.Args) >= 2 {
			count, err := strconv.Atoi(cmd.Args[1])
			if err != nil || count < 0 {
				w.Write([]byte(resp.Error("value is not an integer or out of range")))
				return
			}
			items, err := s.LPopNChecked(key, count)
			if err != nil {
				w.Write([]byte(resp.Error(err.Error())))
				return
			}
			w.Write([]byte(resp.Array(items)))
			return
		}

		// LPOP key → returns single bulk string (original behavior)
		val, ok, err := s.LPopChecked(key)
		if err != nil {
			w.Write([]byte(resp.Error(err.Error())))
			return
		}
		if !ok {
			w.Write([]byte(resp.NullBulkString()))
			return
		}
		w.Write([]byte(resp.BulkString(val)))
	case "BLPOP":
		// Format: BLPOP key [key ...] timeout
		if len(cmd.Args) < 2 {
			w.Write([]byte(resp.Error("wrong number of arguments for 'blpop' command")))
			return
		}
		keys := cmd.Args[:len(cmd.Args)-1]
		timeoutStr := cmd.Args[len(cmd.Args)-1]
		timeoutSecs, err := strconv.ParseFloat(timeoutStr, 64)
		if err != nil || timeoutSecs < 0 || math.IsNaN(timeoutSecs) || math.IsInf(timeoutSecs, 0) || timeoutSecs > float64((1<<63-1)/int64(time.Second)) {
			w.Write([]byte(resp.Error("timeout is not a float or out of range")))
			return
		}
		timeout := time.Duration(timeoutSecs * float64(time.Second))
		key, val, ok, err := s.BLPopMultiContext(ctx, keys, timeout)
		if err != nil && !errors.Is(err, context.Canceled) {
			w.Write([]byte(resp.Error(err.Error())))
			return
		}
		if !ok {
			w.Write([]byte(resp.NullArray()))
			return
		}
		w.Write([]byte(resp.Array([]string{key, val})))
	case "TYPE":
		if len(cmd.Args) != 1 {
			w.Write([]byte(resp.Error("wrong number of arguments for 'type' command")))
			return
		}
		w.Write([]byte(resp.SimpleString(s.Type(cmd.Args[0]))))
	case "XADD":
		// Format: XADD key id field1 value1 [field2 value2 ...]
		if len(cmd.Args) < 4 || (len(cmd.Args)-2)%2 != 0 {
			w.Write([]byte(resp.Error("wrong number of arguments for 'xadd' command")))
			return
		}
		key := cmd.Args[0]
		idStr := cmd.Args[1]
		fields := cmd.Args[2:] // [field1, value1, field2, value2, ...]

		entryID, err := s.XAdd(key, idStr, fields)
		if err != nil {
			w.Write([]byte(resp.Error(err.Error())))
			return
		}
		w.Write([]byte(resp.BulkString(entryID)))
	case "XRANGE":
		// Format: XRANGE key start end
		if len(cmd.Args) != 3 {
			w.Write([]byte(resp.Error("wrong number of arguments for 'xrange' command")))
			return
		}
		key := cmd.Args[0]
		start, err := stream.ParseRangeStart(cmd.Args[1])
		if err != nil {
			w.Write([]byte(resp.Error(err.Error())))
			return
		}
		end, err := stream.ParseRangeEnd(cmd.Args[2])
		if err != nil {
			w.Write([]byte(resp.Error(err.Error())))
			return
		}
		entries, err := s.XRangeChecked(key, start, end)
		if err != nil {
			w.Write([]byte(resp.Error(err.Error())))
			return
		}
		w.Write([]byte(resp.StreamEntries(entries)))
	case "XREAD":
		args := cmd.Args
		isBlocking := false
		var blockTimeout time.Duration

		// Parse optional BLOCK <milliseconds> at the beginning
		if len(args) > 0 && strings.ToUpper(args[0]) == "BLOCK" {
			if len(args) < 2 {
				w.Write([]byte(resp.Error("syntax error")))
				return
			}
			blockMs, err := strconv.ParseInt(args[1], 10, 64)
			if err != nil || blockMs < 0 || blockMs > int64((1<<63-1)/int64(time.Millisecond)) {
				w.Write([]byte(resp.Error("timeout is not an integer or out of range")))
				return
			}
			blockTimeout = time.Duration(blockMs) * time.Millisecond
			isBlocking = true
			args = args[2:] // skip past "BLOCK <ms>"
		}

		// Now expect: STREAMS key1 [key2 ...] id1 [id2 ...]
		if len(args) < 3 || strings.ToUpper(args[0]) != "STREAMS" {
			w.Write([]byte(resp.Error("syntax error")))
			return
		}

		remaining := args[1:] // everything after "STREAMS"
		if len(remaining)%2 != 0 {
			w.Write([]byte(resp.Error("unbalanced STREAMS list")))
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
					w.Write([]byte(resp.Error(err.Error())))
					return
				}
				afterIDs[i] = id
			}
			results, err := s.XReadChecked(keys, afterIDs)
			if err != nil {
				w.Write([]byte(resp.Error(err.Error())))
				return
			}
			if len(results) == 0 {
				w.Write([]byte(resp.NullArray()))
				return
			}
			w.Write([]byte(resp.StreamReadResults(results)))
		} else {
			// Blocking XREAD wakes when any requested stream advances, then
			// returns all streams with entries available after their IDs.
			afterIDs := make([]stream.EntryID, len(idStrs))
			for i, idStr := range idStrs {
				if idStr == "$" {
					afterIDs[i] = s.GetStreamLastID(keys[i])
					continue
				}
				var err error
				afterIDs[i], err = stream.Parse(idStr)
				if err != nil {
					w.Write([]byte(resp.Error(err.Error())))
					return
				}
			}

			results, ok, err := s.BXReadMultiContext(ctx, keys, afterIDs, blockTimeout)
			if err != nil && !errors.Is(err, context.Canceled) {
				w.Write([]byte(resp.Error(err.Error())))
				return
			}
			if !ok {
				w.Write([]byte(resp.NullArray()))
				return
			}
			w.Write([]byte(resp.StreamReadResults(results)))
		}
	case "ACL":
		if len(cmd.Args) < 1 {
			w.Write([]byte(resp.Error("wrong number of arguments for 'acl' command")))
			return
		}
		switch strings.ToUpper(cmd.Args[0]) {
		case "WHOAMI":
			if len(cmd.Args) != 1 {
				w.Write([]byte(resp.Error("wrong number of arguments for 'acl|whoami' command")))
				return
			}
			w.Write([]byte(resp.BulkString((*currentUser).Username)))

		case "GETUSER":
			if len(cmd.Args) != 2 {
				w.Write([]byte(resp.Error("wrong number of arguments for 'acl|getuser' command")))
				return
			}
			user, ok := auth.GetUser(cmd.Args[1])
			if !ok {
				w.Write([]byte(resp.NullBulkString()))
				return
			}
			flags, passwords := user.Snapshot()
			response := "*4\r\n" +
				resp.BulkString("flags") + resp.Array(flags) +
				resp.BulkString("passwords") + resp.Array(passwords)
			w.Write([]byte(response))

		case "SETUSER":
			if len(cmd.Args) < 2 {
				w.Write([]byte(resp.Error("wrong number of arguments for 'acl|setuser' command")))
				return
			}
			username := cmd.Args[1]
			user, ok := auth.GetUser(username)
			if !ok {
				w.Write([]byte(resp.Error(fmt.Sprintf("ERR User '%s' not found", username))))
				return
			}
			for _, rule := range cmd.Args[2:] {
				if strings.HasPrefix(rule, ">") {
					user.SetPassword(rule[1:])
				}
			}
			w.Write([]byte(resp.SimpleString("OK")))

		default:
			w.Write([]byte(resp.Error(fmt.Sprintf("unknown subcommand '%s' for 'acl' command", cmd.Args[0]))))
		}

	case "AUTH":
		// Format: AUTH <username> <password>
		if len(cmd.Args) != 2 {
			w.Write([]byte(resp.Error("wrong number of arguments for 'auth' command")))
			return
		}
		username := cmd.Args[0]
		password := cmd.Args[1]
		user, ok := auth.GetUser(username)
		if !ok || !user.Authenticate(password) {
			w.Write([]byte("-WRONGPASS invalid username-password pair or user is disabled\r\n"))
			return
		}
		*currentUser = user
		w.Write([]byte(resp.SimpleString("OK")))
	case "ZADD":
		// Format: ZADD key score member
		if len(cmd.Args) != 3 {
			w.Write([]byte(resp.Error("wrong number of arguments for 'zadd' command")))
			return
		}

		key := cmd.Args[0]
		scoreStr := cmd.Args[1]
		member := cmd.Args[2]

		// Parse the score as a 64-bit float
		score, err := strconv.ParseFloat(scoreStr, 64)
		if err != nil || math.IsNaN(score) {
			w.Write([]byte(resp.Error("ERR value is not a valid float")))
			return
		}

		addedCount, err := s.ZAddChecked(key, score, member)
		if err != nil {
			w.Write([]byte(resp.Error(err.Error())))
			return
		}
		w.Write([]byte(resp.Integer(addedCount)))

	case "ZRANK":
		// Format: ZRANK key member
		if len(cmd.Args) != 2 {
			w.Write([]byte(resp.Error("wrong number of arguments for 'zrank' command")))
			return
		}

		key := cmd.Args[0]
		member := cmd.Args[1]

		rank, exists, err := s.ZRankChecked(key, member)
		if err != nil {
			w.Write([]byte(resp.Error(err.Error())))
			return
		}
		if !exists {
			w.Write([]byte(resp.NullBulkString()))
			return
		}

		w.Write([]byte(resp.Integer(rank)))

	case "ZRANGE":
		// Format: ZRANGE key start stop
		if len(cmd.Args) != 3 {
			w.Write([]byte(resp.Error("wrong number of arguments for 'zrange' command")))
			return
		}

		key := cmd.Args[0]
		start, err1 := strconv.Atoi(cmd.Args[1])
		stop, err2 := strconv.Atoi(cmd.Args[2])

		if err1 != nil || err2 != nil {
			w.Write([]byte(resp.Error("ERR value is not an integer or out of range")))
			return
		}

		members, err := s.ZRangeChecked(key, start, stop)
		if err != nil {
			w.Write([]byte(resp.Error(err.Error())))
			return
		}
		w.Write([]byte(resp.Array(members)))
	case "ZCARD":
		// Format: ZCARD key
		if len(cmd.Args) != 1 {
			w.Write([]byte(resp.Error("wrong number of arguments for 'zcard' command")))
			return
		}

		key := cmd.Args[0]
		card, err := s.ZCardChecked(key)
		if err != nil {
			w.Write([]byte(resp.Error(err.Error())))
			return
		}
		w.Write([]byte(resp.Integer(card)))
	case "ZSCORE":
		// Format: ZSCORE key member
		if len(cmd.Args) != 2 {
			w.Write([]byte(resp.Error("wrong number of arguments for 'zscore' command")))
			return
		}

		key := cmd.Args[0]
		member := cmd.Args[1]

		score, exists, err := s.ZScoreChecked(key, member)
		if err != nil {
			w.Write([]byte(resp.Error(err.Error())))
			return
		}
		if !exists {
			w.Write([]byte(resp.NullBulkString()))
			return
		}

		// Convert float to string cleanly (removing trailing zeros if any)
		scoreStr := strconv.FormatFloat(score, 'f', -1, 64)
		w.Write([]byte(resp.BulkString(scoreStr)))
	case "ZREM":
		// Format: ZREM key member
		if len(cmd.Args) != 2 {
			w.Write([]byte(resp.Error("wrong number of arguments for 'zrem' command")))
			return
		}

		key := cmd.Args[0]
		member := cmd.Args[1]

		removedCount, err := s.ZRemChecked(key, member)
		if err != nil {
			w.Write([]byte(resp.Error(err.Error())))
			return
		}
		w.Write([]byte(resp.Integer(removedCount)))
	case "INCR":
		// Format: INCR key
		if len(cmd.Args) != 1 {
			w.Write([]byte(resp.Error("wrong number of arguments for 'incr' command")))
			return
		}

		newVal, err := s.Incr(cmd.Args[0])
		if err != nil {
			w.Write([]byte(resp.Error("value is not an integer or out of range")))
			return
		}

		w.Write([]byte(resp.Integer(newVal)))
	case "MULTI":
		// Format: MULTI — starts a transaction; further commands are queued (later stages)
		if len(cmd.Args) != 0 {
			w.Write([]byte(resp.Error("wrong number of arguments for 'multi' command")))
			return
		}
		if tx.InTransaction {
			w.Write([]byte(resp.Error("MULTI calls can not be nested")))
			return
		}
		tx.Begin()
		w.Write([]byte(resp.SimpleString("OK")))
	case "EXEC":
		// Format: EXEC — runs queued commands; error if MULTI was not called
		if len(cmd.Args) != 0 {
			w.Write([]byte(resp.Error("wrong number of arguments for 'exec' command")))
			return
		}
		if !tx.InTransaction {
			w.Write([]byte(resp.Error("EXEC without MULTI")))
			return
		}

		// Run every queued command; failures are captured as error replies, others still execute
		// Blocking commands cannot safely run while the command lock is held because
		// their producers need to acquire the same lock. Report them as runtime
		// errors and continue with the remaining queued commands.
		replies := tx.RunQueue(func(queuedCmd *resp.Command) string {
			if isPotentiallyBlocking(queuedCmd) {
				return resp.Error("blocking commands are not supported inside transactions")
			}
			var buf bytes.Buffer
			dispatchContext(ctx, queuedCmd, &buf, s, currentUser, tx)
			return buf.String()
		})

		tx.End()
		w.Write([]byte(resp.ArrayOfReplies(replies)))

	case "DISCARD":
		// Format: DISCARD — aborts a transaction and discards queued commands
		if len(cmd.Args) != 0 {
			w.Write([]byte(resp.Error("wrong number of arguments for 'discard' command")))
			return
		}
		if !tx.InTransaction {
			w.Write([]byte(resp.Error("DISCARD without MULTI")))
			return
		}

		tx.End()
		w.Write([]byte(resp.SimpleString("OK")))

	default:
		w.Write([]byte(resp.Error(fmt.Sprintf("unknown command '%s'", cmd.Name))))
	}
}
