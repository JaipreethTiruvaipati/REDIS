package transactions

import "github.com/jaipreethtiruvaipati/redis-clone/app/resp"

// RunQueue executes each queued command using fn and collects RESP-encoded replies.
// If a command fails, its error reply is included in the result and remaining commands still run.
func (st *State) RunQueue(fn func(cmd *resp.Command) string) []string {
	replies := make([]string, 0, len(st.Queue))
	for _, cmd := range st.Queue {
		replies = append(replies, fn(cmd))
	}
	return replies
}
