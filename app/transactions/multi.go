package transactions

import "github.com/codecrafters-io/redis-starter-go/app/resp"

// State holds per-connection transaction state for one client connection.
type State struct {
	// InTransaction is true after MULTI until EXEC or DISCARD.
	InTransaction bool

	// Queue holds commands received after MULTI, executed on EXEC.
	Queue []*resp.Command
}

// Begin marks the connection as inside a transaction (MULTI command).
func (st *State) Begin() {
	st.InTransaction = true
	st.Queue = nil // fresh queue for each transaction
}

// Enqueue appends a command to the transaction queue without executing it.
func (st *State) Enqueue(cmd *resp.Command) {
	// Copy args so the queued command is independent of the parser's buffer
	argsCopy := make([]string, len(cmd.Args))
	copy(argsCopy, cmd.Args)

	st.Queue = append(st.Queue, &resp.Command{
		Name: cmd.Name,
		Args: argsCopy,
	})
}

// End clears transaction state after EXEC or DISCARD completes.
func (st *State) End() {
	st.InTransaction = false
	st.Queue = nil
}
