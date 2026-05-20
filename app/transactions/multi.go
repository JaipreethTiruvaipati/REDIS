package transactions

// State holds per-connection transaction state for one client connection.
type State struct {
	// InTransaction is true after MULTI until EXEC or DISCARD.
	InTransaction bool
}

// Begin marks the connection as inside a transaction (MULTI command).
func (st *State) Begin() {
	st.InTransaction = true
}
// End clears transaction state after EXEC or DISCARD completes.
func (st *State) End() {
	st.InTransaction = false
	// st.Queue = nil  // add when you introduce a command queue in the next stage
}