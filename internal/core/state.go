package core

// Represents the status of an operation throughout its lifecycle.
type State string

const (
	StatePending   State = "pending"
	StateRunning   State = "running"
	StateCompleted State = "completed"
	StateFailed    State = "failed"
	StateCancelled State = "cancelled"
)

// Checks whether a transition from the current state
// to the specified state is allowed.
//
// Allowed transitions are:
//   - Pending: Running or Cancelled.
//   - Running: Completed, Failed, or Cancelled.
//
// The Completed, Failed and Cancelled states are final and do not
// allow new transitions.
//
// Returns true if the transition is allowed or false otherwise.
//
// Does not modify the current state.
func (state State) CanTransitionTo(next State) bool {
	switch state {
	case StatePending:
		return next == StateRunning || next == StateCancelled

	case StateRunning:
		return next == StateCompleted || next == StateFailed || next == StateCancelled

	default:
		return false
	}
}
